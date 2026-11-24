#!/usr/bin/env bash
# WXEO campaign host bootstrap (Ubuntu/Debian, x86_64). Run from the repository root:
#   bash deploy/linux/bootstrap.sh
#
# Installs the campaign toolchain (JDK 21, Maven, PostgreSQL client, Docker Compose, Python 3.12
# with the pinned runner dependencies), builds the frozen service jar, starts the container stack,
# runs the fault-efficacy proof (fail closed), the artifact test suites and the pre-campaign checks.
# It never runs experiment cells; those are in run-campaign.sh.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"
echo "== repo: $REPO"
# S5 §12.2 requires a bootstrap host log with versions, image digests and test summaries.
mkdir -p results/phase-logs
BOOT_LOG="results/phase-logs/bootstrap-$(date -u +%Y%m%dT%H%M%SZ).log"
exec > >(tee -a "$BOOT_LOG") 2>&1
echo "bootstrap log: $BOOT_LOG"

need_sudo=""
if [[ "$(id -u)" -ne 0 ]]; then need_sudo="sudo"; fi

echo "== apt packages"
$need_sudo apt-get update -y
$need_sudo apt-get install -y --no-install-recommends \
  ca-certificates curl git maven openjdk-21-jdk-headless postgresql-client python3 python3-venv \
  python3-pip
# Docker engine + compose plugin: package names differ across Ubuntu releases.
$need_sudo apt-get install -y --no-install-recommends docker.io || true
$need_sudo apt-get install -y --no-install-recommends docker-compose-v2 \
  || $need_sudo apt-get install -y --no-install-recommends docker-compose-plugin || true

echo "== docker access (hard requirement: the current shell must be able to use docker)"
if ! docker info >/dev/null 2>&1; then
  $need_sudo usermod -aG docker "$USER" || true
  echo "ERROR: the current shell cannot reach the docker daemon." >&2
  echo "       Log out and back in (or run 'newgrp docker'), then re-run this script." >&2
  exit 1
fi
docker compose version

echo "== python venv + pinned dependencies"
PYTHON_BIN="$(command -v python3.12 || command -v python3)"
"$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("Python >= 3.10 is required (frozen runner uses modern typing)")
print("python", sys.version.split()[0])
PY
"$PYTHON_BIN" -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip
pip install -r deploy/linux/requirements.txt

echo "== versions"
java -version
mvn -v | head -n 2
psql --version
python --version
docker --version
docker compose version

echo "== Windows pilot rows must be archived before the Linux campaign (red-team L5)"
mkdir -p results/windows-pilot
if [[ -n "$(find results/windows-pilot -name '*.tgz' -print -quit 2>/dev/null || true)" ]]; then
  echo "lead-owned Windows archive already present under results/windows-pilot/ (precondition met)"
elif [[ -d results/raw/run-records ]] && [[ -n "$(ls -A results/raw/run-records 2>/dev/null || true)" ]]; then
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  tar -czf "results/windows-pilot/windows-run-records-${stamp}.tgz" -C results/raw run-records
  sha256sum "results/windows-pilot/windows-run-records-${stamp}.tgz" \
    > "results/windows-pilot/windows-run-records-${stamp}.sha256"
  rm -rf results/raw/run-records
  echo "archived Windows run records -> results/windows-pilot/"
fi
mkdir -p results/raw/run-records

echo "== build the frozen service jar"
mvn -q -f artifact/pom.xml -DskipTests=false package
JAR="artifact/target/wxeo-service.jar"
test -f "$JAR"
sha256sum "$JAR"

echo "== pull + start the container campaign stack"
export WXEO_STACK_MODE=docker
export EOS_RUNTIME_ROOT="${EOS_RUNTIME_ROOT:-$HOME/eos-runtime}"
mkdir -p "$EOS_RUNTIME_ROOT/run"
mkdir -p results/harness
docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign pull
# B5: bind the repo digests of the three pinned images (fail closed if a digest is missing).
python deploy/linux/record_container_images.py --out results/harness/container-images.json
test -s results/harness/container-images.json
docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign images --digests
python -m experiments.stack up
python -m experiments.stack status

echo "== verify the pinned container names (F1/F2 injectors address these exactly)"
EXPECTED_CONTAINERS="wxeo-campaign-kafka-1 wxeo-campaign-kafka-2 wxeo-campaign-kafka-3 wxeo-campaign-postgres wxeo-campaign-toxiproxy"
for c in $EXPECTED_CONTAINERS; do
  if ! docker inspect -f '{{.State.Status}}' "$c" >/dev/null 2>&1; then
    echo "ERROR: container $c is missing; check 'docker compose ps --format'" >&2
    exit 1
  fi
done
docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign ps --format \
  'table {{.Name}}\t{{.State}}\t{{.Ports}}'

echo "== wait for brokers/DB/proxies from the runner's point of view"
python - <<'PY'
import sys, time
sys.path.insert(0, ".")
from experiments import kafka_admin as KA
from experiments import injectors as INJ
deadline = time.time() + 240
ok = False
while time.time() < deadline:
    try:
        brokers = KA.KafkaAdmin("127.0.0.1:9091").broker_count()
        proxies = INJ.Toxiproxy().proxies()
        if brokers == 3 and len(proxies) >= 4:
            print("stack live: brokers=%d proxies=%s" % (brokers, sorted(proxies)))
            ok = True
            break
    except Exception as exc:  # noqa: BLE001
        print("waiting:", type(exc).__name__, exc)
    time.sleep(3)
if not ok:
    raise SystemExit("stack did not become live")
PY

echo "== FAULT-EFFICACY GATE (fail closed): toxics must actually break the operation"
python deploy/linux/fault-efficacy.py --out results/harness/linux-fault-efficacy.json

echo "== artifact unit tests + integration ITs against the container stack"
mvn -q -f artifact/pom.xml test
mvn -q -f artifact/pom.xml verify -Pintegration \
  -Dwxeo.it.kafka.bootstrap=127.0.0.1:29091 \
  -Dwxeo.it.db.url=jdbc:postgresql://127.0.0.1:5433/postgres
sha256sum "$JAR" > results/harness/wxeo-service.jar.sha256

echo "== harness unit tests"
python -m pytest experiments/tests -q

echo "== runner preflight"
python experiments/runner.py --check-env

echo "== PROVISIONAL freeze only (no --require-evidence); the single FINAL freeze is T2:"
echo "   T0 smoke -> T0b fault-efficacy -> T1 calibrate (rates validated) -> T3 gate-controls"
echo "   -> T2 freeze.py --write --force --require-evidence (last step before T4 pilot)."
echo "   B5 image digests recorded at results/harness/container-images.json (bind in the final freeze)."
python experiments/freeze.py --write --force
python experiments/freeze.py --check
echo "bootstrap complete."
echo "  T0 : bash deploy/linux/run-campaign.sh smoke"
echo "  T0b: bash deploy/linux/run-campaign.sh fault-efficacy   (re-runnable proof)"
echo "  T1 : bash deploy/linux/run-campaign.sh calibrate"
echo "  T2 : bash deploy/linux/run-campaign.sh freeze"
echo "  T3 : bash deploy/linux/run-campaign.sh gate-controls"
echo "  T4 : bash deploy/linux/run-campaign.sh pilot"
