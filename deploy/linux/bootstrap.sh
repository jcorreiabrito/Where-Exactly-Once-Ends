#!/usr/bin/env bash
# WXEO campaign host bootstrap (Ubuntu/Debian, x86_64). Run from the repository root:
#   bash deploy/linux/bootstrap.sh
#
# Installs the campaign toolchain (JDK 21, Maven, PostgreSQL client, Docker Compose, Python 3.12
# with the pinned runner dependencies), builds the frozen service jar, starts the container stack
# and runs the pre-campaign checks. It never runs experiment cells; those are in run-campaign.sh.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"
echo "== repo: $REPO"

need_sudo=""
if [[ "$(id -u)" -ne 0 ]]; then need_sudo="sudo"; fi

echo "== apt packages"
$need_sudo apt-get update -y
$need_sudo apt-get install -y --no-install-recommends \
  ca-certificates curl git maven openjdk-21-jdk-headless postgresql-client python3.12 python3.12-venv \
  python3-pip docker.io docker-compose-plugin

echo "== docker access (add the current user to the docker group if needed)"
if ! docker info >/dev/null 2>&1; then
  $need_sudo usermod -aG docker "$USER" || true
  echo "NOTE: log out/in (or run 'newgrp docker') so the current shell can use docker."
fi

echo "== python venv + pinned dependencies"
python3.12 -m venv .venv
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

echo "== build the frozen service jar"
mvn -q -f artifact/pom.xml -DskipTests=false package
JAR="artifact/target/wxeo-service.jar"
test -f "$JAR"
sha256sum "$JAR"

echo "== start the container campaign stack"
export WXEO_STACK_MODE=docker
export EOS_RUNTIME_ROOT="${EOS_RUNTIME_ROOT:-$HOME/eos-runtime}"
mkdir -p "$EOS_RUNTIME_ROOT/run"
docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign up -d --wait --remove-orphans
docker compose -f deploy/docker/docker-compose.yml -p wxeo-campaign ps

echo "== wait for the brokers/DB/proxies from the runner's point of view"
python - <<'PY'
import time, sys
sys.path.insert(0, ".")
from experiments import kafka_admin as KA
from experiments import injectors as INJ
deadline = time.time() + 240
ok = False
while time.time() < deadline:
    try:
        brokers = KA.KafkaAdmin("127.0.0.1:9091").broker_count()
        proxies = INJ.Toxiproxy().proxies()
        if brokers == 3 and len(proxies) >= 2:
            print("stack live: brokers=%d proxies=%s" % (brokers, sorted(proxies)))
            ok = True
            break
    except Exception as exc:  # noqa: BLE001
        print("waiting:", type(exc).__name__, exc)
    time.sleep(3)
if not ok:
    raise SystemExit("stack did not become live")
PY

echo "== runner preflight (writes results/raw/summaries/reserved-runs.csv)"
python experiments/runner.py --check-env

echo "== artifact unit tests (the Linux substitute for S3's Windows-only acceptance suite)"
mvn -q -f artifact/pom.xml test

echo "== harness unit tests"
python -m pytest experiments/tests -q

echo "== record the frozen base and issue the single pre-campaign freeze"
python experiments/freeze.py --write --force
python experiments/freeze.py --check
echo "bootstrap complete; next: bash deploy/linux/run-campaign.sh pilot"
