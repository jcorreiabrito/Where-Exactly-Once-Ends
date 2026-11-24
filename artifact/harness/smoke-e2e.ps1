# smoke-e2e.ps1 -- end-to-end fault-free smoke for one DB mechanism on the native stack.
#
# Acceptance helper for S3/S4 and for manual verification:
#   1. creates fresh topics (source + publication stream) and a fresh PostgreSQL schema,
#   2. starts ONE service instance and waits for its readiness file,
#   3. runs the deterministic LoadGenerator (ground truth written+hashed before any produce),
#   4. waits until the expected number of postings is durable,
#   5. stops the service gracefully and verifies the produced artefacts,
#   6. prints the SQL counts and a JSON summary (file: <scratch>\smoke-result.json) and cleans up.
#
# Also supports the Level-1 crash smoke:
#   powershell -ExecutionPolicy Bypass -File artifact/harness/smoke-e2e.ps1 -Mechanism M0 -CrashAt before_effect_commit
# expects exit code 99, exactly one "fired" receipt in faults.log and no "restored" receipt.
param(
    [string]$Mechanism = 'M0',
    [int]$Events = 1000,
    [int]$Partitions = 3,
    [string]$SeedHex = '0000000000000001',
    [string]$RunId = $null,
    [string]$CrashAt = $null,
    [int]$TimeoutSec = 180,
    [switch]$Keep
)

$ErrorActionPreference = 'Continue'
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')
$RuntimeRoot = if ($env:EOS_RUNTIME_ROOT) { $env:EOS_RUNTIME_ROOT } else { Join-Path $env:LOCALAPPDATA 'eos-runtime' }
$Java = Join-Path $RuntimeRoot 'vendor\jdk21\bin\java.exe'
$Jar = Join-Path $RepoRoot 'artifact\target\wxeo-service.jar'
$Kafka = Join-Path $RuntimeRoot 'vendor\kafka'
$KafkaCp = Join-Path $Kafka 'libs\*'
$Psql = Join-Path $RuntimeRoot 'vendor\pgsql\bin\psql.exe'
$PgHost = '127.0.0.1'
$PgPort = 5433
$PgDb = 'wxeo'
$Bootstrap = '127.0.0.1:9091'

if (-not (Test-Path $Jar)) { throw "service jar not found at $Jar (run artifact/build.ps1 package)" }
if (-not (Test-Path $Java)) { throw "JDK 21 not found at $Java (set EOS_RUNTIME_ROOT)" }
if ($Mechanism -eq 'M1-kafka') { throw "this smoke requires a DB-sink mechanism (M1-kafka's sink is Kafka)" }

if (-not $RunId) {
    $token = $Mechanism.ToLower() -replace '\+', ''
    $RunId = "smoke-$token-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))"
}
$SourceTopic = "wxeo.src.$RunId"
$SinkTopic = "wxeo.sink.$RunId"
$Group = "wxeo.cg.$RunId"
$Schema = "wxeo_run_$RunId"
$Scratch = Join-Path $RuntimeRoot "scratch\$RunId"
$OutDir = Join-Path $Scratch 'out'
$GtPath = Join-Path $Scratch "gt_$RunId.ndjson"
$ManifestPath = Join-Path $Scratch "manifest_$RunId.json"
$ProducedPath = Join-Path $Scratch "produced_$RunId.ndjson"
$FaultsLog = Join-Path $Scratch 'faults.log'
$ServiceStdout = Join-Path $OutDir 'service.stdout.txt'
$ServiceStderr = Join-Path $OutDir 'service.stderr.txt'
$LoaderLog = Join-Path $Scratch 'loader.log'
$ResultPath = Join-Path $Scratch 'smoke-result.json'
New-Item -ItemType Directory -Force -Path $Scratch, $OutDir | Out-Null
$StopFlag = Join-Path $OutDir "stop_${RunId}_1.flag"
Remove-Item $StopFlag -ErrorAction SilentlyContinue
Remove-Item $FaultsLog -ErrorAction SilentlyContinue

function Log([string]$msg) { Write-Host ("[{0}] {1}" -f (Get-Date).ToUniversalTime().ToString('HH:mm:ss'), $msg) }
function PsqlQuery([string]$sql) {
    $out = & $Psql -h $PgHost -p $PgPort -U postgres -d $PgDb -tAc $sql 2>&1
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $out" }
    return ("$out").Trim()
}
function CreateTopic([string]$name) {
    & $Java -cp $KafkaCp kafka.tools.TopicCommand --bootstrap-server $Bootstrap --create --topic $name `
        --partitions $Partitions --replication-factor 3 --config min.insync.replicas=2 --config retention.ms=3600000 2>&1 |
        Where-Object { $_ -notmatch 'log4j' } | ForEach-Object { Log "topic: $_" }
}
function DeleteTopic([string]$name) {
    & $Java -cp $KafkaCp kafka.tools.TopicCommand --bootstrap-server $Bootstrap --delete --topic $name 2>&1 |
        Where-Object { $_ -notmatch 'log4j' } | Out-Null
}

$summary = [ordered]@{
    schema        = 'wxeo.smoke.v1'
    mechanism     = $Mechanism
    run_id        = $RunId
    events        = $Events
    partitions    = $Partitions
    seed_hex      = $SeedHex
    crash_at      = $CrashAt
    source_topic  = $SourceTopic
    sink_topic    = $SinkTopic
    group         = $Group
    db_schema     = $Schema
    scratch       = $Scratch
    started_utc   = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    ok            = $false
}
$service = $null
$exitCode = 0

try {
    # ---- 1. fresh logical state -------------------------------------------------------------
    Log "creating topics $SourceTopic / $SinkTopic (partitions=$Partitions, RF=3)"
    CreateTopic $SourceTopic
    CreateTopic $SinkTopic
    Log "initialising schema $Schema for $Mechanism"
    $schemaOut = & $Java -cp $Jar com.wxeo.sink.SchemaTool --mode=init `
        --db-url="jdbc:postgresql://${PgHost}:${PgPort}/$PgDb" --db-user=postgres `
        --schema=$Schema --mechanism=$Mechanism --app-role=wxeo_app --read-role=wxeo_read 2>&1
    Log "schema: $schemaOut"
    if ($LASTEXITCODE -ne 0) { throw "SchemaTool init failed: $schemaOut" }

    # ---- 2. service ------------------------------------------------------------------------
    $svcArgs = @(
        '-Xms256m', '-Xmx1g', '-XX:+UseG1GC',
        '-Dfile.encoding=UTF-8',
        '-jar', $Jar,
        "--wxeo.mechanism=$Mechanism",
        '--wxeo.ordering=strict',
        "--wxeo.run-id=$RunId",
        '--wxeo.instance-id=1',
        "--wxeo.bootstrap=$Bootstrap",
        "--wxeo.db-url=jdbc:postgresql://${PgHost}:${PgPort}/$PgDb",
        '--wxeo.db-user=wxeo_app',
        "--wxeo.db-schema=$Schema",
        "--wxeo.source-topic=$SourceTopic",
        "--wxeo.sink-topic=$SinkTopic",
        "--wxeo.group=$Group",
        "--wxeo.output-dir=$OutDir",
        "--wxeo.faults-log=$FaultsLog"
    )
    if ($CrashAt) {
        $svcArgs += @(
            '-Dwxeo.failpoints.enabled=true',
            '--wxeo.failpoint.enabled=true',
            "--wxeo.failpoint.name=$CrashAt",
            '--wxeo.failpoint.action=crash',
            '--wxeo.failpoint.after=1'
        )
    }
    Log "starting service: $Mechanism (instance 1)"
    $service = Start-Process -FilePath $Java -ArgumentList $svcArgs `
        -RedirectStandardOutput $ServiceStdout -RedirectStandardError $ServiceStderr `
        -WindowStyle Hidden -PassThru
    $readyPath = Join-Path $OutDir "ready_${RunId}_1.json"
    $deadline = (Get-Date).AddSeconds(90)
    $ready = $false
    while ((Get-Date) -lt $deadline) {
        if (Test-Path $readyPath) {
            $r = Get-Content $readyPath -Raw | ConvertFrom-Json
            if ($r.status -eq 'assigned' -or $r.status -eq 'subscribed') { $ready = $true; break }
        }
        if ($service.HasExited) { throw "service exited early (code $($service.ExitCode)); see $ServiceStderr" }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) { throw "service did not become ready within 90s" }
    Log "service ready (ready file: $readyPath)"

    # ---- 3. deterministic load (ground truth first) ---------------------------------------
    Log "running load generator: $Events events, partitions=$Partitions, rate=max"
    $loaderOut = & $Java -cp $Jar com.wxeo.load.LoadGenerator `
        --run-id=$RunId --bootstrap=$Bootstrap --topic=$SourceTopic `
        --events=$Events --partitions=$Partitions --rate=max `
        --data-seed-hex=$SeedHex `
        --gt-out=$GtPath --manifest-out=$ManifestPath --produced-out=$ProducedPath 2>&1
    $loaderOut | Tee-Object -FilePath $LoaderLog | ForEach-Object { Log "loader: $_" }
    if ($LASTEXITCODE -ne 0) { throw "load generator failed (see $LoaderLog)" }
    $loaderResult = ($loaderOut | Where-Object { $_ -match '^\{"schema"' } | Select-Object -Last 1) | ConvertFrom-Json
    $summary['loader'] = $loaderResult

    # ---- 4a. crash-failpoint mode ---------------------------------------------------------
    if ($CrashAt) {
        $deadline = (Get-Date).AddSeconds([Math]::Min($TimeoutSec, 120))
        while (-not $service.HasExited -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
        if (-not $service.HasExited) { Stop-Process -Id $service.Id -Force; throw "service did not crash within timeout" }
        $service.WaitForExit()
        $summary['service_exit_code'] = $service.ExitCode
        $receipts = @()
        if (Test-Path $FaultsLog) {
            $receipts = @(Get-Content $FaultsLog | Where-Object { $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json })
        }
        $fired = @($receipts | Where-Object { $_.event -eq 'fired' })
        $restored = @($receipts | Where-Object { $_.event -eq 'restored' })
        $summary['fault_receipts'] = $receipts
        $summary['fired_count'] = $fired.Count
        $summary['restored_count'] = $restored.Count
        if ($service.ExitCode -ne 99) { throw "expected exit code 99, got $($service.ExitCode)" }
        if ($fired.Count -ne 1) { throw "expected exactly one fired receipt, got $($fired.Count)" }
        if ($fired[0].fault -ne $CrashAt) { throw "receipt fault '$($fired[0].fault)' != '$CrashAt'" }
        if ($restored.Count -ne 0) { throw "crash action must not write a restored receipt" }
        $summary['ok'] = $true
        Log "CRASH SMOKE PASS: exit=99, one fired receipt at $CrashAt, no restored receipt"
        $summary | ConvertTo-Json -Depth 10 | Set-Content $ResultPath -Encoding utf8
        Write-Host ($summary | ConvertTo-Json -Depth 10)
        exit 0
    }

    # ---- 4b. fault-free mode: wait for durable effect -------------------------------------
    $expectedPostings = $Events * 2
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $count = -1
    while ((Get-Date) -lt $deadline) {
        $count = [int](PsqlQuery "SELECT count(*) FROM $Schema.posting")
        if ($count -ge $expectedPostings) { break }
        if ($service.HasExited) { throw "service exited early (code $($service.ExitCode)); see $ServiceStderr" }
        Start-Sleep -Milliseconds 300
    }
    Log "posting rows after load: $count / $expectedPostings"

    # ---- 5. graceful stop + artefact verification ----------------------------------------
    Log "requesting graceful stop (flag $StopFlag)"
    Set-Content -Path $StopFlag -Value 'stop' -Encoding ascii
    $deadline = (Get-Date).AddSeconds(120)
    while (-not $service.HasExited -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
    if (-not $service.HasExited) { Stop-Process -Id $service.Id -Force; throw "graceful stop timed out" }
    $service.WaitForExit()
    $summary['service_exit_code'] = $service.ExitCode
    if ($service.ExitCode -ne 0) { throw "graceful service exit code $($service.ExitCode) (expected 0)" }

    $appPath = Join-Path $OutDir "app_${RunId}_1.json"
    $configPath = Join-Path $OutDir "config_${RunId}_1.json"
    $eventHlog = Join-Path $OutDir "hist_${RunId}_1_event.hlog"
    $batchHlog = Join-Path $OutDir "hist_${RunId}_1.hlog"
    $histJson = Join-Path $OutDir "hist_${RunId}_1.json"
    foreach ($p in @($appPath, $configPath, $eventHlog, $batchHlog, $histJson, $GtPath, "$Scratch\gt_$RunId.sha256", $ManifestPath, $ProducedPath)) {
        if (-not (Test-Path $p)) { throw "missing artefact: $p" }
    }
    $app = Get-Content $appPath -Raw | ConvertFrom-Json
    $manifest = Get-Content $ManifestPath -Raw | ConvertFrom-Json
    $hist = Get-Content $histJson -Raw | ConvertFrom-Json

    # GT digest verification: file hash == .sha256 file == manifest gt_sha256
    $gtDigest = (Get-FileHash $GtPath -Algorithm SHA256).Hash.ToLower()
    $shaFileDigest = (Get-Content "$Scratch\gt_$RunId.sha256" -Raw).Trim()
    if ($gtDigest -ne $shaFileDigest) { throw "GT file digest != .sha256 file ($gtDigest vs $shaFileDigest)" }
    if ($gtDigest -ne $manifest.gt_sha256) { throw "GT digest != manifest gt_sha256" }
    $gtLines = @(Get-Content $GtPath | Where-Object { $_.Trim() }).Count
    if ($gtLines -ne $expectedPostings) { throw "GT lines $gtLines != $expectedPostings" }
    $producedLines = @(Get-Content $ProducedPath | Where-Object { $_.Trim() }).Count
    if ($producedLines -ne $Events) { throw "produced lines $producedLines != $Events" }

    # SQL: totals, duplicates (I3), losses (I4) against the GT as a temporary table is S4's oracle;
    # here we check the mechanism's basic end-state: exact posting count and zero duplicate keys.
    $dupKeys = [int](PsqlQuery "SELECT count(*) FROM (SELECT event_id, leg_no FROM $Schema.posting GROUP BY event_id, leg_no HAVING count(*) > 1) d")
    $distinctKeys = [int](PsqlQuery "SELECT count(DISTINCT (event_id, leg_no)) FROM $Schema.posting")
    $schemaAttest = & $Java -cp $Jar com.wxeo.sink.SchemaTool --mode=verify `
        --db-url="jdbc:postgresql://${PgHost}:${PgPort}/$PgDb" --db-user=wxeo_app `
        --schema=$Schema --mechanism=$Mechanism 2>&1
    $attest = ("$schemaAttest" | Select-Object -Last 1) | ConvertFrom-Json

    $summary['posting_rows'] = $count
    $summary['expected_postings'] = $expectedPostings
    $summary['distinct_keys'] = $distinctKeys
    $summary['duplicate_keys'] = $dupKeys
    $summary['gt_sha256'] = $gtDigest
    $summary['gt_lines'] = $gtLines
    $summary['produced_lines'] = $producedLines
    $summary['artifacts'] = [ordered]@{
        gt                   = $GtPath
        gt_sha256            = "$Scratch\gt_$RunId.sha256"
        manifest             = $ManifestPath
        produced             = $ProducedPath
        config               = $configPath
        app                  = $appPath
        hist_event_hlog      = $eventHlog
        hist_batch_hlog      = $batchHlog
        hist_json            = $histJson
        service_stdout       = $ServiceStdout
        service_stderr       = $ServiceStderr
        faults_log           = $FaultsLog
    }
    $summary['config_shared_sha256'] = $app.shared_sha256
    $summary['counters'] = $app.counters
    $summary['percentiles_event'] = $hist.event
    $summary['percentiles_batch'] = $hist.batch
    $summary['attestation'] = $attest.checks

    if ($count -ne $expectedPostings) { throw "posting count $count != $expectedPostings" }
    if ($dupKeys -ne 0) { throw "duplicate posting keys present in fault-free run: $dupKeys" }
    if ($app.counters.committed_events -lt $Events) { throw "app committed_events $($app.counters.committed_events) < $Events" }
    $summary['ok'] = $true
    Log "E2E SMOKE PASS: $Mechanism, $Events events -> $count postings, 0 duplicates, artefacts verified"
}
catch {
    $summary['ok'] = $false
    $summary['error'] = $_.Exception.Message
    Log "SMOKE FAILED: $($_.Exception.Message)"
    if (Test-Path $ServiceStderr) { Get-Content $ServiceStderr -Tail 30 | ForEach-Object { Log "stderr: $_" } }
    if (Test-Path $ServiceStdout) { Get-Content $ServiceStdout -Tail 20 | ForEach-Object { Log "stdout: $_" } }
    $exitCode = 1
}
finally {
    if ($service -and -not $service.HasExited) { Stop-Process -Id $service.Id -Force -ErrorAction SilentlyContinue }
    if (-not $Keep) {
        try { & $Java -cp $Jar com.wxeo.sink.SchemaTool --mode=drop --db-url="jdbc:postgresql://${PgHost}:${PgPort}/$PgDb" --db-user=postgres --schema=$Schema 2>&1 | Out-Null } catch { }
        try { DeleteTopic $SourceTopic } catch { }
        try { DeleteTopic $SinkTopic } catch { }
    }
    $summary['finished_utc'] = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    $summary | ConvertTo-Json -Depth 12 | Set-Content $ResultPath -Encoding utf8
    Log "result: $ResultPath"
    Write-Host ($summary | ConvertTo-Json -Depth 12)
}
exit $exitCode
