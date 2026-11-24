# acceptance-suite.ps1 -- self-contained S3 acceptance campaign on the native stack.
# ASCII-only: Windows PowerShell 5.1 parses BOM-less files as ANSI; keep this file pure ASCII.
#
# Starts PostgreSQL + the 3-broker Kafka KRaft cluster + Toxiproxy, runs the acceptance checks
# (Kafka create/produce/consume; M0 1k E2E; M3 1k E2E; failpoint-crash exit 99), then stops the whole
# stack. Topic deletion is never performed (fatal on this Windows KRaft setup); cleanup is reset-kafka.
# Evidence: $EOS_RUNTIME_ROOT\run\acceptance\acceptance-result.json + acceptance-suite.log
#
#   powershell -ExecutionPolicy Bypass -File artifact/harness/acceptance-suite.ps1 [-Events 1000] [-NoReset]
param(
    [int]$Events = 1000,
    [int]$Partitions = 3,
    [string]$SeedHex = '0000000000000001',
    [switch]$NoReset
)
$ErrorActionPreference = 'Continue'
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')
$RuntimeRoot = if ($env:EOS_RUNTIME_ROOT) { $env:EOS_RUNTIME_ROOT } else { Join-Path $env:LOCALAPPDATA 'eos-runtime' }
$Java = Join-Path $RuntimeRoot 'vendor\jdk21\bin\java.exe'
$Jar = Join-Path (Join-Path $RepoRoot 'artifact\target') 'wxeo-service.jar'
$Harness = Join-Path $PSScriptRoot 'native-stack.ps1'
$Smoke = Join-Path $PSScriptRoot 'smoke-e2e.ps1'
$EvidenceDir = Join-Path $RuntimeRoot 'run\acceptance'
New-Item -ItemType Directory -Force -Path $EvidenceDir | Out-Null
$ResultPath = Join-Path $EvidenceDir 'acceptance-result.json'
$LogPath = Join-Path $EvidenceDir 'acceptance-suite.log'
if (-not (Test-Path $Jar)) { throw "service jar not found at $Jar (run artifact/build.ps1 package)" }

$result = [ordered]@{
    schema = 'wxeo.acceptance.v1'
    started_utc = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    events = $Events; partitions = $Partitions; seed_hex = $SeedHex
    jar = $Jar; evidence_dir = $EvidenceDir
    tests = [ordered]@{}
    ok = $false
}

function Log([string]$m) {
    $line = "[{0}] {1}" -f (Get-Date).ToUniversalTime().ToString('HH:mm:ss'), $m
    Write-Host $line
    Add-Content -Path $LogPath -Value $line
}
function Harness([string]$verb) {
    Log "harness $verb"
    try { & $Harness $verb 2>&1 | ForEach-Object { Log "  $_" } }
    catch { Log "  harness error: $($_.Exception.Message)" }
}
function WaitPort([int]$port, [int]$seconds) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-NetConnection -ComputerName 127.0.0.1 -Port $port -InformationLevel Quiet -WarningAction SilentlyContinue) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}
function Find-LatestSmokeResult([string]$MechPattern) {
    # Fallback when a smoke script's stdout cannot be scraped: newest scratch\smoke-<mech>-*\smoke-result.json.
    $scratchRoot = Join-Path $RuntimeRoot 'scratch'
    if (-not (Test-Path $scratchRoot)) { return $null }
    $dirs = @(Get-ChildItem -Path $scratchRoot -Directory -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -like ('smoke-' + $MechPattern + '-*') } |
        Sort-Object LastWriteTime -Descending)
    foreach ($d in $dirs) {
        $candidate = Join-Path $d.FullName 'smoke-result.json'
        if (Test-Path $candidate) {
            try { return [ordered]@{ path = $candidate; obj = (Get-Content $candidate -Raw | ConvertFrom-Json) } } catch { }
        }
    }
    return $null
}
function Get-SmokeResult([string]$MechPattern, $Output) {
    $obj = $null
    $resLine = $Output | Where-Object { $_ -match '^WXEO_SMOKE_RESULT_PATH=' } | Select-Object -Last 1
    if ($resLine) {
        $resPath = ($resLine -replace '^WXEO_SMOKE_RESULT_PATH=', '').Trim()
        if (Test-Path $resPath) { try { $obj = Get-Content $resPath -Raw | ConvertFrom-Json } catch { } }
    }
    if (-not $obj) {
        $text = ($Output | Out-String)
        $jsonStart = $text.LastIndexOf('{"schema"')
        if ($jsonStart -ge 0) { try { $obj = $text.Substring($jsonStart) | ConvertFrom-Json } catch { } }
    }
    if (-not $obj) {
        $latest = Find-LatestSmokeResult $MechPattern
        if ($latest) { $obj = $latest.obj; Log ("  recovered smoke result from " + $latest.path) }
    }
    return $obj
}

Remove-Item $LogPath -ErrorAction SilentlyContinue
try {
    Log "acceptance suite start (events=$Events partitions=$Partitions seed=$SeedHex)"
    if (-not $NoReset) { Harness 'reset-kafka' } else { Harness 'start-kafka' }
    Harness 'start-pg'
    Harness 'start-toxiproxy'
    $kafkaUp = (WaitPort 9091 120) -and (WaitPort 9092 30) -and (WaitPort 9093 30)
    $pgUp = WaitPort 5433 60
    $toxiUp = WaitPort 8474 30
    Log "ports: kafka=$kafkaUp postgres=$pgUp toxiproxy=$toxiUp"
    if (-not ($kafkaUp -and $pgUp -and $toxiUp)) { throw "stack did not become ready (kafka=$kafkaUp pg=$pgUp toxiproxy=$toxiUp)" }

    # ---- acceptance 3: Kafka create/produce/consume (NO topic deletion) -------------------------
    $ts = [DateTime]::UtcNow.ToString('HHmmss')
    $topic = "smoke-accept-$ts"
    $kscratch = Join-Path $RuntimeRoot "scratch\kafka-smoke-$ts"
    New-Item -ItemType Directory -Force -Path $kscratch | Out-Null
    Log "ACCEPTANCE 3: Kafka create/produce/consume on $topic (delete step SKIPPED: fatal on Windows KRaft)"
    $createOut = & $Java -cp $Jar com.wxeo.tools.TopicTool --action=create --bootstrap=127.0.0.1:9091 --topic=$topic --partitions $Partitions --replication-factor 3 --min-insync 2 2>&1
    $createOk = [bool]($createOut | Select-String -Pattern '"ok":true' -Quiet)
    Log "topic create ok=$createOk"
    $loadOut = & $Java -cp $Jar com.wxeo.load.LoadGenerator --run-id=$topic --bootstrap=127.0.0.1:9091 --topic=$topic --events=100 --partitions=$Partitions --rate=max --data-seed-hex=$SeedHex --gt-out="$kscratch\gt.ndjson" --manifest-out="$kscratch\manifest.json" 2>&1
    $loadLine = $loadOut | Where-Object { $_ -match '^\{"schema":"wxeo.loader.result.v1"' } | Select-Object -Last 1
    $load = if ($loadLine) { $loadLine | ConvertFrom-Json } else { $null }
    if ($load) { Log "loader acked=$($load.acked) failed=$($load.failed) gt=$($load.gt_sha256)" } else { Log "loader produced no parsable result" }
    $consOut = & $Java -cp $Jar com.wxeo.tools.SmokeConsume --bootstrap=127.0.0.1:9091 --topic=$topic --count=100 --timeout-ms=30000 2>&1
    $consLine = $consOut | Where-Object { $_ -match '^\{"schema":"wxeo.smoke.consume.v1"' } | Select-Object -Last 1
    $cons = if ($consLine) { $consLine | ConvertFrom-Json } else { $null }
    if ($cons) { Log "consumer consumed=$($cons.consumed)/$($cons.expected) ok=$($cons.ok)" } else { Log "consumer produced no parsable result" }
    $kafkaPassed = [bool]($createOk -and $load -and $load.ok -and $cons -and $cons.ok)
    $result.tests['acceptance3_kafka_smoke'] = [ordered]@{
        passed = $kafkaPassed
        topic = $topic; create_ok = $createOk; loader = $load; consumer = $cons
        delete_skipped = "SKIPPED: topic deletion is fatal on Windows KRaft (rename fails, brokers shut down); cleanup via reset-kafka"
    }
    Log "TEST acceptance3_kafka_smoke: $(if ($kafkaPassed) { 'PASS' } else { 'FAIL' })"

    # ---- acceptance 4a/4b: M0 and M3 E2E ------------------------------------------------------
    foreach ($mech in @('M0', 'M3')) {
        Log "ACCEPTANCE 4: $mech $Events-event E2E"
        $out = & $Smoke -Mechanism $mech -Events $Events -Partitions $Partitions -SeedHex $SeedHex 2>&1
        $smokeExit = $LASTEXITCODE
        $smokeOutFile = Join-Path $EvidenceDir ("smoke-" + $mech + "-stdout.txt")
        ($out | Out-String) | Set-Content -Path $smokeOutFile -Encoding utf8
        $out | ForEach-Object { Add-Content -Path $LogPath -Value $_ }
        $smokeObj = Get-SmokeResult $mech.ToLower() $out
        $passed = [bool]($smokeObj -and $smokeObj.ok)
        Log "$mech smoke passed=$passed (script exit=$smokeExit)"
        if ($smokeObj) {
            Log ("  posting_rows={0} expected={1} duplicates={2} gt={3}" -f $smokeObj.posting_rows, $smokeObj.expected_postings, $smokeObj.duplicate_keys, $smokeObj.gt_sha256)
            if (-not $passed -and $smokeObj.error) { Log ("  smoke error: " + $smokeObj.error) }
        } else {
            Log "  smoke produced no parsable result; last 25 lines:"
            if (Test-Path $smokeOutFile) { Get-Content $smokeOutFile -Tail 25 | ForEach-Object { Log "  | $_" } } else { Log "  | (no smoke stdout captured)" }
        }
        $key = if ($mech -eq 'M0') { 'acceptance4a_M0_e2e' } else { 'acceptance4b_M3_e2e' }
        $result.tests[$key] = [ordered]@{ passed = $passed; script_exit = $smokeExit; smoke = $smokeObj }
        Log "TEST $($key): $(if ($passed) { 'PASS' } else { 'FAIL' })"
    }

    # ---- acceptance 5: crash failpoint --------------------------------------------------------
    Log "ACCEPTANCE 5: failpoint crash before_effect_commit"
    $out5 = & $Smoke -Mechanism M0 -Events 200 -Partitions $Partitions -SeedHex $SeedHex -CrashAt before_effect_commit 2>&1
    $smokeExit5 = $LASTEXITCODE
    $crashOutFile = Join-Path $EvidenceDir 'smoke-crash-stdout.txt'
    ($out5 | Out-String) | Set-Content -Path $crashOutFile -Encoding utf8
    $out5 | ForEach-Object { Add-Content -Path $LogPath -Value $_ }
    $smoke5 = Get-SmokeResult 'm0' $out5
    $passed5 = [bool]($smoke5 -and $smoke5.ok -and $smoke5.service_exit_code -eq 99 -and $smoke5.fired_count -eq 1)
    Log "crash smoke passed=$passed5 (script exit=$smokeExit5)`n  exit=$($smoke5.service_exit_code) fired=$($smoke5.fired_count) restored=$($smoke5.restored_count)"
    if ($smoke5 -and -not $passed5 -and $smoke5.error) { Log ("  crash smoke error: " + $smoke5.error) }
    if (-not $smoke5) {
        Log "  crash smoke produced no parsable result; last 25 lines:"
        if (Test-Path $crashOutFile) { Get-Content $crashOutFile -Tail 25 | ForEach-Object { Log "  | $_" } } else { Log "  | (no crash stdout captured)" }
    }
    $result.tests['acceptance5_failpoint_crash'] = [ordered]@{ passed = $passed5; script_exit = $smokeExit5; smoke = $smoke5 }
    Log "TEST acceptance5_failpoint_crash: $(if ($passed5) { 'PASS' } else { 'FAIL' })"

    # ---- honest summary ----------------------------------------------------------------------
    $all = $true
    foreach ($k in $result.tests.Keys) { if (-not $result.tests[$k].passed) { $all = $false } }
    $result.ok = $all
    if ($all) {
        Log "ALL ACCEPTANCE TESTS PASSED"
    } else {
        $failed = @($result.tests.Keys | Where-Object { -not $result.tests[$_].passed })
        Log ("ACCEPTANCE TESTS FAILED: " + ($failed -join ', '))
        $result.error = "failed tests: " + ($failed -join ', ')
    }
}
catch {
    $result.error = $_.Exception.Message
    Log "ACCEPTANCE SUITE ERROR: $($_.Exception.Message)"
}
finally {
    Log "stopping stack"
    try { & $Harness 'stop-toxiproxy' 2>&1 | Out-Null } catch { }
    try { & $Harness 'stop-kafka' 2>&1 | Out-Null } catch { }
    try { & $Harness 'stop-pg' 2>&1 | Out-Null } catch { }
    $result['finished_utc'] = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    ($result | ConvertTo-Json -Depth 14) | Set-Content -Path $ResultPath -Encoding utf8
    Log "evidence written to $ResultPath"
    Write-Host ($result | ConvertTo-Json -Depth 2)
}
if ($result.ok) { exit 0 } else { exit 1 }
