<#
.SYNOPSIS
  Machine-check the bounded TLA+ model of the ledger pipeline (formal/ledger.tla) with TLC.
.DESCRIPTION
  Requires:
    - tla2tools.jar vendored at %LOCALAPPDATA%\eos-runtime\vendor\tla\tla2tools.jar
      (recorded version + SHA-256 in tools/TOOLCHAIN-VERSIONS.txt)
    - the eight configurations in formal/cfg/*.cfg (bounds N=3, BatchSize=2)
  Writes one log per configuration to formal/tlc-results/<config>.txt and a summary JSON.
  Expected outcomes (pre-registered in formal/README.md):
    01-naive        -> I3Unique violated   (W1 duplicate)
    02-dedup        -> no error
    03-atomic       -> no error
    04-atomic-dedup -> no error
    05-kafka-txn    -> no error
    06-txn-only     -> I3Unique violated   (W1 duplicate; stream/ledger divergence)
    07-early-probe  -> I4CompleteWhenDone violated (W2 loss)
    08-early-dedup  -> I4CompleteWhenDone violated (dedup cannot restore a lost posting)
  Exit code 0 iff every configuration matches its expected outcome.
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools/run-tlc.ps1
#>
param(
  [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path,
  [string]$JavaHome = (Join-Path $env:LOCALAPPDATA 'eos-runtime\vendor\jdk21'),
  [string]$TlaJar = (Join-Path $env:LOCALAPPDATA 'eos-runtime\vendor\tla\tla2tools.jar')
)
$ErrorActionPreference = 'Continue'
$java = Join-Path $JavaHome 'bin\java.exe'
if (-not (Test-Path $java)) { throw "Java not found: $java (run tools/bootstrap-toolchain.ps1)" }
if (-not (Test-Path $TlaJar)) { throw "tla2tools.jar not found: $TlaJar (see formal/README.md)" }
$spec = Join-Path $Root 'formal\ledger.tla'
$cfgDir = Join-Path $Root 'formal\cfg'
$outDir = Join-Path $Root 'formal\tlc-results'
$metaDir = Join-Path $env:LOCALAPPDATA 'eos-runtime\tlc'
New-Item -ItemType Directory -Force -Path $outDir, $metaDir | Out-Null

$expected = [ordered]@{
  '01-naive'        = 'I3Unique'
  '02-dedup'        = 'none'
  '03-atomic'       = 'none'
  '04-atomic-dedup' = 'none'
  '05-kafka-txn'    = 'none'
  '06-txn-only'     = 'I3Unique'
  '07-early-probe'  = 'I4CompleteWhenDone'
  '08-early-dedup'  = 'I4CompleteWhenDone'
}
$summary = @()
$allMatch = $true
$tlcVersion = ''
foreach ($name in $expected.Keys) {
  $cfg = Join-Path $cfgDir "$name.cfg"
  if (-not (Test-Path $cfg)) { throw "Missing TLC config: $cfg" }
  $runMeta = Join-Path $metaDir ($name + '-' + (Get-Date).ToUniversalTime().ToString('HHmmssfff'))
  New-Item -ItemType Directory -Force -Path $runMeta | Out-Null
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  $out = & $java -XX:+UseParallelGC -jar $TlaJar -config $cfg -workers 2 -deadlock -metadir $runMeta $spec 2>&1 | Out-String
  $sw.Stop()
  $exit = $LASTEXITCODE
  # Sanitize absolute local paths from the public log before writing it.
  $sanitized = $out -replace [regex]::Escape($Root), '<repo>'
  $sanitized = $sanitized -replace [regex]::Escape($env:TEMP), '<tmp>'
  $sanitized | Set-Content -Path (Join-Path $outDir "$name.txt") -Encoding utf8
  if (-not $tlcVersion -and $out -match 'TLC2 Version [^\r\n]+') { $tlcVersion = $matches[0].Trim() }
  $violated = 'none'
  if ($out -match 'Invariant (\w+) is violated') { $violated = $matches[1] }
  elseif ($out -match 'Invariant (\w+) violated') { $violated = $matches[1] }
  $noError = ($out -match 'No error has been found')
  $states = if ($out -match '(\d+) states generated, (\d+) distinct states found') { [int]$matches[2] } else { -1 }
  $match = ($violated -eq $expected[$name])
  if (-not $match) { $allMatch = $false }
  $summary += [pscustomobject]@{
    Config = $name; Expected = $expected[$name]; Observed = $violated; Match = $match
    NoError = $noError; DistinctStates = $states; Exit = $exit; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1)
  }
  Write-Output ("{0}: expected={1} observed={2} match={3} states={4} ({5:N1}s)" -f $name, $expected[$name], $violated, $match, $states, $sw.Elapsed.TotalSeconds)
}
$summary | Format-Table -AutoSize | Out-String | Write-Output
$meta = [ordered]@{
  checked_utc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
  tlc_version = $(if ($tlcVersion) { $tlcVersion } else { 'unknown' })
  tla2tools_sha256 = (Get-FileHash -Algorithm SHA256 $TlaJar).Hash
  bounds = @{ N = 3; BatchSize = 2; partitions = 1 }
  results = $summary
}
$meta | ConvertTo-Json -Depth 5 | Set-Content -Path (Join-Path $outDir 'tlc-summary.json') -Encoding utf8
if ($allMatch) { Write-Output 'TLC CHECK: ALL CONFIGURATIONS MATCH PRE-REGISTERED EXPECTATIONS'; exit 0 }
else { Write-Output 'TLC CHECK: MISMATCH - revise the model or the propositions (do not tune expectations)'; exit 1 }
