#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Thin wrapper for the WXEO experiment runner (S4). All logic lives in experiments/runner.py.

.DESCRIPTION
    Runs the frozen per-run sequence (docs/prereg.md §4.1) for a stage of the matrix, or a
    fixture-backed dry-run. The pilot must not be started until S3's artifact acceptance tests
    pass and the lead confirms the P2 integration gate.

    Every invocation is itself an experiment command: it writes its own RUNLOG row and the
    runner logs every external command it executes.

.EXAMPLE
    pwsh -File experiments/run-matrix.ps1 -CheckEnv
    pwsh -File experiments/run-matrix.ps1 -DryRun -Fixture results/dryrun/fixtures/pilot-m2-f0-1k-p3-r00 -Stage pilot -Limit 2
    pwsh -File experiments/run-matrix.ps1 -Stage pilot
    pwsh -File experiments/run-matrix.ps1 -WriteDeviations
#>
[CmdletBinding()]
param(
    [ValidateSet('pilot', 'A', 'B', 'C', 'all')]
    [string]$Stage = 'pilot',
    [int]$Limit = 0,
    [string]$RunId = '',
    [switch]$DryRun,
    [string]$Fixture = '',
    [string]$Rate = 'max',
    [string]$ResultsDir = '',
    [switch]$NoResume,
    [switch]$CheckEnv,
    [switch]$WriteDeviations
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$runner = Join-Path $PSScriptRoot 'runner.py'
if (-not (Test-Path $runner)) { throw "runner.py not found at $runner" }

# Resolve Python: explicit env override, then the host's standard 3.12 install, then PATH.
$python = $env:WXEO_PYTHON
if (-not $python) {
    $candidate = 'C:\Program Files\Python312\python.exe'
    if (Test-Path $candidate) { $python = $candidate } else { $python = (Get-Command python -ErrorAction SilentlyContinue).Source }
}
if (-not $python) { throw 'Python 3.12 not found; set $env:WXEO_PYTHON to python.exe' }

$argsList = @($runner, '--stage', $Stage)
if ($Limit -gt 0) { $argsList += @('--limit', "$Limit") }
if ($RunId) { $argsList += @('--run-id', $RunId) }
if ($Rate) { $argsList += @('--rate', $Rate) }
if ($ResultsDir) { $argsList += @('--results-dir', $ResultsDir) }
if ($NoResume) { $argsList += '--no-resume' }
if ($CheckEnv) { $argsList += '--check-env' }
if ($WriteDeviations) { $argsList += '--write-deviations' }
if ($DryRun) {
    if (-not $Fixture) { throw '-DryRun requires -Fixture <dir>' }
    $argsList += @('--dry-run', '--fixture', $Fixture)
}

$utc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
Write-Host "[$utc] run-matrix: $python $($argsList -join ' ')" -ForegroundColor Cyan
& $python @argsList
exit $LASTEXITCODE
