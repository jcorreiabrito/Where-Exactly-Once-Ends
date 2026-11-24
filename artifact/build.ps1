# Build helper for the wxeo-service artifact.
# Runs the vendored Maven 3.9.9 on the vendored JDK 21 (the only supported compiler),
# so the host's Java 22 installation is never used.
#
#   powershell -ExecutionPolicy Bypass -File artifact/build.ps1 test         # unit tests (no Docker)
#   powershell -ExecutionPolicy Bypass -File artifact/build.ps1 package      # runnable flat jar
#   powershell -ExecutionPolicy Bypass -File artifact/build.ps1 verify -Pintegration   # native-stack ITs
param(
    [Parameter(Position = 0)][string]$Goal = 'package',
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Rest
)

$ErrorActionPreference = 'Stop'
$runtime = if ($env:EOS_RUNTIME_ROOT) { $env:EOS_RUNTIME_ROOT } else { Join-Path $env:LOCALAPPDATA 'eos-runtime' }
$jdk  = Join-Path $runtime 'vendor\jdk21'
$mvn  = Join-Path $runtime 'vendor\maven\bin\mvn.cmd'
if (-not (Test-Path $jdk)) { throw "JDK 21 not found at $jdk (set EOS_RUNTIME_ROOT)" }
if (-not (Test-Path $mvn)) { throw "Maven not found at $mvn (set EOS_RUNTIME_ROOT)" }

$env:JAVA_HOME = $jdk
$env:PATH = (Join-Path $jdk 'bin') + ';' + $env:PATH
$pom = Join-Path $PSScriptRoot 'pom.xml'
$arguments = @('-B', '-ntp', '-f', $pom, $Goal) + $Rest
Write-Host "JAVA_HOME=$jdk"
Write-Host "mvn $($arguments -join ' ')"
& $mvn @arguments
exit $LASTEXITCODE
