<#
.SYNOPSIS
  Portable, pinned toolchain bootstrap for the Kafka/PostgreSQL experiment harness.
.DESCRIPTION
  Downloads and extracts, without administrator rights:
    - Eclipse Temurin JDK 21 (Windows x64)
    - Apache Maven 3.9.9
    - Apache Kafka 3.7.2 (Scala 2.13, KRaft mode)
    - PostgreSQL 16.11 portable binaries (EnterpriseDB)
    - Toxiproxy 2.9.0 (server + cli, Windows amd64)
  Runtime data (Kafka logs, PostgreSQL data dir, broker configs) lives OUTSIDE the cloud-synced
  workspace by default, under %LOCALAPPDATA%\eos-runtime, so sync does not perturb measurements.
  Downloads are cached; checksums are written to vendor/VERSIONS.txt and committed.
.PARAMETER Force
  Re-download archives even if present.
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools/bootstrap-toolchain.ps1
#>
param([switch]$Force)
$ErrorActionPreference = 'Stop'

$RuntimeRoot = if ($env:EOS_RUNTIME_ROOT) { $env:EOS_RUNTIME_ROOT } else { Join-Path $env:LOCALAPPDATA 'eos-runtime' }
$Vendor   = Join-Path $RuntimeRoot 'vendor'
$Dl       = Join-Path $Vendor 'dl'
$Bin      = Join-Path $Vendor 'bin'
New-Item -ItemType Directory -Force -Path $Dl, $Bin | Out-Null

$items = @(
  @{ Name='jdk21.zip'; Url='https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jdk/hotspot/normal/eclipse'; Kind='zip'; Dest='jdk21' },
  @{ Name='apache-maven-3.9.9-bin.zip'; Url='https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.zip'; Kind='zip'; Dest='maven' },
  @{ Name='kafka_2.13-3.7.2.tgz'; Url='https://archive.apache.org/dist/kafka/3.7.2/kafka_2.13-3.7.2.tgz'; Kind='tgz'; Dest='kafka' },
  @{ Name='postgresql-16.11-1-windows-x64-binaries.zip'; Url='https://get.enterprisedb.com/postgresql/postgresql-16.11-1-windows-x64-binaries.zip'; Kind='zip'; Dest='pgsql' },
  @{ Name='toxiproxy-server-windows-amd64.exe'; Url='https://github.com/Shopify/toxiproxy/releases/download/v2.9.0/toxiproxy-server-windows-amd64.exe'; Kind='raw'; Dest='bin' },
  @{ Name='toxiproxy-cli-windows-amd64.exe'; Url='https://github.com/Shopify/toxiproxy/releases/download/v2.9.0/toxiproxy-cli-windows-amd64.exe'; Kind='raw'; Dest='bin' }
)

function Get-Curl { Join-Path $env:SystemRoot 'System32\curl.exe' }

foreach ($it in $items) {
  $path = Join-Path $Dl $it.Name
  $fresh = $false
  if ($Force) { Remove-Item -Force $path -ErrorAction SilentlyContinue }
  if (-not (Test-Path $path) -or (Get-Item $path).Length -lt 10000) {
    Write-Host "DOWNLOAD $($it.Name) <- $($it.Url)" -ForegroundColor Cyan
    & (Get-Curl) -sS -L --retry 3 --retry-delay 2 --connect-timeout 30 -o $path $it.Url
    if ($LASTEXITCODE -ne 0) { throw "download failed: $($it.Url)" }
    $fresh = $true
  } else { Write-Host "CACHED  $($it.Name)" -ForegroundColor DarkGray }

  $destPath = Join-Path $Vendor $it.Dest
  if ($it.Kind -eq 'raw') {
    Copy-Item -Force $path (Join-Path $Bin $it.Name)
    continue
  }
  if (-not (Test-Path $destPath) -or $fresh) {
    Write-Host "EXTRACT $($it.Name) -> $destPath" -ForegroundColor Cyan
    $tmp = Join-Path $Vendor ("_x_" + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
    if ($it.Kind -eq 'zip') { Expand-Archive -Path $path -DestinationPath $tmp -Force }
    else { & tar -xzf $path -C $tmp; if ($LASTEXITCODE -ne 0) { throw "tar failed for $($it.Name)" } }
    $inner = Get-ChildItem $tmp | Select-Object -First 1
    if (Test-Path $destPath) { Remove-Item -Recurse -Force $destPath }
    Move-Item -Path $inner.FullName -Destination $destPath
    Remove-Item -Recurse -Force $tmp
  }
}

# Record pinned versions + archive hashes (committed for reproducibility)
$verFile = Join-Path $Vendor 'VERSIONS.txt'
$lines = @("# Toolchain pinned by tools/bootstrap-toolchain.ps1 on $((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'))")
$lines += "runtime_root=$RuntimeRoot"
foreach ($it in $items) {
  $p = Join-Path $Dl $it.Name
  if (Test-Path $p) {
    $h = (Get-FileHash -Algorithm SHA256 $p).Hash
    $lines += ("{0} sha256={1} bytes={2}" -f $it.Name, $h, (Get-Item $p).Length)
  }
}
$javaExe = Join-Path $Vendor 'jdk21\bin\java.exe'
if (Test-Path $javaExe) { $lines += ('jdk_version=' + (& $javaExe -version 2>&1 | Select-Object -First 1)) }
$pgCtl = Join-Path $Vendor 'pgsql\bin\pg_ctl.exe'
if (Test-Path $pgCtl) { $lines += ('postgres_version=' + (& (Join-Path $Vendor 'pgsql\bin\postgres.exe') --version 2>&1)) }
$lines | Set-Content -Encoding utf8 $verFile
Write-Host "`nVERSIONS:" -ForegroundColor Green
Get-Content $verFile | Write-Host
Write-Host "`nToolchain ready at $Vendor" -ForegroundColor Green
