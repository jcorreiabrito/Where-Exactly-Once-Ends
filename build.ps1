<#
.SYNOPSIS
  Reproducible LaTeX build for the RITA manuscript (pdflatex -> bibtex -> pdflatex x2).
.DESCRIPTION
  latexmk is unusable on this machine (no Perl). This script runs the four passes explicitly,
  parses the log and exits non-zero on any LaTeX error, undefined citation/reference or missing PDF.
.PARAMETER Main
  Main .tex file (default main.tex).
.PARAMETER Dir
  Directory containing the main file (default: this script's directory).
.PARAMETER Clean
  Remove build artefacts before compiling.
.PARAMETER OutDir
  Optional directory to copy the final PDF to.
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File build.ps1 -Main main.tex -Clean
#>
param(
  [string]$Main = 'main.tex',
  [string]$Dir = '',
  [switch]$Clean,
  [string]$OutDir = ''
)
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Dir)) { $Dir = $here }
$Dir = (Resolve-Path -LiteralPath $Dir).Path
$mainPath = Join-Path $Dir $Main
if (-not (Test-Path $mainPath)) { Write-Host "FATAL: main file not found: $mainPath" -ForegroundColor Red; exit 2 }
$job = [System.IO.Path]::GetFileNameWithoutExtension($Main)

Push-Location $Dir
try {
  $auxExt = @('.aux','.bbl','.blg','.log','.out','.toc','.lof','.lot','.fls','.fdb_latexmk','.synctex.gz','.nav','.snm','.vrb','.run.xml','.bcf')
  if ($Clean) {
    foreach ($e in $auxExt) { Get-ChildItem -File -Filter "$job$e" -ErrorAction SilentlyContinue | Remove-Item -Force }
    Write-Host "CLEAN: removed artefacts for $job" -ForegroundColor DarkGray
  }

  function Invoke-Pass([string[]]$exeArgs, [string]$label) {
    Write-Host "== $label ==" -ForegroundColor Cyan
    $out = & pdflatex @exeArgs 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
      Write-Host $out
      throw "pdflatex failed on $label with exit code $LASTEXITCODE"
    }
  }

  # Pass 1
  Invoke-Pass @('-interaction=nonstopmode','-halt-on-error','-file-line-error', $Main) 'pdflatex pass 1'

  # Bibliography
  if (Test-Path "$job.aux") {
    $bibCandidates = @()
    if (Test-Path "$job.bib") { $bibCandidates += $job }
    $auxText = Get-Content "$job.aux" -Raw
    if ($auxText -match '\\citation') {
      Write-Host "== bibtex ==" -ForegroundColor Cyan
      $bibOut = & bibtex $job 2>&1 | Out-String
      Write-Host $bibOut
      if ($LASTEXITCODE -ne 0) { throw "bibtex failed with exit code $LASTEXITCODE" }
      if ($bibOut -match '(?im)^(I found no|I couldn''t open|error message)') {
        throw "bibtex reported an error: $bibOut"
      }
    }
  }

  # Passes 2-3 (resolve refs/citations)
  Invoke-Pass @('-interaction=nonstopmode','-halt-on-error','-file-line-error', $Main) 'pdflatex pass 2'
  Invoke-Pass @('-interaction=nonstopmode','-halt-on-error','-file-line-error', $Main) 'pdflatex pass 3'

  $pdf = Join-Path $Dir "$job.pdf"
  if (-not (Test-Path $pdf)) { throw "PDF was not produced: $pdf" }

  # Log analysis
  $logPath = Join-Path $Dir "$job.log"
  $log = Get-Content $logPath -Raw
  $problems = @()
  if ($log -match '(?m)^! ') { $problems += ($log -split "`n" | Where-Object { $_ -match '^! ' }) }
  if ($log -match 'LaTeX Error') { $problems += 'LaTeX Error found in log' }
  if ($log -match 'Citation `[^'']+'' undefined') { $problems += ($log -split "`n" | Where-Object { $_ -match 'Citation .* undefined' }) }
  if ($log -match 'Reference `[^'']+'' undefined') { $problems += ($log -split "`n" | Where-Object { $_ -match 'Reference .* undefined' }) }
  if ($log -match 'There were undefined references') { $problems += 'There were undefined references' }
  if ($log -match 'Rerun to get') { $problems += 'LaTeX asks for another rerun' }
  if ($problems.Count -gt 0) {
    Write-Host "`nBUILD PROBLEMS:" -ForegroundColor Red
    $problems | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
    throw "log analysis found $($problems.Count) problem(s)"
  }

  $pages = ([regex]::Match($log, 'Output written on .*?\((\d+) pages?')).Groups[1].Value
  $size = (Get-Item $pdf).Length
  Write-Host "`nBUILD OK: $pdf ($pages pages, $size bytes)" -ForegroundColor Green

  if (-not [string]::IsNullOrWhiteSpace($OutDir)) {
    if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }
    Copy-Item -Force $pdf (Join-Path $OutDir "$job.pdf")
    Write-Host "COPIED: $(Join-Path $OutDir "$job.pdf")" -ForegroundColor Green
  }
} finally {
  Pop-Location
}
