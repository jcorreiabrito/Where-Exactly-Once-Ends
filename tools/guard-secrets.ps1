<#
.SYNOPSIS
  Confidentiality / secret scanner. Runs before every push and at submission time.
.DESCRIPTION
  Scans git-tracked files (or a directory) for:
   - entries from the local confidential deny-list (tools/forbidden.local.txt, gitignored)
   - generic employer-identifying patterns kept OUT of the committed scanner
   - AWS account IDs / access keys, JWTs, private keys, generic password/token assignments
   - personal machine paths & usernames
  Exits 1 on any hit. The deny-list file is never committed to the public repo.
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools/guard-secrets.ps1
#>
param(
  [string]$Root = (Get-Location).Path,
  [switch]$IncludeUntracked
)
$ErrorActionPreference = 'Stop'
$fail = 0
$hits = @()

$denyFile = Join-Path $Root 'tools/forbidden.local.txt'
$denyTerms = @()
if (Test-Path $denyFile) {
  $denyTerms = Get-Content $denyFile | Where-Object { $_ -and -not $_.StartsWith('#') } | ForEach-Object { $_.Trim() }
}

# Which files to scan: tracked + (optionally) untracked, excluding vendor/build/ignored
Push-Location $Root
try {
  $files = @()
  if (Test-Path (Join-Path $Root '.git')) {
    $tracked = git ls-files
    if ($IncludeUntracked) {
      $untracked = git ls-files --others --exclude-standard
      $files = @($tracked) + @($untracked)
    } else { $files = @($tracked) }
  } else {
    $files = Get-ChildItem -Recurse -File | ForEach-Object { $_.FullName.Substring($Root.Length).TrimStart('\','/') }
  }
  $exclude = @('tools/guard-secrets.ps1','tools/forbidden.local.txt','.git/','tools/vendor/','artifact/target/','data/kafka/','data/postgres/','data/raw/lit/','lit/_discovery/')
  $files = $files | Where-Object {
    $f = $_ -replace '\\','/'
    $skip = $false
    foreach ($e in $exclude) { if ($f.StartsWith($e) -or $f -eq $e) { $skip = $true } }
    -not $skip -and (Test-Path (Join-Path $Root $f))
  }

  $genericPatterns = @(
    @{ Name='AWS access key';      Regex='AKIA[0-9A-Z]{16}' },
    @{ Name='AWS secret key';      Regex='(?i)aws_secret_access_key\s*[:=]\s*[A-Za-z0-9/+=]{40}' },
    @{ Name='AWS account id';      Regex='\b\d{12}\b' },
    @{ Name='Private key block';   Regex='-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----' },
    @{ Name='JWT';                 Regex='\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}' },
    @{ Name='GitHub token';        Regex='\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36}\b' },
    @{ Name='Generic secret assign'; Regex='(?i)(password|passwd|secret|api[_-]?key|access[_-]?token)\s*[:=]\s*["'']?[A-Za-z0-9_\-]{12,}' },
    @{ Name='Personal Windows path'; Regex='C:\\Users\\jvtri|/home/jvtri' },
    @{ Name='Internal TLD';        Regex='(?i)\b[a-z0-9-]+\.(internal|corp|intranet|lan|localdomain)\b' },
    @{ Name='Jira ticket';         Regex='\b(?!(SHA|UTC|ISO|RFC|HTTP|HTTPS|TLS|SSL|AES|RSA|MD|HDR|TCP|UDP|IP|GB|MB|KB|JDK|JRE|LTS|KIP|API|CPU|SQL|EOS|DB|PG|F|M|C|K|P)\b)[A-Z][A-Z0-9]{1,5}-\d{2,5}\b' }
  )

  foreach ($f in $files) {
    $full = Join-Path $Root $f
    try { $text = Get-Content -LiteralPath $full -Raw -ErrorAction Stop } catch { continue }
    if (-not $text) { continue }
    $lineNo = 0
    foreach ($line in ($text -split "`n")) {
      $lineNo++
      foreach ($t in $denyTerms) {
        if ($t -and $line.IndexOf($t, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
          $hits += [pscustomobject]@{File=$f;Line=$lineNo;Rule="deny-list:$t";Text=$line.Trim().Substring(0,[Math]::Min(120,$line.Trim().Length))}
        }
      }
      foreach ($p in $genericPatterns) {
        if ($line -match $p.Regex) {
          # Jira-ticket style false positives: allow conventional-commit trailers if clearly not tickets
          $hits += [pscustomobject]@{File=$f;Line=$lineNo;Rule=$p.Name;Text=$line.Trim().Substring(0,[Math]::Min(120,$line.Trim().Length))}
        }
      }
    }
  }
} finally { Pop-Location }

if ($hits.Count -gt 0) {
  Write-Host "SECRET/CONFIDENTIALITY SCAN: $($hits.Count) HIT(S)" -ForegroundColor Red
  $hits | Format-Table -AutoSize | Out-String -Width 240 | Write-Host
  $fail = 1
} else {
  Write-Host "SECRET/CONFIDENTIALITY SCAN: CLEAN ($($files.Count) files)" -ForegroundColor Green
}
if ($denyTerms.Count -eq 0) { Write-Host "WARNING: tools/forbidden.local.txt missing - only generic patterns applied." -ForegroundColor Yellow }
exit $fail
