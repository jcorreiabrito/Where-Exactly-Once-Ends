<#
.SYNOPSIS
  Confidentiality / secret scanner. Runs before every push and at submission time.
.DESCRIPTION
  Scans git-tracked files (or a directory) for:
   - entries from the local confidential deny-list (tools/forbidden.local.txt, gitignored)
   - employer-identifying / internal-TLD patterns
   - AWS account IDs / access keys, JWTs, private keys, generic password assignments
   - personal machine paths & usernames
  Binary files (pdf/bin/image/archive) are skipped: they are verified by the pdftotext pass at
  submission time (see -PdfText mode note at the end of this file).
  Exits 1 on any hit. The deny-list file itself is never committed.
.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools/guard-secrets.ps1
#>
param(
  [string]$Root = (Get-Location).Path,
  [switch]$IncludeUntracked,
  [switch]$Verbose
)
$ErrorActionPreference = 'Stop'
$fail = 0
$hits = @()

$denyFile = Join-Path $Root 'tools/forbidden.local.txt'
$denyTerms = @()
if (Test-Path $denyFile) {
  $denyTerms = Get-Content $denyFile | Where-Object { $_ -and -not $_.StartsWith('#') } | ForEach-Object { $_.Trim() }
}

Push-Location $Root
try {
  $files = @()
  if (Test-Path (Join-Path $Root '.git')) {
    $files = @(git ls-files 2>$null)
    if ($IncludeUntracked) { $files += @(git ls-files --others --exclude-standard 2>$null) }
  } else {
    $files = @(Get-ChildItem -Recurse -File | ForEach-Object { $_.FullName.Substring($Root.Length).TrimStart('\','/') })
  }

  $excludePrefixes = @('tools/guard-secrets.ps1','tools/forbidden.local.txt','.git/','tools/vendor/',
    'artifact/target/','data/kafka/','data/postgres/','data/raw/lit/','lit/_discovery/')
  $binaryExt = @('.pdf','.bin','.png','.jpg','.jpeg','.gif','.zip','.gz','.tgz','.exe','.dll','.jar','.class','.woff','.woff2','.ttf','.hprof')

  $candidates = @()
  foreach ($f0 in $files) {
    $f = "$f0" -replace '\\','/'
    $skip = $false
    foreach ($e in $excludePrefixes) { if ($f.StartsWith($e) -or $f -eq $e) { $skip = $true } }
    $ext = [System.IO.Path]::GetExtension($f).ToLowerInvariant()
    if ($binaryExt -contains $ext) { $skip = $true }
    if (-not $skip -and (Test-Path -LiteralPath (Join-Path $Root $f))) { $candidates += $f }
  }

  # Rules. 'Scope' restricts a noisy rule to prose/config/code, never to data dumps.
  $rules = @(
    @{ Name='AWS access key';       Regex='AKIA[0-9A-Z]{16}'; Scope='all' },
    @{ Name='AWS secret key';       Regex='(?i)aws_secret_access_key\s*[:=]\s*[A-Za-z0-9/+=]{40}'; Scope='all' },
    @{ Name='AWS account id';       Regex='(?i)(aws[_ -]?account|account[_ -]?id|aws_acct)[^0-9]{0,20}\d{12}'; Scope='prose' },
    @{ Name='Private key block';    Regex='-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----'; Scope='all' },
    @{ Name='JWT';                  Regex='\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'; Scope='all' },
    @{ Name='GitHub token';         Regex='\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36}\b'; Scope='all' },
    @{ Name='Generic secret assign';Regex='(?i)(password|passwd|secret|api[_-]?key|access[_-]?token)\s*[:=]\s*["'']?[A-Za-z0-9_\-]{12,}'; Scope='prose' },
    @{ Name='Personal Windows path';Regex='C:\\Users\\jvtri|/home/jvtri'; Scope='all' },
    @{ Name='Internal TLD';         Regex='(?i)\b[a-z0-9-]+\.(internal|corp|intranet|lan|localdomain)\b'; Scope='prose' },
    @{ Name='Jira ticket';          Regex='\b(?!(SHA|UTC|ISO|RFC|HTTP|HTTPS|TLS|SSL|AES|RSA|MD|HDR|TCP|UDP|IP|GB|MB|KB|JDK|JRE|LTS|KIP|API|CPU|SQL|EOS|DB|PG|MSR|TR|F|M|C|K|P|S1|S2|S3|S4|S5|S6|S7|S8|S9)\b)[A-Z][A-Z0-9]{1,5}-\d{2,5}\b'; Scope='prose' }
  )
  $proseExt = @('.md','.tex','.ps1','.psm1','.java','.kt','.yml','.yaml','.sql','.properties','.toml','.sh','.py','.cff','.cls','.sty','.bib')
  $proseNames = @('LICENSE','README')

  foreach ($f in $candidates) {
    try { $text = Get-Content -LiteralPath (Join-Path $Root $f) -Raw -ErrorAction Stop } catch { continue }
    if (-not $text) { continue }
    $ext = [System.IO.Path]::GetExtension($f).ToLowerInvariant()
    $isProse = ($proseExt -contains $ext) -or ($proseNames -contains [System.IO.Path]::GetFileName($f))
    $lineNo = 0
    foreach ($line in ($text -split "`n")) {
      $lineNo++
      foreach ($t in $denyTerms) {
        if ($t -and $line.IndexOf($t, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
          $hits += [pscustomobject]@{File=$f;Line=$lineNo;Rule="deny-list:$t";Text=$line.Trim().Substring(0,[Math]::Min(110,$line.Trim().Length))}
        }
      }
      foreach ($r in $rules) {
        if ($r.Scope -eq 'prose' -and -not $isProse) { continue }
        if ($line -cmatch $r.Regex) {
          $hits += [pscustomobject]@{File=$f;Line=$lineNo;Rule=$r.Name;Text=$line.Trim().Substring(0,[Math]::Min(110,$line.Trim().Length))}
        }
      }
    }
  }
} finally { Pop-Location }

if ($Verbose) { Write-Host "SCANNED FILES: $($candidates.Count)" }
if ($hits.Count -gt 0) {
  Write-Host "SECRET/CONFIDENTIALITY SCAN: $($hits.Count) HIT(S)" -ForegroundColor Red
  $hits | Format-Table -AutoSize | Out-String -Width 240 | Write-Host
  $fail = 1
} else {
  Write-Host "SECRET/CONFIDENTIALITY SCAN: CLEAN ($($candidates.Count) text files scanned)" -ForegroundColor Green
}
if ($denyTerms.Count -eq 0) { Write-Host "WARNING: tools/forbidden.local.txt missing - only generic patterns applied." -ForegroundColor Yellow }
exit $fail
