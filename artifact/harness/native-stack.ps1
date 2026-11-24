# native-stack.ps1 -- native-process harness for "Where Exactly-Once Ends" (S3 artifact).
#
# No Docker, no admin rights, no WSL. Starts/stops a 3-node Kafka 3.7.2 KRaft cluster,
# PostgreSQL 16.11 and Toxiproxy 2.9.0 as native Windows processes, using the portable toolchain
# under $env:EOS_RUNTIME_ROOT\vendor (default %LOCALAPPDATA%\eos-runtime\vendor).
#
# Verbs: start-kafka | stop-kafka | start-pg | stop-pg | start-toxiproxy | stop-toxiproxy |
#        status | restart-all
#
# All runtime data goes to $env:EOS_RUNTIME_ROOT\run (local, non-synced scratch; D11).
# Every start/stop writes $env:EOS_RUNTIME_ROOT\run\stack-state.json with pids, command lines and
# log paths (docs/CONTRACTS.md §8) -- S4's injectors read the pids from there.
#
#   powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 start-kafka
#   powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 status
#   powershell -ExecutionPolicy Bypass -File artifact/harness/native-stack.ps1 restart-all
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('start-kafka', 'stop-kafka', 'start-pg', 'stop-pg', 'start-toxiproxy',
        'stop-toxiproxy', 'status', 'restart-all')]
    [string]$Verb,
    [switch]$Quiet
)

$ErrorActionPreference = 'Continue'
function ConvertTo-Hashtable($value) {
    # PowerShell 5.1 compatible recursive JSON object -> ordered hashtable conversion.
    if ($null -eq $value) { return $null }
    if ($value -is [System.Collections.IDictionary]) {
        $h = [ordered]@{}
        foreach ($k in $value.Keys) { $h[$k] = ConvertTo-Hashtable $value[$k] }
        return $h
    }
    if ($value -is [System.Management.Automation.PSCustomObject]) {
        $h = [ordered]@{}
        foreach ($p in $value.PSObject.Properties) { $h[$p.Name] = ConvertTo-Hashtable $p.Value }
        return $h
    }
    if ($value -is [System.Collections.IEnumerable] -and -not ($value -is [string])) {
        $arr = @()
        foreach ($item in $value) { $arr += ,(ConvertTo-Hashtable $item) }
        return $arr
    }
    return $value
}
$HarnessDir = $PSScriptRoot
$Stack = ConvertTo-Hashtable (Get-Content (Join-Path $HarnessDir 'stack.json') -Raw | ConvertFrom-Json)

$RuntimeRoot = if ($env:EOS_RUNTIME_ROOT) { $env:EOS_RUNTIME_ROOT } else { Join-Path $env:LOCALAPPDATA 'eos-runtime' }
$VendorRoot = Join-Path $RuntimeRoot $Stack.vendorRoot
$RunRoot = Join-Path $RuntimeRoot $Stack.runRoot
$StatePath = Join-Path $RuntimeRoot $Stack.statePath
$LogRoot = Join-Path $RunRoot 'logs'

function Write-Log([string]$msg) {
    if (-not $Quiet) { Write-Host ("[{0}] {1}" -f (Get-Date).ToUniversalTime().ToString('HH:mm:ss'), $msg) }
}
function Ensure-Dir([string]$p) { if (-not (Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null } }
function Resolve-Root([string]$rel) {
    if ([System.IO.Path]::IsPathRooted($rel)) { return $rel }
    return (Join-Path $RuntimeRoot $rel)
}
function UtcNow { return (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ') }

function Test-Port([string]$hostName, [int]$port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $client.BeginConnect($hostName, $port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(500)) { return $false }
        $client.EndConnect($iar)
        return $true
    } catch { return $false } finally { try { $client.Close() } catch { } }
}



function Get-ProcCommandLine([int]$procId) {
    try {
        $p = Get-CimInstance Win32_Process -Filter "ProcessId=$procId" -ErrorAction SilentlyContinue
        if ($p) { return $p.CommandLine } else { return $null }
    } catch { return $null }
}

function Test-ProcAlive([int]$procId) {
    if (-not $procId -or $procId -le 0) { return $false }
    return [bool](Get-Process -Id $procId -ErrorAction SilentlyContinue)
}

function Read-State {
    if (Test-Path $StatePath) {
        try { return (ConvertTo-Hashtable (Get-Content $StatePath -Raw | ConvertFrom-Json)) } catch { }
    }
    $s = [ordered]@{
        schema       = 'wxeo.stack-state.v1'
        updated_utc  = $null
        runtime_root = $RuntimeRoot
        components   = [ordered]@{}
    }
    return $s
}

function Save-State($state) {
    Ensure-Dir (Split-Path $StatePath -Parent)
    $state.updated_utc = (UtcNow)
    $state.runtime_root = $RuntimeRoot
    ($state | ConvertTo-Json -Depth 12) | Set-Content -Path $StatePath -Encoding utf8
}

function Set-Component($state, [string]$name, $value) {
    $state.components[$name] = $value
}

function Get-ToolchainVersions {
    $rel = $Stack.toolchainVersionsFile
    if (-not $rel) { return $null }
    $path = Join-Path $HarnessDir $rel
    if (-not (Test-Path $path)) { return $null }
    $map = [ordered]@{}
    foreach ($line in Get-Content $path) {
        if ($line -match '^([a-z0-9_]+)=(.*)$') { $map[$Matches[1]] = $Matches[2].Trim() }
    }
    return $map
}

function Assert-Toolchain($state) {
    $versions = Get-ToolchainVersions
    if (-not $versions) { Write-Log "WARN: tools/TOOLCHAIN-VERSIONS.txt not found; skipping version check"; return }
    $checks = [ordered]@{
        jdk        = $versions['jdk']
        maven      = $versions['maven']
        kafka      = $versions['kafka']
        postgresql = $versions['postgresql']
        toxiproxy  = $versions['toxiproxy']
    }
    $ok = $true
    foreach ($k in $checks.Keys) {
        $expected = $Stack.expectedVersions[$k]
        $actual = $checks[$k]
        if ($expected -and $actual -and ($actual -notlike "$expected*")) {
            Write-Log "WARN: toolchain version mismatch for ${k}: expected $expected, TOOLCHAIN-VERSIONS.txt says $actual"
            $ok = $false
        }
    }
    $state['toolchain'] = [ordered]@{ versions_file = $Stack.toolchainVersionsFile; checks = $checks; ok = $ok }
}

# ------------------------------------------------------------------------------------------------
# Kafka
# ------------------------------------------------------------------------------------------------
function Get-KafkaPaths {
    $kafkaDir = Join-Path $VendorRoot 'kafka'
    return [ordered]@{
        kafka  = $kafkaDir
        java   = Join-Path $VendorRoot 'jdk21\bin\java.exe'
        cp     = (Join-Path $kafkaDir 'libs\*')
        log4j  = (Join-Path $kafkaDir 'config\log4j.properties')
        work   = (Resolve-Root 'run/kafka')
    }
}

function Start-Kafka {
    $p = Get-KafkaPaths
    Assert-Toolchain (Read-State) | Out-Null
    if (-not (Test-Path $p.java)) { throw "JDK 21 not found at $($p.java) (set EOS_RUNTIME_ROOT)" }
    if (-not (Test-Path $p.kafka)) { throw "Kafka not found at $($p.kafka) (set EOS_RUNTIME_ROOT)" }
    Ensure-Dir $p.work
    Ensure-Dir $LogRoot

    $state = Read-State
    Assert-Toolchain $state

    # Cluster id (create once, reuse for idempotent restarts).
    $clusterIdFile = Resolve-Root $Stack.kafka.clusterIdFile
    if (Test-Path $clusterIdFile) {
        $clusterId = (Get-Content $clusterIdFile -Raw).Trim()
        Write-Log "Kafka cluster id (existing): $clusterId"
    } else {
        $out = & $p.java -cp $p.cp kafka.tools.StorageTool random-uuid 2>&1
        $clusterId = @($out | Where-Object { "$_" -match '^[A-Za-z0-9_-]{20,}$' } | Select-Object -Last 1)
        if (-not $clusterId) { throw "could not obtain Kafka cluster id; output: $out" }
        $clusterId = "$clusterId".Trim()
        Ensure-Dir (Split-Path $clusterIdFile -Parent)
        Set-Content -Path $clusterIdFile -Value $clusterId -Encoding ascii
        Write-Log "Kafka cluster id (new): $clusterId"
    }

    $nodes = @()
    foreach ($node in $Stack.kafka.nodes) {
        $nodeDir = Resolve-Root $node.dir
        $dataDir = Join-Path $nodeDir 'data'
        $logsDir = Join-Path $nodeDir 'logs'
        Ensure-Dir $dataDir; Ensure-Dir $logsDir
        $cfgPath = Join-Path $nodeDir 'server.properties'

        $lines = @()
        $lines += "process.roles=broker,controller"
        $lines += "node.id=$($node.id)"
        $lines += "controller.quorum.voters=$($Stack.kafka.controllerQuorumVoters)"
        $lines += "listeners=PLAINTEXT://127.0.0.1:$($node.brokerPort),CONTROLLER://127.0.0.1:$($node.controllerPort)"
        $lines += "advertised.listeners=PLAINTEXT://127.0.0.1:$($node.brokerPort)"
        $lines += "controller.listener.names=CONTROLLER"
        $lines += "inter.broker.listener.name=PLAINTEXT"
        $lines += ("log.dirs=" + ($dataDir -replace '\\', '/'))
        foreach ($k in $Stack.kafka.settings.Keys) { $lines += ("{0}={1}" -f $k, $Stack.kafka.settings[$k]) }
        Set-Content -Path $cfgPath -Value $lines -Encoding ascii

        if (-not (Test-Path (Join-Path $dataDir 'meta.properties'))) {
            Write-Log "formatting Kafka node $($node.id) ($dataDir)"
            $fmt = & $p.java -cp $p.cp kafka.tools.StorageTool format -t $clusterId -c $cfgPath 2>&1
            if ($LASTEXITCODE -ne 0) { throw "kafka-storage format failed for node $($node.id): $fmt" }
        }

        $jvm = @($Stack.kafka.jvmArgs)
        $argList = @()
        foreach ($a in $jvm) { $argList += $a }
        $argList += "-Dkafka.logs.dir=$logsDir"
        $argList += "-Dlog4j.configuration=file:$($p.log4j)"
        $argList += '-cp'
        $argList += $p.cp
        $argList += 'kafka.Kafka'
        $argList += $cfgPath
        $proc = Start-Process -FilePath $p.java -ArgumentList $argList `
            -RedirectStandardOutput (Join-Path $logsDir 'stdout.txt') `
            -RedirectStandardError (Join-Path $logsDir 'stderr.txt') `
            -WindowStyle Hidden -PassThru
        Write-Log "started Kafka node $($node.id) pid=$($proc.Id) broker=$($node.brokerPort) controller=$($node.controllerPort)"
        $nodes += [ordered]@{
            id              = $node.id
            broker_port     = $node.brokerPort
            controller_port = $node.controllerPort
            pid             = $proc.Id
            data_dir        = $dataDir
            config          = $cfgPath
            log_dir         = $logsDir
            command_line    = (Get-ProcCommandLine $proc.Id)
        }
    }

    # Readiness: all three broker ports listening and the cluster answers a broker API call.
    $deadline = (Get-Date).AddSeconds([int]$Stack.kafka.bootTimeoutSec)
    $ready = $false
    while ((Get-Date) -lt $deadline) {
        $portsUp = $true
        foreach ($node in $Stack.kafka.nodes) {
            if (-not (Test-Port '127.0.0.1' $node.brokerPort)) { $portsUp = $false; break }
        }
        if ($portsUp) {
            $apiOut = & $p.java -cp $p.cp kafka.admin.BrokerApiVersionsCommand --bootstrap-server 127.0.0.1:$($Stack.kafka.nodes[0].brokerPort) 2>&1
            if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        }
        Start-Sleep -Seconds 2
    }
    if (-not $ready) {
        foreach ($node in $Stack.kafka.nodes) {
            $logsDir = Join-Path (Resolve-Root $node.dir) 'logs'
            Write-Log "== node$($node.id) stderr tail =="
            if (Test-Path (Join-Path $logsDir 'stderr.txt')) { Get-Content (Join-Path $logsDir 'stderr.txt') -Tail 15 | ForEach-Object { Write-Log $_ } }
            Write-Log "== node$($node.id) server.log tail =="
            if (Test-Path (Join-Path $logsDir 'server.log')) { Get-Content (Join-Path $logsDir 'server.log') -Tail 25 | ForEach-Object { Write-Log $_ } }
        }
        throw "Kafka brokers did not become ready within $($Stack.kafka.bootTimeoutSec)s"
    }
    Write-Log "KAFKA UP (3 brokers, RF=3, min.insync.replicas=2, KRaft static quorum)"

    Set-Component $state 'kafka' ([ordered]@{
            status          = 'running'
            started_utc     = (UtcNow)
            cluster_id      = $clusterId
            nodes           = $nodes
            bootstrap       = "127.0.0.1:$($Stack.kafka.nodes[0].brokerPort)"
            controller_ports = @($Stack.kafka.nodes | ForEach-Object { $_.controllerPort })
            log_root        = $LogRoot
        })
    Save-State $state
}

function Stop-Kafka {
    $state = Read-State
    $stopped = @()
    $comp = $state.components['kafka']
    if ($comp -and $comp.nodes) {
        foreach ($node in $comp.nodes) {
            if (Test-ProcAlive ([int]$node.pid)) {
                try { Stop-Process -Id ([int]$node.pid) -Force -ErrorAction Stop; $stopped += $node.pid } catch { }
            }
        }
    }
    # Fallback: any java process running kafka.Kafka for this runtime root.
    $workPath = (Resolve-Root 'run/kafka') -replace '\\', '\\'
    Get-CimInstance Win32_Process -Filter "Name='java.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match 'kafka\.Kafka' -and $_.CommandLine -match 'eos-runtime' } |
        ForEach-Object {
            try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop; $stopped += $_.ProcessId } catch { }
        }
    Start-Sleep -Milliseconds 800
    $still = 0
    foreach ($node in $Stack.kafka.nodes) { if (Test-Port '127.0.0.1' $node.brokerPort) { $still++ } }
    if ($still -gt 0) { Write-Log "WARN: $still Kafka broker ports still listening after stop" }
    Write-Log "KAFKA DOWN (stopped pids: $($stopped -join ','))"
    Set-Component $state 'kafka' ([ordered]@{
            status      = 'stopped'
            stopped_utc = (UtcNow)
            stopped_pids = $stopped
            nodes       = @()
        })
    Save-State $state
}

# ------------------------------------------------------------------------------------------------
# PostgreSQL
# ------------------------------------------------------------------------------------------------
function Get-PgPaths {
    $bin = Join-Path $VendorRoot 'pgsql\bin'
    return [ordered]@{
        bin       = $bin
        dataDir   = (Resolve-Root $Stack.postgres.dataDir)
        logFile   = (Resolve-Root $Stack.postgres.logFile)
        port      = [int]$Stack.postgres.port
        host      = $Stack.postgres.host
        database  = $Stack.postgres.database
        adminRole = $Stack.postgres.roles.admin
        appRole   = $Stack.postgres.roles.app
        readRole  = $Stack.postgres.roles.read
        faultRole = $Stack.postgres.roles.fault
    }
}

function Ensure-ConfSetting([string]$ConfFile, [string]$Key, [string]$Value) {
    $lines = Get-Content $ConfFile -ErrorAction SilentlyContinue
    $pattern = "^\s*" + [regex]::Escape($Key) + "\s*="
    if (-not (@($lines | Where-Object { $_ -match $pattern }).Count -gt 0)) {
        Add-Content -Path $ConfFile -Value ("{0} = {1}" -f $Key, $Value) -Encoding ascii
    }
}
function Invoke-PgCtl([string[]]$PgCtlArgs, [string]$LogPrefix) {
    # pg_ctl start spawns postgres, which inherits the stdout pipe; in Windows PowerShell a plain
    # `& pg_ctl ...` call then blocks waiting for EOF even after pg_ctl exits. Start-Process with
    # redirected output avoids that hang and gives us the real exit code.
    $pg = Get-PgPaths
    Ensure-Dir $LogRoot
    $out = Join-Path $LogRoot ("{0}.out.txt" -f $LogPrefix)
    $err = Join-Path $LogRoot ("{0}.err.txt" -f $LogPrefix)
    $p = Start-Process -FilePath (Join-Path $pg.bin 'pg_ctl.exe') -ArgumentList $PgCtlArgs `
        -RedirectStandardOutput $out -RedirectStandardError $err -WindowStyle Hidden -PassThru -Wait
    if ($p.ExitCode -ne 0) {
        $msg = ""
        if (Test-Path $err) { $msg = (Get-Content $err -Raw) }
        if (Test-Path $out) { $msg = $msg + (Get-Content $out -Raw) }
        throw ("pg_ctl {0} failed (exit {1}): {2}" -f ($PgCtlArgs -join ' '), $p.ExitCode, $msg)
    }
}
function Invoke-Psql([string[]]$sqlArgs) {
    $pg = Get-PgPaths
    $out = & (Join-Path $pg.bin 'psql.exe') -v ON_ERROR_STOP=1 -h $pg.host -p $pg.port -U $pg.adminRole @sqlArgs 2>&1
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $out" }
    return $out
}

function Start-Pg {
    $pg = Get-PgPaths
    Assert-Toolchain (Read-State) | Out-Null
    if (-not (Test-Path (Join-Path $pg.bin 'pg_ctl.exe'))) { throw "PostgreSQL binaries not found at $($pg.bin) (set EOS_RUNTIME_ROOT)" }
    Ensure-Dir (Split-Path $pg.logFile -Parent)

    if (-not (Test-Path (Join-Path $pg.dataDir 'PG_VERSION'))) {
        Write-Log "initdb $($pg.dataDir)"
        Ensure-Dir $pg.dataDir
        $initOut = & (Join-Path $pg.bin 'initdb.exe') -D $pg.dataDir -U $pg.adminRole --auth=trust --encoding=UTF8 --locale=C 2>&1
        if ($LASTEXITCODE -ne 0) { throw "initdb failed: $initOut" }
        $confFile = Join-Path $pg.dataDir 'postgresql.conf'
        Add-Content -Path $confFile -Value "`n# ---- wxeo native-stack harness (Appendix C settings) ----" -Encoding ascii
        foreach ($k in $Stack.postgres.settings.Keys) {
            Add-Content -Path $confFile -Value ("{0} = {1}" -f $k, $Stack.postgres.settings[$k]) -Encoding ascii
        }
    }

    # Sanitise postgresql.conf: an earlier harness version wrote a relative hba_file entry, which
    # PostgreSQL resolves against the process working directory and which then fails to load.
    $confFile = Join-Path $pg.dataDir 'postgresql.conf'
    if (Test-Path $confFile) {
        $confLines = Get-Content $confFile
        $clean = @($confLines | Where-Object { $_ -notmatch '^\s*hba_file\s*=' })
        if ($clean.Count -ne $confLines.Count) {
            Set-Content -Path $confFile -Value $clean -Encoding ascii
            Write-Log "removed relative hba_file entry from postgresql.conf"
        }
    }
    # Remove a stale pid file left by an interrupted start.
    $pidFile = Join-Path $pg.dataDir 'postmaster.pid'
    if (Test-Path $pidFile) {
        $stale = 0
        try { $stale = [int]((Get-Content $pidFile -TotalCount 1).Trim()) } catch { }
        if (-not (Test-ProcAlive $stale)) {
            Remove-Item -Force $pidFile -ErrorAction SilentlyContinue
            Write-Log "removed stale postmaster.pid"
        }
    }
        # Ensure server-side logging (postgres writes its own log; we launch postgres.exe directly
        # so the postmaster pid is exactly what S4 needs and there is no pg_ctl stdout-pipe hang).
        Ensure-ConfSetting $confFile 'logging_collector' 'on'
        Ensure-ConfSetting $confFile 'log_directory' "'logs'"
        Ensure-ConfSetting $confFile 'log_filename' "'postgres.log'"
    if (Test-Port $pg.host $pg.port) {
        Write-Log "PostgreSQL already listening on $($pg.host):$($pg.port)"
    } else {
        Write-Log "starting PostgreSQL on $($pg.host):$($pg.port) (data=$($pg.dataDir))"
        $bootstrapDir = Split-Path $pg.logFile -Parent
        Ensure-Dir $bootstrapDir
        $startupOut = Join-Path $bootstrapDir 'startup.out.txt'
        $startupErr = Join-Path $bootstrapDir 'startup.err.txt'
        $proc = Start-Process -FilePath (Join-Path $pg.bin 'postgres.exe') -ArgumentList @('-D', $pg.dataDir) `
            -RedirectStandardOutput $startupOut -RedirectStandardError $startupErr -WindowStyle Hidden -PassThru
        Write-Log "launched postgres pid=$($proc.Id) (server log $($pg.logFile))"
    }

    # Readiness
    $deadline = (Get-Date).AddSeconds([int]$Stack.postgres.bootTimeoutSec)
    $ready = $false
    while ((Get-Date) -lt $deadline) {
        $r = & (Join-Path $pg.bin 'pg_isready.exe') -h $pg.host -p $pg.port 2>&1
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Milliseconds 500
    }
    if (-not $ready) { throw "PostgreSQL did not become ready within $($Stack.postgres.bootTimeoutSec)s" }
    Write-Log "PostgreSQL ready on $($pg.host):$($pg.port)"

    # Database + roles (idempotent).
    $dbExists = Invoke-Psql @('-d', 'postgres', '-tAc', ("SELECT 1 FROM pg_database WHERE datname='{0}'" -f $pg.database))
    if (("$dbExists".Trim()) -ne '1') {
        $createOut = & (Join-Path $pg.bin 'createdb.exe') -h $pg.host -p $pg.port -U $pg.adminRole $pg.database 2>&1
        if ($LASTEXITCODE -ne 0) { throw "createdb failed: $createOut" }
        Write-Log "created database $($pg.database)"
    }
    foreach ($role in @($pg.appRole, $pg.readRole, $pg.faultRole)) {
        $exists = Invoke-Psql @('-d', 'postgres', '-tAc', ("SELECT 1 FROM pg_roles WHERE rolname='{0}'" -f $role))
        if (("$exists".Trim()) -ne '1') {
            Invoke-Psql @('-d', 'postgres', '-c', ("CREATE ROLE {0} LOGIN" -f $role)) | Out-Null
            Write-Log "created role $role"
        }
    }
    Invoke-Psql @('-d', 'postgres', '-c', ("GRANT CONNECT ON DATABASE {0} TO {1}, {2}" -f $pg.database, $pg.appRole, $pg.readRole)) | Out-Null

    $postmasterPid = 0
    $pidFile = Join-Path $pg.dataDir 'postmaster.pid'
    if (Test-Path $pidFile) { $postmasterPid = [int]((Get-Content $pidFile -TotalCount 1).Trim()) }

    $state = Read-State
    Assert-Toolchain $state
    Set-Component $state 'postgres' ([ordered]@{
            status         = 'running'
            started_utc    = (UtcNow)
            host           = $pg.host
            port           = $pg.port
            pid            = $postmasterPid
            data_dir       = $pg.dataDir
            log_file       = $pg.logFile
            database       = $pg.database
            roles          = [ordered]@{ admin = $pg.adminRole; app = $pg.appRole; read = $pg.readRole; fault = $pg.faultRole }
            jdbc_url       = ("jdbc:postgresql://{0}:{1}/{2}" -f $pg.host, $pg.port, $pg.database)
            jdbc_url_proxy = ("jdbc:postgresql://127.0.0.1:15433/{0}" -f $pg.database)
            command_line   = (Get-ProcCommandLine $postmasterPid)
        })
    Save-State $state
    Write-Log "PG UP ($($pg.database) @ $($pg.host):$($pg.port), trust auth localhost)"
}

function Stop-Pg {
    $pg = Get-PgPaths
    $stopped = @()
    if (Test-Path (Join-Path $pg.dataDir 'PG_VERSION')) {
        try { Invoke-PgCtl @('-D', $pg.dataDir, 'stop', '-m', 'fast', '-w') 'pgctl-stop' } catch { Write-Log "pg_ctl stop: $($_.Exception.Message)" }
    }
    $pidFile = Join-Path $pg.dataDir 'postmaster.pid'
    if (Test-Path $pidFile) {
        try {
            $postmaster = [int]((Get-Content $pidFile -TotalCount 1).Trim())
            if (Test-ProcAlive $postmaster) { Stop-Process -Id $postmaster -Force -ErrorAction SilentlyContinue; $stopped += $postmaster }
        } catch { }
    }
    Start-Sleep -Milliseconds 500
    if (Test-Port $pg.host $pg.port) { Write-Log "WARN: PostgreSQL port still listening after stop" }
    Write-Log "PG DOWN"
    $state = Read-State
    Set-Component $state 'postgres' ([ordered]@{ status = 'stopped'; stopped_utc = (UtcNow); stopped_pids = $stopped })
    Save-State $state
}

# ------------------------------------------------------------------------------------------------
# Toxiproxy
# ------------------------------------------------------------------------------------------------
function Invoke-Toxi([string]$Method, [string]$Path, [string]$Body) {
    # Toxiproxy 2.9.0 rejects the default Windows PowerShell user agent with HTTP 403
    # ("User agent not allowed"); always send an explicit agent.
    $tp = Get-ToxiPaths
    $params = @{ Uri = ("$($tp.api)$Path"); Method = $Method; TimeoutSec = 10; UserAgent = 'wxeo-harness/1.0' }
    if ($Body) { $params['ContentType'] = 'application/json'; $params['Body'] = $Body }
    return (Invoke-RestMethod @params)
}
function Get-ToxiPaths {
    return [ordered]@{
        server = Join-Path $VendorRoot $Stack.toxiproxy.serverExe
        cli    = Join-Path $VendorRoot $Stack.toxiproxy.cliExe
        host   = $Stack.toxiproxy.apiHost
        port   = [int]$Stack.toxiproxy.apiPort
        api    = ("http://{0}:{1}" -f $Stack.toxiproxy.apiHost, $Stack.toxiproxy.apiPort)
        logDir = (Resolve-Root 'run/toxiproxy')
    }
}

function Start-Toxiproxy {
    $tp = Get-ToxiPaths
    if (-not (Test-Path $tp.server)) { throw "Toxiproxy server not found at $($tp.server) (set EOS_RUNTIME_ROOT)" }
    Ensure-Dir $tp.logDir
    $proc = $null
    if (Test-Port $tp.host $tp.port) {
        Write-Log "Toxiproxy API already listening on $($tp.host):$($tp.port)"
    } else {
        $proc = Start-Process -FilePath $tp.server -ArgumentList @('-host', $tp.host, '-port', "$($tp.port)") `
            -RedirectStandardOutput (Join-Path $tp.logDir 'stdout.txt') `
            -RedirectStandardError (Join-Path $tp.logDir 'stderr.txt') `
            -WindowStyle Hidden -PassThru
        $deadline = (Get-Date).AddSeconds([int]$Stack.toxiproxy.bootTimeoutSec)
        $ready = $false
        while ((Get-Date) -lt $deadline) {
            try {
                $null = Invoke-Toxi -Method Get -Path '/version'
                $ready = $true; break
            } catch { Start-Sleep -Milliseconds 300 }
        }
        if (-not $ready) { throw "Toxiproxy API did not become ready within $($Stack.toxiproxy.bootTimeoutSec)s" }
        Write-Log "started Toxiproxy pid=$($proc.Id) api=$($tp.api)"
    }

    # Recreate proxies so a restart yields a clean state.
    $existing = @{}
    try {
        $list = Invoke-Toxi -Method Get -Path '/proxies'
        foreach ($prop in $list.PSObject.Properties) { $existing[$prop.Name] = $prop.Value }
    } catch { }
    $proxies = @()
    foreach ($proxy in $Stack.toxiproxy.proxies) {
        if ($existing.ContainsKey($proxy.name)) {
            try { $null = Invoke-Toxi -Method Delete -Path ("/proxies/{0}" -f $proxy.name) } catch { }
        }
        $body = ([ordered]@{ name = $proxy.name; listen = $proxy.listen; upstream = $proxy.upstream; enabled = $true } | ConvertTo-Json -Compress)
        $created = Invoke-Toxi -Method Post -Path '/proxies' -Body $body
        Write-Log ("proxy {0}: {1} -> {2} (enabled={3})" -f $created.name, $created.listen, $created.upstream, $created.enabled)
        $proxies += [ordered]@{
            name     = $created.name
            listen   = $created.listen
            upstream = $created.upstream
            enabled  = $created.enabled
            role     = $proxy.role
        }
    }

    $state = Read-State
    Set-Component $state 'toxiproxy' ([ordered]@{
            status       = 'running'
            started_utc  = (UtcNow)
            api          = $tp.api
            pid          = if ($proc) { $proc.Id } else { 0 }
            proxies      = $proxies
            log_dir      = $tp.logDir
            command_line = if ($proc) { Get-ProcCommandLine $proc.Id } else { $null }
        })
    Save-State $state
    Write-Log "TOXIPROXY UP ($($tp.api))"
}

function Stop-Toxiproxy {
    $tp = Get-ToxiPaths
    $state = Read-State
    $stopped = @()
    $comp = $state.components['toxiproxy']
    if ($comp -and $comp.pid -and (Test-ProcAlive ([int]$comp.pid))) {
        try { Stop-Process -Id ([int]$comp.pid) -Force -ErrorAction Stop; $stopped += [int]$comp.pid } catch { }
    }
    Get-CimInstance Win32_Process -Filter "Name='toxiproxy-server-windows-amd64.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match 'eos-runtime' -or $_.ExecutablePath -match 'eos-runtime' } |
        ForEach-Object { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop; $stopped += $_.ProcessId } catch { } }
    Start-Sleep -Milliseconds 400
    Write-Log "TOXIPROXY DOWN (stopped pids: $($stopped -join ','))"
    Set-Component $state 'toxiproxy' ([ordered]@{ status = 'stopped'; stopped_utc = (UtcNow); stopped_pids = $stopped })
    Save-State $state
}

# ------------------------------------------------------------------------------------------------
# Status / restart
# ------------------------------------------------------------------------------------------------
function Show-Status {
    $state = Read-State
    $pg = Get-PgPaths
    $tp = Get-ToxiPaths
    $summary = [ordered]@{
        schema       = 'wxeo.stack.status.v1'
        timestamp    = (UtcNow)
        runtime_root = $RuntimeRoot
        state_path   = $StatePath
    }
    $kafkaComp = $state.components['kafka']
    $kafkaRunning = $false
    if ($kafkaComp -and $kafkaComp.nodes) {
        foreach ($node in $kafkaComp.nodes) {
            if ((Test-ProcAlive ([int]$node.pid)) -and (Test-Port '127.0.0.1' ([int]$node.broker_port))) { $kafkaRunning = $true }
        }
    }
    $kafkaStatus = 'stopped'
    if ($kafkaRunning) {
        $allUp = $true
        foreach ($node in $Stack.kafka.nodes) { if (-not (Test-Port '127.0.0.1' $node.brokerPort)) { $allUp = $false } }
        $kafkaStatus = if ($allUp) { 'running' } else { 'degraded' }
    }
    $summary['kafka'] = [ordered]@{
        status   = $kafkaStatus
        bootstrap = "127.0.0.1:$($Stack.kafka.nodes[0].brokerPort)"
        nodes    = @($Stack.kafka.nodes | ForEach-Object {
                [ordered]@{ id = $_.id; broker_port = $_.brokerPort; controller_port = $_.controllerPort; listening = (Test-Port '127.0.0.1' $_.brokerPort) }
            })
    }

    $postmasterPid = 0
    $pidFile = Join-Path $pg.dataDir 'postmaster.pid'
    if (Test-Path $pidFile) { try { $postmasterPid = [int]((Get-Content $pidFile -TotalCount 1).Trim()) } catch { } }
    $summary['postgres'] = [ordered]@{
        status   = if (Test-Port $pg.host $pg.port) { 'running' } else { 'stopped' }
        host     = $pg.host
        port     = $pg.port
        pid      = $postmasterPid
        database = $pg.database
        jdbc_url = ("jdbc:postgresql://{0}:{1}/{2}" -f $pg.host, $pg.port, $pg.database)
    }

    $tpAlive = $false
    try { $null = Invoke-Toxi -Method Get -Path '/version'; $tpAlive = $true } catch { }
    $summary['toxiproxy'] = [ordered]@{
        status = if ($tpAlive) { 'running' } else { 'stopped' }
        api    = $tp.api
        proxies = if ($tpAlive) { @(try { (Invoke-Toxi -Method Get -Path '/proxies').PSObject.Properties.Name } catch { @() }) } else { @() }
    }
    $summary['ports'] = [ordered]@{
        kafka_brokers = @($Stack.kafka.nodes | ForEach-Object { $_.brokerPort })
        kafka_controllers = @($Stack.kafka.nodes | ForEach-Object { $_.controllerPort })
        postgres      = $pg.port
        toxiproxy_api = $tp.port
        toxiproxy_listeners = @($Stack.toxiproxy.proxies | ForEach-Object { $_.listen })
    }
    ($summary | ConvertTo-Json -Depth 8)
}

function Restart-All {
    Write-Log "restart-all: stopping PostgreSQL, Kafka, Toxiproxy"
    try { Stop-Toxiproxy } catch { Write-Log "stop-toxiproxy: $($_.Exception.Message)" }
    try { Stop-Kafka } catch { Write-Log "stop-kafka: $($_.Exception.Message)" }
    try { Stop-Pg } catch { Write-Log "stop-pg: $($_.Exception.Message)" }
    Start-Sleep -Seconds 1
    Write-Log "restart-all: starting PostgreSQL, Kafka, Toxiproxy"
    Start-Pg
    Start-Kafka
    Start-Toxiproxy
    Write-Log "restart-all: done"
}

# ------------------------------------------------------------------------------------------------
switch ($Verb) {
    'start-kafka' { Start-Kafka }
    'stop-kafka' { Stop-Kafka }
    'start-pg' { Start-Pg }
    'stop-pg' { Stop-Pg }
    'start-toxiproxy' { Start-Toxiproxy }
    'stop-toxiproxy' { Stop-Toxiproxy }
    'status' { Show-Status }
    'restart-all' { Restart-All }
}
exit 0
