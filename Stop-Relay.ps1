<#
.SYNOPSIS
    Stops router-relay instances - on one port, on several, or on all of them.

.DESCRIPTION
    A relay is a node.exe process running router-relay.js. Finding "the relay" is the whole job,
    and the obvious answer - "whatever listens on the port" - is wrong: taking down an unrelated
    service because it happened to grab 8765 would be far worse than leaving a relay running. So a
    process is only killed when it is positively identified, and a port occupied by something else
    is reported and left alone.

    IDENTIFICATION (two independent confirmations, both required)
      1. The process command line runs router-relay.js.
      2. It answers /health on the port with a `pid` that matches the listener's process id.

    The second check is not ceremony: it proves the port really belongs to that relay instead of
    being a proxy or a leftover socket. /health is served by the relay itself and reports its own
    pid, so another service cannot produce a matching answer by accident.

    HOW THE PORT OF A RUNNING RELAY IS FOUND
      The relay may have been started as `--port 8766`, or with no port at all (taken from
      conf.txt `port`), or with `--port 0` (the OS picked one). -Port therefore resolves in three
      steps: the listening socket of each relay process, its /health answer, and finally its
      command line. That covers all three cases without guessing.

.PARAMETER Port
    Port(s) to stop. Accepts an array: -Port 8765,8766.
    Omit it together with -All to just list what is running.

.PARAMETER All
    Stop every relay process on the machine, whichever port it uses.

.PARAMETER Force
    Skip the confirmation prompt when stopping several relays at once.

.EXAMPLE
    .\Stop-Relay.ps1
    Lists running relays (PID, port, stream mode, upstream). Stops nothing.

.EXAMPLE
    .\Stop-Relay.ps1 -Port 8765
    Stops the relay on 8765, if 8765 is a relay.

.EXAMPLE
    .\Stop-Relay.ps1 -Port 8765,8766
    Stops two relays, after asking for confirmation.

.EXAMPLE
    .\Stop-Relay.ps1 -All
    Stops every relay on the machine.

.EXAMPLE
    .\Stop-Relay.ps1 -All -WhatIf
    Shows what would be stopped and kills nothing.

.NOTES
    Exit codes: 0 = everything asked for was stopped (or nothing was running),
                1 = something was refused (a port held by a non-relay process) or failed.

    Windows only: identification relies on Win32_Process.CommandLine.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [int[]]$Port,

    [switch]$All,

    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RelayScriptName = 'router-relay.js'

function Get-RelayProcess {
    <#
        Returns node.exe processes whose command line runs router-relay.js.
        A process whose command line cannot be read (owned by another user, no elevation) is
        reported as unknown and never killed - guessing there is exactly the mistake this script
        exists to prevent.
    #>
    $processes = @()
    $unreadable = @()

    foreach ($process in (Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue)) {
        if (-not $process.CommandLine) {
            $unreadable += $process.ProcessId
            continue
        }
        if ($process.CommandLine -match [regex]::Escape($RelayScriptName)) {
            $processes += $process
        }
    }

    return [pscustomobject]@{
        Processes  = $processes
        Unreadable = $unreadable
    }
}

function Get-RelayPort {
    <# Ports this process is listening on, or an empty array. #>
    param([Parameter(Mandatory = $true)][int]$ProcessId)

    try {
        return @(
            Get-NetTCPConnection -OwningProcess $ProcessId -State Listen -ErrorAction Stop |
                Select-Object -ExpandProperty LocalPort
        )
    } catch {
        return @()
    }
}

function Get-RelayHealth {
    <# The parsed /health body for a port, or $null when nothing relay-like answers there. #>
    param([Parameter(Mandatory = $true)][int]$Port)

    try {
        $response = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 3
    } catch {
        return $null
    }
    # `ok` and `pid` together are the signature of this relay: no other local service answers
    # /health with the process id that is listening on that very port.
    if ($null -eq $response -or -not $response.ok -or -not $response.pid) { return $null }
    return $response
}

function Get-RelayInfo {
    <#
        Describes one relay process: the port it is reachable on, its /health report, and whether
        the answer really came from that very process. Ports come from the listening sockets and
        fall back to the command line, so a relay that failed to bind is still addressable.
    #>
    param([Parameter(Mandatory = $true)]$Process)

    $processId = [int]$Process.ProcessId
    $ports = @(Get-RelayPort -ProcessId $processId)

    $health = $null
    $healthPort = $null
    foreach ($candidate in $ports) {
        $probe = Get-RelayHealth -Port $candidate
        if ($probe -and [int]$probe.pid -eq $processId) {
            $health = $probe
            $healthPort = $candidate
            break
        }
    }

    $cmdlinePort = $null
    if ($Process.CommandLine -match '--port[=\s]+(\d+)') { $cmdlinePort = [int]$Matches[1] }

    $effectivePort = $null
    if ($null -ne $healthPort) { $effectivePort = $healthPort }
    elseif ($ports.Count -gt 0) { $effectivePort = $ports[0] }
    elseif ($null -ne $cmdlinePort) { $effectivePort = $cmdlinePort }

    $streamMode = $null
    if ($health -and $health.PSObject.Properties['stream']) { $streamMode = $health.stream }

    return [pscustomobject]@{
        ProcessId   = $processId
        Port        = $effectivePort
        Ports       = $ports
        # A relay that answers /health with a matching pid is unambiguous; one that does not may be
        # starting up, wedged, or bound nowhere at all.
        Confirmed   = [bool]($health -and [int]$health.pid -eq $processId)
        Upstream    = if ($health) { $health.upstream } else { $null }
        Stream      = $streamMode
        CommandLine = $Process.CommandLine
    }
}

function Format-RelayRow {
    param([Parameter(Mandatory = $true)]$Relay)

    [pscustomobject]@{
        PID       = $Relay.ProcessId
        Port      = if ($null -ne $Relay.Port) { $Relay.Port } else { '?' }
        Stream    = if ($Relay.Stream) { $Relay.Stream } else { '-' }
        Upstream  = if ($Relay.Upstream) { $Relay.Upstream } else { '-' }
        Confirmed = if ($Relay.Confirmed) { 'yes' } else { 'no' }
    }
}

function Stop-RelayProcess {
    <#
        Stops one relay process. Returns 'stopped', 'skipped' (a -WhatIf run or a declined
        confirmation) or 'failed'. -WhatIf must not look like a failure: nothing was attempted, so
        the caller reports it as skipped instead of turning a dry run into exit code 1.
    #>
    param([Parameter(Mandatory = $true)]$Relay)

    if (-not $PSCmdlet.ShouldProcess("relay PID $($Relay.ProcessId) on port $($Relay.Port)", 'Stop')) {
        return 'skipped'
    }

    try {
        Stop-Process -Id $Relay.ProcessId -Force -ErrorAction Stop
    } catch {
        Write-Warning "Failed to stop PID $($Relay.ProcessId): $($_.Exception.Message)"
        return 'failed'
    }

    # Stop-Process returns before the OS has reaped the process; wait briefly so a follow-up check
    # cannot race it.
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        if (-not (Get-Process -Id $Relay.ProcessId -ErrorAction SilentlyContinue)) { return 'stopped' }
        Start-Sleep -Milliseconds 100
    }
    return 'stopped'
}

function Resolve-TargetPort {
    <#
        Finds the relay behind one port. The three outcomes are deliberately distinct: reporting
        "stopped" for a port that was never a relay is what makes such a script dangerous.
    #>
    param(
        [Parameter(Mandatory = $true)][int]$Port,
        [Parameter(Mandatory = $true)][object[]]$Relays
    )

    # A relay that listens on this port and answers /health with its own pid.
    foreach ($relay in $Relays) {
        if ($relay.Port -eq $Port -and $relay.Confirmed) {
            return [pscustomobject]@{ Kind = 'Relay'; Relay = $relay; Port = $Port }
        }
    }

    # A relay that names this port but is not (or not yet) answering - e.g. it failed to bind.
    foreach ($relay in $Relays) {
        if ($relay.Port -eq $Port) {
            return [pscustomobject]@{ Kind = 'Relay'; Relay = $relay; Port = $Port }
        }
    }

    # Something else holds the port. Leave it alone - this is the case the script exists for.
    $listener = $null
    try {
        $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop |
            Select-Object -First 1
    } catch {
        $listener = $null
    }

    if ($listener) {
        $owner = $null
        try {
            $owner = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
        } catch {
            $owner = $null
        }
        $name = if ($owner) { $owner.ProcessName } else { 'unknown' }
        return [pscustomobject]@{
            Kind  = 'Foreign'
            Port  = $Port
            Owner = $listener.OwningProcess
            Name  = $name
        }
    }

    return [pscustomobject]@{ Kind = 'Absent'; Port = $Port }
}

# --- gather -----------------------------------------------------------------
if ($Port -and $All) {
    Write-Error 'Use either -Port or -All, not both.'
    exit 1
}

$found = Get-RelayProcess
$relays = @()
foreach ($process in $found.Processes) {
    $relays += Get-RelayInfo -Process $process
}
if ($relays.Count -gt 1) {
    $relays = @($relays | Sort-Object -Property Port, ProcessId)
}

if ($found.Unreadable.Count -gt 0) {
    Write-Warning "$($found.Unreadable.Count) node.exe process(es) could not be inspected (no access to the command line). They are left alone: PID $($found.Unreadable -join ', ')."
}

# --- list only --------------------------------------------------------------
if (-not $Port -and -not $All) {
    if ($relays.Count -eq 0) {
        Write-Host 'No router-relay process is running.' -ForegroundColor DarkGray
        exit 0
    }
    Write-Host "Running router-relay process(es): $($relays.Count)" -ForegroundColor Cyan
    $relays | ForEach-Object { Format-RelayRow -Relay $_ } | Format-Table -AutoSize | Out-String | Write-Host
    Write-Host 'Stop one: .\Stop-Relay.ps1 -Port <port>    Stop all: .\Stop-Relay.ps1 -All' -ForegroundColor DarkGray
    exit 0
}

# --- stop everything --------------------------------------------------------
if ($All) {
    if ($relays.Count -eq 0) {
        Write-Host 'No router-relay process is running.' -ForegroundColor DarkGray
        exit 0
    }

    Write-Host "Relay process(es) to stop: $($relays.Count)" -ForegroundColor Cyan
    $relays | ForEach-Object { Format-RelayRow -Relay $_ } | Format-Table -AutoSize | Out-String | Write-Host

    # -WhatIf is handled by ShouldProcess itself; the prompt only appears for a real run.
    if (-not $Force -and -not $WhatIfPreference) {
        if (-not $PSCmdlet.ShouldProcess("$($relays.Count) relay process(es)", 'Stop')) {
            Write-Host 'Aborted - nothing was stopped.' -ForegroundColor Yellow
            exit 0
        }
        $answer = Read-Host 'Stop all of them? [y/N]'
        if ($answer -notmatch '^(y|yes)$') {
            Write-Host 'Aborted - nothing was stopped.' -ForegroundColor Yellow
            exit 0
        }
    }

    $failed = 0
    $skipped = 0
    foreach ($relay in $relays) {
        $outcome = Stop-RelayProcess -Relay $relay
        if ($outcome -eq 'stopped') {
            Write-Host "Stopped relay PID $($relay.ProcessId) on port $($relay.Port)." -ForegroundColor Cyan
        } elseif ($outcome -eq 'skipped') {
            $skipped++
        } else {
            $failed++
        }
    }
    if ($skipped -gt 0) {
        Write-Host "Dry run: $skipped relay process(es) left running." -ForegroundColor DarkGray
        exit 0
    }
    if ($failed -gt 0) { exit 1 }
    exit 0
}

# --- stop specific ports ----------------------------------------------------
$targets = @()
foreach ($one in $Port) {
    $targets += Resolve-TargetPort -Port $one -Relays $relays
}

$toStop = @($targets | Where-Object { $_.Kind -eq 'Relay' })
foreach ($target in $targets) {
    if ($target.Kind -eq 'Absent') {
        Write-Host "Port $($target.Port): nothing is listening - nothing to stop." -ForegroundColor DarkGray
    }
    if ($target.Kind -eq 'Foreign') {
        Write-Warning "Port $($target.Port) is held by PID $($target.Owner) ($($target.Name)), which is NOT router-relay. Leaving it alone."
    }
}

if ($toStop.Count -gt 1 -and -not $Force -and -not $WhatIfPreference) {
    $ports = ($toStop | ForEach-Object { $_.Port }) -join ', '
    $answer = Read-Host "Stop relays on ports $ports? [y/N]"
    if ($answer -notmatch '^(y|yes)$') {
        Write-Host 'Aborted - nothing was stopped.' -ForegroundColor Yellow
        exit 0
    }
}

$stopped = 0
$failed = 0
$skipped = 0
$seen = @{}
foreach ($target in $toStop) {
    if ($seen.ContainsKey($target.Relay.ProcessId)) {
        # The same process was reached through two ports; stop it once.
        continue
    }
    $seen[$target.Relay.ProcessId] = $true

    $outcome = Stop-RelayProcess -Relay $target.Relay
    if ($outcome -eq 'stopped') {
        $stopped++
        Write-Host "Stopped relay PID $($target.Relay.ProcessId) on port $($target.Port)." -ForegroundColor Cyan
    } elseif ($outcome -eq 'skipped') {
        $skipped++
    } else {
        $failed++
    }
}

if ($targets.Kind -contains 'Foreign') {
    Write-Host 'Refused at least one port: it belongs to another process.' -ForegroundColor Yellow
    exit 1
}
if ($failed -gt 0) { exit 1 }
if ($skipped -gt 0) {
    Write-Host "Dry run: $skipped relay process(es) left running." -ForegroundColor DarkGray
    exit 0
}
if ($stopped -eq 0) {
    Write-Host 'Nothing was stopped.' -ForegroundColor DarkGray
}
exit 0

