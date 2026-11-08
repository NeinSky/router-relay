<#
.SYNOPSIS
    Launcher for the Hermes Agent CLI, wired to the local router relay.

.DESCRIPTION
    Hermes is configured (config.yaml + .env) to use the local relay at http://127.0.0.1:8765/v1,
    which forwards to the router through the HAPP proxy. This script makes the two moving parts
    safe to forget:

      * it starts router-relay.js if the relay is not answering /health;
      * it warns when the HAPP proxy port is not listening (the relay needs it for upstream calls);
      * then it runs hermes.exe, forwarding every argument verbatim
        (for example: chat, model, doctor, -z "prompt", --reasoning high).

.PARAMETER SkipRelayCheck
    Do not touch the relay at all - just run Hermes.

.PARAMETER RelayPort
    Relay port to verify/start. Default: from conf.txt `port`, otherwise 8765.
    Pass it explicitly to point the launcher at a specific instance.

.PARAMETER HermesArgs
    Any remaining arguments are passed to the Hermes CLI unchanged.

.EXAMPLE
    .\Start-Hermes.ps1
    Opens the Hermes TUI.

.EXAMPLE
    .\Start-Hermes.ps1 -z "Reply with exactly: OK"
    One-shot prompt through the relay.

.EXAMPLE
    .\Start-Hermes.ps1 doctor
    Runs Hermes' own configuration/dependency check.
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [switch]$SkipRelayCheck,

    [int]$RelayPort = 0,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$HermesArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Read-ApiConf {
    param([Parameter(Mandatory = $true)][string]$Path)

    $values = @{}
    foreach ($line in (Get-Content -LiteralPath $Path)) {
        $entry = $line.Trim()
        if (-not $entry -or $entry.StartsWith('#') -or $entry.StartsWith(';')) { continue }

        $separator = $entry.IndexOf('=')
        if ($separator -lt 1) { continue }

        $key = $entry.Substring(0, $separator).Trim()
        $value = $entry.Substring($separator + 1).Trim()
        if ($key) { $values[$key] = $value }
    }

    return $values
}

function Resolve-RelayPort {
    <#
        Resolves the relay port: explicit -RelayPort, then conf.txt `port`, then 8765.
        conf.txt `port=0` means "any free port" - which a launcher cannot use, because it has to know
        the address it verifies. A concrete free port is chosen instead.
    #>
    param([int]$Requested)

    if ($Requested -gt 0) { return $Requested }

    $resolved = 8765
    $confPath = Join-Path -Path $PSScriptRoot -ChildPath 'conf.txt'
    if (Test-Path -LiteralPath $confPath) {
        $conf = Read-ApiConf -Path $confPath
        if ($conf.ContainsKey('port')) {
            $parsed = 0
            if ([int]::TryParse($conf['port'], [ref]$parsed) -and $parsed -ge 0 -and $parsed -le 65535) {
                $resolved = $parsed
            }
        }
    }

    if ($resolved -eq 0) {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
        $listener.Start()
        $resolved = ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
        $listener.Stop()
        Write-Host "conf.txt asks for port 0 (any free port); using $resolved for this run." -ForegroundColor DarkGray
    }

    return $resolved
}

function Resolve-HermesCommand {
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA 'hermes\bin\hermes.exe'),
        (Join-Path $env:USERPROFILE '.local\bin\hermes.exe')
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) { return (Resolve-Path -LiteralPath $candidate).Path }
    }
    $fromPath = Get-Command -Name 'hermes' -ErrorAction SilentlyContinue
    if ($fromPath) { return $fromPath.Source }
    throw 'hermes.exe was not found. Re-run the Hermes installer.'
}

function Test-ClineRelay {
    param([int]$Port)
    try {
        $response = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 3
        return [bool]$response.ok
    } catch {
        return $false
    }
}

function Start-ClineRelay {
    param([int]$Port)
    $relay = Join-Path -Path (Split-Path -Parent $PSCommandPath) -ChildPath 'router-relay.js'
    if (-not (Test-Path -LiteralPath $relay)) { throw "Relay script not found: $relay" }
    $node = (Get-Command -Name 'node' -ErrorAction SilentlyContinue).Source
    if (-not $node) { throw 'node was not found in PATH; it is required to run the relay.' }
    $logPath = Join-Path -Path (Split-Path -Parent $PSCommandPath) -ChildPath 'router-relay.log'
    Write-Host "Starting local relay on port $Port..." -ForegroundColor Cyan
    Start-Process -FilePath $node -ArgumentList @($relay, '--port', "$Port", '--log', $logPath) -WindowStyle Hidden | Out-Null
    for ($attempt = 0; $attempt -lt 24; $attempt++) {
        Start-Sleep -Milliseconds 500
        if (Test-ClineRelay -Port $Port) { Write-Host "Relay is up: http://127.0.0.1:$Port/v1" -ForegroundColor Cyan; return $true }
    }
    return $false
}

$hermesCommand = Resolve-HermesCommand

# Resolved once: -RelayPort, then conf.txt `port`, then 8765.
$RelayPort = Resolve-RelayPort -Requested $RelayPort

if (-not $SkipRelayCheck) {
    if (Test-ClineRelay -Port $RelayPort) {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$RelayPort/health" -TimeoutSec 3
        Write-Host "Relay OK: upstream $($health.upstream), proxy $($health.proxy)" -ForegroundColor DarkGray
    } elseif (-not (Start-ClineRelay -Port $RelayPort)) {
        Write-Warning "Relay did not come up on port $RelayPort - Hermes will fail to reach the model."
    }
}

# The relay reaches the router through the HAPP proxy; without it, upstream calls fail.
try {
    $proxyListener = Get-NetTCPConnection -LocalPort 10809 -State Listen -ErrorAction Stop
    if ($proxyListener) { Write-Host 'HAPP proxy port 10809 is listening.' -ForegroundColor DarkGray }
} catch {
    Write-Warning 'HAPP proxy port 10809 is not listening - the relay cannot reach the router. Start HAPP first.'
}

$forward = @()
if ($HermesArgs) { $forward = $HermesArgs }

& $hermesCommand @forward
$exitCode = $LASTEXITCODE
if ($exitCode -ne 0) { Write-Host "Hermes exited with code $exitCode." -ForegroundColor Yellow }
