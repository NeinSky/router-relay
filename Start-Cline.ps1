<#
.SYNOPSIS
    Launcher for the Cline CLI wired to the local router relay, using its own isolated profile.

.DESCRIPTION
    Runs the Cline CLI against the cheap router ([OI]-compatible endpoint) without touching the
    provider configuration Cline uses by default.

    HOW THE ISOLATION WORKS
      Cline keeps its provider settings in <data-dir>\settings\providers.json (credentials) and
      <data-dir>\settings\models.json (model metadata: context window, max output, prices), and its
      default data dir is ~\.cline\data. This launcher never writes there. Instead it uses a
      separate profile next to this script:

          D:\api\cline-router\data\settings\providers.json     <- router profile (this script)
          D:\api\cline-router\data\settings\models.json        <- router model info (this script)
          C:\Users\<user>\.cline\data\settings\...             <- default profile (never written)

      The CLI is started with `--data-dir <profile>\data`, so sessions, logs and the provider
      selection of router runs live in the isolated profile. The *config* dir stays the default
      ~\.cline on purpose, so global rules / skills / hooks / workflows are still shared, and the
      global MCP server list is shared too (via CLINE_MCP_SETTINGS_PATH, see -NoSharedMcp).
      Consequence: the provider you normally use in Cline (its model, key and base URL) stays
      exactly as it is; `cline` typed by hand keeps using it.

    WHY A CUSTOM PROVIDER ID
      The profile uses its own provider id, "oi-router", instead of the built-in
      "openai-compatible". Cline ignores models.json entries for its built-in providers, so with
      the built-in id the model metadata below would never be applied: the status bar would show
      $0.00 and the context window would stay unknown. A file-registered provider id does get its
      model metadata, which is what makes the profile behave like the default one.

    MODEL METADATA (context window, max output, prices)
      The default profile's models.json is the source of truth. On every run this launcher copies
      the entry for conf.txt `model` into the isolated profile, so context window / max input /
      max output / input, output and cache prices are identical to what plain `cline` uses for the
      same model. Nothing in the default profile is modified - it is only read.

    THE RELAY
      The router never finishes an SSE stream: it drops the connection without a finish_reason
      chunk and without `data: [DONE]`, which Cline reports as
      "Failed to process successful response: ... socket connection was closed unexpectedly
      (ECONNRESET)". router-relay.js (next to this script) works around that: it receives Cline's
      streaming request, asks the router with "stream": false, and replays the finished JSON answer
      as a valid SSE stream. This launcher starts that relay when it is not answering /health and
      points the isolated profile at http://127.0.0.1:<RelayPort>/v1. The relay itself reads
      conf.txt (base_url first, base_url_reserved as the spare) and does its own transport/host
      failover, so the profile only ever knows the local relay URL.

.PARAMETER Workspace
    Working directory for Cline. Forwarded to the CLI as --cwd.
    Default: the current directory (identical to running `cline` by hand).

.PARAMETER RelayPort
    TCP port of the local relay. Default: 8765 (shared with Start-Hermes.ps1).

.PARAMETER ProfileDir
    Root of the isolated Cline profile. Default: <script dir>\cline-router.
    Its data dir (<ProfileDir>\data) is what Cline is pointed at; nothing outside it is written.

.PARAMETER SyncProvider
    Force a rewrite of the isolated profile from conf.txt (base_url / api_key / model) and from the
    default profile's models.json. Idempotent - use it after editing conf.txt or after changing
    -RelayPort. Without this switch the sync still runs when the profile is missing or out of date.

.PARAMETER NoRelay
    Bypass the relay: point the isolated profile straight at conf.txt base_url. The router does
    not terminate SSE streams, so long answers will fail with ECONNRESET - debugging only.

.PARAMETER UsePublic
    With -NoRelay: use conf.txt base_url_reserved (the spare address) instead of base_url.

.PARAMETER NoSharedMcp
    Do not share the global MCP settings file with the isolated profile. By default router runs
    see the same MCP servers as the default profile (read from ~\.cline, never written).

.PARAMETER Status
    Print the router profile and the default Cline profile, then exit without launching Cline.
    Read-only: it changes neither profile. -SyncProvider needs its own run (it is ignored here).

.PARAMETER StopRelay
    Stop the running local relay process(es) and exit without launching Cline.

.PARAMETER ClineArgs
    Any remaining arguments are passed through to the Cline CLI unchanged.

.EXAMPLE
    .\Start-Cline.ps1
    Opens the Cline TUI on the router profile (default profile untouched).

.EXAMPLE
    .\Start-Cline.ps1 -Workspace D:\my-project
    Same, in D:\my-project.

.EXAMPLE
    .\Start-Cline.ps1 --yolo "run the tests and fix the failures"
    Headless one-shot run through the relay, router profile.

.EXAMPLE
    .\Start-Cline.ps1 -Status
    Shows the router profile (provider, model, context window, prices) and the default profile.

.EXAMPLE
    .\Start-Cline.ps1 -SyncProvider
    Re-applies conf.txt and the default model metadata to the isolated profile, then opens the TUI.
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$Workspace,

    [int]$RelayPort = 0,

    [string]$ProfileDir,

    [switch]$SyncProvider,

    [switch]$NoRelay,

    [switch]$UsePublic,

    [switch]$NoSharedMcp,

    [switch]$Status,

    [switch]$StopRelay,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ClineArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# conf.txt describes the [OI]-compatible endpoint (base_url / api_key / model).
$ConfPath = Join-Path -Path $PSScriptRoot -ChildPath 'conf.txt'

# Isolated profile used by this launcher. Cline's own default profile (~\.cline) is never written.
if (-not $ProfileDir) { $ProfileDir = Join-Path -Path $PSScriptRoot -ChildPath 'cline-router' }
$ProfileDir = [System.IO.Path]::GetFullPath($ProfileDir)
$ProfileData = Join-Path -Path $ProfileDir -ChildPath 'data'
$ProfileSettings = Join-Path -Path $ProfileData -ChildPath 'settings\providers.json'
$ProfileModels = Join-Path -Path $ProfileData -ChildPath 'settings\models.json'

# Default Cline profile - read-only for this script (source of the model metadata, see -Status).
$DefaultData = Join-Path -Path $env:USERPROFILE -ChildPath '.cline\data'
$DefaultSettings = Join-Path -Path $DefaultData -ChildPath 'settings\providers.json'
$DefaultModels = Join-Path -Path $DefaultData -ChildPath 'settings\models.json'

# Provider id of the router profile. Deliberately NOT the built-in "openai-compatible": Cline
# ignores models.json for built-in providers, so a file-registered id is what makes the model
# metadata (context window, prices) take effect.
$RouterProviderId = 'oi-router'
$RouterProviderName = 'OI Router (relay)'
$RouterProtocol = 'openai-chat'
$RouterClient = 'openai-compatible'

# Written by the previous version of this launcher; dropped from the profile when it points at
# the relay, so the TUI does not show two providers for the same endpoint.
$LegacyProviderId = 'openai-compatible'

function Resolve-ClineCommand {
    <#
        Finds the real Cline CLI entry point.

        This script itself is excluded from the candidates, so putting a launcher on PATH can
        never cause infinite recursion. The .cmd shim is preferred over the .ps1 shim because it
        starts the CLI directly (no nested PowerShell script, no execution-policy surprise).
    #>
    $fromPath = @(
        Get-Command -Name 'cline' -All -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Path -and
                [System.IO.Path]::GetExtension($_.Path) -and
                $_.Path -ne $PSCommandPath
            } |
            Select-Object -ExpandProperty Path
    )

    $candidates = @($fromPath | Where-Object { $_ -like '*.cmd' }) +
                  $fromPath +
                  (Join-Path -Path $env:APPDATA -ChildPath 'npm\cline.cmd')

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    return $null
}

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

function Get-JsonValue {
    <# Safe property read: works with StrictMode even when the property is absent. #>
    param($Object, [Parameter(Mandatory = $true)][string]$Name)

    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($property) { return $property.Value }
    return $null
}

function Read-JsonFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    try {
        return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json)
    } catch {
        return $null
    }
}

function ConvertTo-JsonText {
    <#
        Pretty JSON with 2-space indentation, the way Cline writes its own settings files.
        Windows PowerShell 5.1's ConvertTo-Json aligns dictionary values into one wide column,
        which makes these files hard to read; this keeps them close to what the CLI produces.
    #>
    param($Value, [int]$Depth = 0)

    $indent = ' ' * ($Depth * 2)
    $childIndent = ' ' * (($Depth + 1) * 2)

    if ($null -eq $Value) { return 'null' }

    if ($Value -is [System.Collections.IDictionary]) {
        if ($Value.Count -eq 0) { return '{}' }
        $parts = @()
        foreach ($key in $Value.Keys) {
            $name = ConvertTo-Json -InputObject ([string]$key) -Compress
            $parts += "$childIndent${name}: $(ConvertTo-JsonText -Value $Value[$key] -Depth ($Depth + 1))"
        }
        return "{`r`n" + ($parts -join ",`r`n") + "`r`n$indent}"
    }

    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        $properties = @($Value.PSObject.Properties)
        if ($properties.Count -eq 0) { return '{}' }
        $parts = @()
        foreach ($property in $properties) {
            $name = ConvertTo-Json -InputObject ([string]$property.Name) -Compress
            $parts += "$childIndent${name}: $(ConvertTo-JsonText -Value $property.Value -Depth ($Depth + 1))"
        }
        return "{`r`n" + ($parts -join ",`r`n") + "`r`n$indent}"
    }

    if (($Value -is [System.Collections.IEnumerable]) -and -not ($Value -is [string])) {
        $items = @($Value)
        if ($items.Count -eq 0) { return '[]' }
        $parts = @()
        foreach ($item in $items) {
            $parts += "$childIndent$(ConvertTo-JsonText -Value $item -Depth ($Depth + 1))"
        }
        return "[`r`n" + ($parts -join ",`r`n") + "`r`n$indent]"
    }

    return (ConvertTo-Json -InputObject $Value -Compress)
}

function Write-JsonFile {
    <# UTF-8 (no BOM) + temp file + move, the same way Cline writes its own settings. #>
    param([Parameter(Mandatory = $true)][string]$Path, $Object)

    $directory = Split-Path -Path $Path -Parent
    if (-not (Test-Path -LiteralPath $directory)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }

    $json = (ConvertTo-JsonText -Value $Object) + "`r`n"
    $temp = "$Path.$PID.tmp"
    [System.IO.File]::WriteAllText($temp, $json, (New-Object System.Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $temp -Destination $Path -Force
}

function Get-ProviderEntry {
    <# Returns the whole provider entry (settings/updatedAt/tokenSource) for $ProviderId. #>
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ProviderId
    )

    $json = Read-JsonFile -Path $Path
    $providers = Get-JsonValue -Object $json -Name 'providers'
    if ($null -eq $providers) { return $null }
    return (Get-JsonValue -Object $providers -Name $ProviderId)
}

function Get-ProviderModelInfo {
    <# Returns the model metadata entry stored in models.json for one provider/model. #>
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ProviderId,
        [Parameter(Mandatory = $true)][string]$ModelId
    )

    $entry = Get-ProviderEntry -Path $Path -ProviderId $ProviderId
    if ($null -eq $entry) { return $null }
    $models = Get-JsonValue -Object $entry -Name 'models'
    if ($null -eq $models) { return $null }
    return (Get-JsonValue -Object $models -Name $ModelId)
}

function Find-DefaultModelInfo {
    <#
        Finds the model metadata the default profile uses for $ModelId. The built-in
        "openai-compatible" entry is preferred (that is where the DeepSeek V4.1 Flash numbers
        live), then any other provider that knows the model. Read-only.
    #>
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ModelId
    )

    $json = Read-JsonFile -Path $Path
    if ($null -eq $json) { return $null }
    $providers = Get-JsonValue -Object $json -Name 'providers'
    if ($null -eq $providers) { return $null }

    $preferred = Get-JsonValue -Object $providers -Name $LegacyProviderId
    if ($preferred) {
        $models = Get-JsonValue -Object $preferred -Name 'models'
        $found = Get-JsonValue -Object $models -Name $ModelId
        if ($found) { return $found }
    }

    foreach ($property in $providers.PSObject.Properties) {
        $models = Get-JsonValue -Object $property.Value -Name 'models'
        $found = Get-JsonValue -Object $models -Name $ModelId
        if ($found) { return $found }
    }

    return $null
}

function Test-JsonEqual {
    param($Left, $Right)

    if ($null -eq $Left -or $null -eq $Right) { return $false }
    return (($Left | ConvertTo-Json -Depth 20 -Compress) -eq ($Right | ConvertTo-Json -Depth 20 -Compress))
}

function Save-ProviderFile {
    <#
        Writes one provider section into a providers.json / models.json envelope, keeping any other
        provider already in that file. The legacy "openai-compatible" entry is dropped when it
        points at $DropLegacyBaseUrl (it was created by the previous version of this launcher).
    #>
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ProviderId,
        [Parameter(Mandatory = $true)]$Section,
        [string]$DropLegacyBaseUrl,
        [switch]$SetLastUsed
    )

    $existing = Read-JsonFile -Path $Path
    $providers = [ordered]@{}

    if ($existing) {
        $existingProviders = Get-JsonValue -Object $existing -Name 'providers'
        if ($existingProviders) {
            foreach ($property in $existingProviders.PSObject.Properties) {
                if ($property.Name -eq $ProviderId) { continue }

                if ($property.Name -eq $LegacyProviderId -and $DropLegacyBaseUrl) {
                    $settings = Get-JsonValue -Object $property.Value -Name 'settings'
                    $provider = Get-JsonValue -Object $property.Value -Name 'provider'
                    $baseUrl = Get-JsonValue -Object $settings -Name 'baseUrl'
                    if (-not $baseUrl) { $baseUrl = Get-JsonValue -Object $provider -Name 'baseUrl' }
                    if ($baseUrl -eq $DropLegacyBaseUrl) { continue }
                }

                $providers[$property.Name] = $property.Value
            }
        }
    }

    $providers[$ProviderId] = $Section

    $envelope = [ordered]@{ version = 1 }
    if ($SetLastUsed) {
        $modes = $null
        if ($existing) { $modes = Get-JsonValue -Object $existing -Name 'modes' }
        if ($null -eq $modes) { $modes = [ordered]@{} }
        $envelope['lastUsedProvider'] = $ProviderId
        $envelope['modes'] = $modes
    }
    $envelope['providers'] = $providers

    Write-JsonFile -Path $Path -Object $envelope
}

function Format-ModelInfo {
    <# One-line summary of a model metadata entry, or $null when there is none. #>
    param($ModelInfo)

    if ($null -eq $ModelInfo) { return $null }

    $contextWindow = Get-JsonValue -Object $ModelInfo -Name 'contextWindow'
    $maxInput = Get-JsonValue -Object $ModelInfo -Name 'maxInputTokens'
    $maxTokens = Get-JsonValue -Object $ModelInfo -Name 'maxTokens'
    $inputPrice = Get-JsonValue -Object $ModelInfo -Name 'inputPrice'
    $outputPrice = Get-JsonValue -Object $ModelInfo -Name 'outputPrice'
    $cacheRead = Get-JsonValue -Object $ModelInfo -Name 'cacheReadsPrice'
    $cacheWrite = Get-JsonValue -Object $ModelInfo -Name 'cacheWritesPrice'

    $context = "context $contextWindow / maxInput $maxInput / maxOutput $maxTokens"
    $price = "in $inputPrice, out $outputPrice, cacheRead $cacheRead, cacheWrite $cacheWrite (per 1M tokens)"
    return "$context; $price"
}

function Resolve-RelayScript {
    $script = Join-Path -Path $PSScriptRoot -ChildPath 'router-relay.js'
    if (-not (Test-Path -LiteralPath $script)) {
        throw "Relay script not found: $script"
    }
    return (Resolve-Path -LiteralPath $script).Path
}

function Test-ClineRelay {
    <# $true when the local relay answers its /health endpoint. #>
    param([Parameter(Mandatory = $true)][int]$Port)

    try {
        $response = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 3
        return [bool]$response.ok
    } catch {
        return $false
    }
}

function Start-ClineRelay {
    <# Starts router-relay.js hidden and waits until /health answers. Returns $true on success. #>
    param([Parameter(Mandatory = $true)][int]$Port)

    $node = Get-Command -Name 'node' -ErrorAction SilentlyContinue
    if (-not $node) {
        throw 'node was not found in PATH; it is required to run the relay.'
    }

    $relayScript = Resolve-RelayScript
    $logPath = Join-Path -Path $PSScriptRoot -ChildPath 'router-relay.log'

    # No output redirection here on purpose: a redirected child keeps a copy of this PowerShell's
    # stdout pipe open for as long as the relay lives, which makes any wrapper that captures our
    # output wait forever. The relay writes its own log file instead (--log).
    Write-Host "Starting local relay: node router-relay.js --port $Port" -ForegroundColor Cyan
    Start-Process -FilePath $node.Source -ArgumentList @($relayScript, '--port', "$Port", '--log', $logPath) `
        -WindowStyle Hidden | Out-Null

    for ($attempt = 0; $attempt -lt 24; $attempt++) {
        Start-Sleep -Milliseconds 500
        if (Test-ClineRelay -Port $Port) {
            Write-Host "Relay is up: http://127.0.0.1:$Port/v1" -ForegroundColor Cyan
            return $true
        }
    }

    Write-Warning "Relay did not become healthy on port $Port. See $logPath"
    return $false
}

function Confirm-ClineRelay {
    <# Ensures the relay runs on $Port. Returns $true when it is usable. #>
    param(
        [Parameter(Mandatory = $true)][int]$Port,
        [switch]$Quiet
    )

    if (Test-ClineRelay -Port $Port) {
        if (-not $Quiet) {
            Write-Host "Local relay already running on port $Port" -ForegroundColor DarkGray
        }
        return $true
    }
    return (Start-ClineRelay -Port $Port)
}

function Resolve-RelayPort {
    <#
        Resolves the relay port: explicit -RelayPort, then conf.txt `port`, then 8765.
        conf.txt `port=0` means "any free port" - which a launcher cannot use, because it has to
        know the address it hands to the client. A concrete free port is chosen instead, so several
        relays can still coexist.
    #>
    param([int]$Requested)

    if ($Requested -gt 0) { return $Requested }

    $resolved = 8765
    if (Test-Path -LiteralPath $ConfPath) {
        $conf = Read-ApiConf -Path $ConfPath
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

function Stop-ClineRelay {
    <#
        Stops the relay on $Port through Stop-Relay.ps1. That script identifies a relay positively
        (command line plus a /health pid match) and refuses to touch an unrelated process holding the
        port. Keeping the identification in one place matters: a relay started without --port names
        its port only in conf.txt, where a command-line match would never find it.
    #>
    param([Parameter(Mandatory = $true)][int]$Port)

    $stopper = Join-Path -Path $PSScriptRoot -ChildPath 'Stop-Relay.ps1'
    if (-not (Test-Path -LiteralPath $stopper)) {
        throw "Stop-Relay.ps1 not found next to this script: $stopper"
    }

    & $stopper -Port $Port -Force
    return ($LASTEXITCODE -eq 0)
}

# The relay port is central to everything below: the profile URL, the health check and the
# stopper. Resolved once: -RelayPort, then conf.txt `port`, then 8765.
$RelayPort = Resolve-RelayPort -Requested $RelayPort

# -StopRelay is a standalone action: stop the background relay, leave Cline alone.
if ($StopRelay) {
    if (Stop-ClineRelay -Port $RelayPort) {
        Write-Host "Relay on port $RelayPort is stopped." -ForegroundColor Cyan
    } else {
        Write-Host "Nothing was stopped on port $RelayPort (not a relay, or already gone)." -ForegroundColor Yellow
    }
    return
}

$clineCommand = Resolve-ClineCommand
if (-not $clineCommand) {
    throw "Cline CLI was not found. Install it with: npm install -g cline"
}

# -Status is a standalone action: show both profiles, change nothing.
if ($Status) {
    $confForStatus = @{}
    if (Test-Path -LiteralPath $ConfPath) { $confForStatus = Read-ApiConf -Path $ConfPath }

    Write-Host 'Router profile (this launcher)' -ForegroundColor Cyan
    Write-Host "  data dir : $ProfileData"
    $routerEntry = Get-ProviderEntry -Path $ProfileSettings -ProviderId $RouterProviderId
    $routerInfo = Get-ProviderModelInfo -Path $ProfileModels -ProviderId $RouterProviderId -ModelId ([string]$confForStatus['model'])
    if ($routerEntry) {
        $routerSettings = Get-JsonValue -Object $routerEntry -Name 'settings'
        Write-Host "  provider : $RouterProviderId ($RouterProviderName)"
        Write-Host "  baseUrl  : $(Get-JsonValue -Object $routerSettings -Name 'baseUrl')"
        Write-Host "  model    : $(Get-JsonValue -Object $routerSettings -Name 'model')"
        $routerSummary = Format-ModelInfo -ModelInfo $routerInfo
        if ($routerSummary) { Write-Host "  model info: $routerSummary" }
        else { Write-Host '  model info: missing (context window and prices unknown)' -ForegroundColor Yellow }
    } else {
        Write-Host "  not configured yet - run this script with -SyncProvider (without -Status) to create it" -ForegroundColor Yellow
    }
    if ($confForStatus['base_url']) {
        Write-Host "  conf.txt : $($confForStatus['base_url']) / $($confForStatus['model'])" -ForegroundColor DarkGray
    }

    Write-Host ''
    Write-Host 'Default profile (plain cline, left untouched by this script)' -ForegroundColor Cyan
    Write-Host "  settings : $DefaultSettings"
    $defaultJson = Read-JsonFile -Path $DefaultSettings
    $defaultProviders = Get-JsonValue -Object $defaultJson -Name 'providers'
    if ($defaultProviders) {
        foreach ($property in $defaultProviders.PSObject.Properties) {
            $settings = Get-JsonValue -Object $property.Value -Name 'settings'
            $defaultModel = Get-JsonValue -Object $settings -Name 'model'
            Write-Host "  $($property.Name): model $defaultModel, baseUrl $(Get-JsonValue -Object $settings -Name 'baseUrl')"
            $info = Get-ProviderModelInfo -Path $DefaultModels -ProviderId $property.Name -ModelId ([string]$defaultModel)
            $summary = Format-ModelInfo -ModelInfo $info
            if ($summary) { Write-Host "    model info: $summary" -ForegroundColor DarkGray }
        }
    } else {
        Write-Host '  (no providers configured)' -ForegroundColor DarkGray
    }
    Write-Host "  last used: $(Get-JsonValue -Object $defaultJson -Name 'lastUsedProvider')" -ForegroundColor DarkGray

    Write-Host ''
    if (Test-ClineRelay -Port $RelayPort) {
        Write-Host "Relay: up on port $RelayPort" -ForegroundColor DarkGray
    } else {
        Write-Host "Relay: not running on port $RelayPort" -ForegroundColor Yellow
    }
    return
}

if (-not (Test-Path -LiteralPath $ConfPath)) {
    throw "API config file not found: $ConfPath"
}

$conf = Read-ApiConf -Path $ConfPath
$apiKey = $conf['api_key']
$model = $conf['model']
if (-not $conf['base_url']) { throw "Missing 'base_url' in $ConfPath" }
if (-not $apiKey) { throw "Missing 'api_key' in $ConfPath" }
if (-not $model) { throw "Missing 'model' in $ConfPath" }

# Where the isolated profile should point: the local relay by default, conf.txt directly on -NoRelay.
$targetBaseUrl = $conf['base_url']
$targetLabel = 'base_url (main, no relay)'
if ($NoRelay) {
    if ($UsePublic -and $conf['base_url_reserved']) {
        $targetBaseUrl = $conf['base_url_reserved']
        $targetLabel = 'base_url_reserved (spare, no relay)'
    }
} else {
    # The relay reaches the router through the HAPP proxy; warn early, exactly like Start-Hermes.ps1.
    try {
        $proxyListener = Get-NetTCPConnection -LocalPort 10809 -State Listen -ErrorAction Stop
        if ($proxyListener) { Write-Host 'HAPP proxy port 10809 is listening.' -ForegroundColor DarkGray }
    } catch {
        Write-Warning 'HAPP proxy port 10809 is not listening - the relay may be unable to reach the router.'
    }

    if (-not (Confirm-ClineRelay -Port $RelayPort)) {
        throw "Local relay is not available on port $RelayPort. Fix it and retry (or use -NoRelay)."
    }
    $targetBaseUrl = "http://127.0.0.1:$RelayPort/v1"
    $targetLabel = "local relay -> $($conf['base_url'])"
}

# --- credentials: providers.json of the isolated profile -------------------------------------
$settingsSection = [ordered]@{
    provider  = $RouterProviderId
    apiKey    = $apiKey
    model     = $model
    baseUrl   = $targetBaseUrl
    headers   = [ordered]@{}
    timeout   = 300000
}

$currentEntry = Get-ProviderEntry -Path $ProfileSettings -ProviderId $RouterProviderId
$currentSettings = $null
if ($currentEntry) { $currentSettings = Get-JsonValue -Object $currentEntry -Name 'settings' }

if ($SyncProvider -or -not (Test-JsonEqual -Left $currentSettings -Right $settingsSection)) {
    $providerSection = [ordered]@{
        settings    = $settingsSection
        updatedAt   = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
        tokenSource = 'manual'
    }

    Write-Host "Configuring router profile: $RouterProviderId / $model" -ForegroundColor Cyan
    Write-Host "  $targetLabel = $targetBaseUrl" -ForegroundColor Cyan
    Write-Host "  profile data dir = $ProfileData" -ForegroundColor DarkGray

    Save-ProviderFile -Path $ProfileSettings -ProviderId $RouterProviderId -Section $providerSection `
        -DropLegacyBaseUrl $targetBaseUrl -SetLastUsed
} else {
    Write-Host "Router profile credentials are up to date: $RouterProviderId / $model -> $targetBaseUrl" -ForegroundColor DarkGray
}

# --- model metadata: models.json of the isolated profile, copied from the default profile -----
$defaultModelInfo = Find-DefaultModelInfo -Path $DefaultModels -ModelId $model
if ($defaultModelInfo) {
    $modelsSection = [ordered]@{
        provider = [ordered]@{
            name           = $RouterProviderName
            baseUrl        = $targetBaseUrl
            defaultModelId = $model
            protocol       = $RouterProtocol
            client         = $RouterClient
        }
        models   = [ordered]@{}
    }
    $modelsSection['models'][$model] = $defaultModelInfo

    $currentModelsSection = Get-ProviderEntry -Path $ProfileModels -ProviderId $RouterProviderId
    if ($SyncProvider -or -not (Test-JsonEqual -Left $currentModelsSection -Right $modelsSection)) {
        Save-ProviderFile -Path $ProfileModels -ProviderId $RouterProviderId -Section $modelsSection `
            -DropLegacyBaseUrl $targetBaseUrl
        $summary = Format-ModelInfo -ModelInfo $defaultModelInfo
        Write-Host "Model info copied from the default profile: $summary" -ForegroundColor Cyan
    } else {
        Write-Host "Model info is up to date with the default profile ($model)." -ForegroundColor DarkGray
    }
} else {
    Write-Warning "The default profile has no model metadata for '$model' ($DefaultModels). Context window and prices stay unknown for the router profile."
}

# Seed the profile-wide Cline settings (Plan/Act mode, auto-approve, telemetry, auto-update) from
# the default profile the first time this profile is created. Afterwards the TUI owns the file, so a
# Plan/Act toggle inside the router session is not overwritten on the next start.
$defaultGlobalSettings = Join-Path -Path $DefaultData -ChildPath 'settings\global-settings.json'
$profileGlobalSettings = Join-Path -Path $ProfileData -ChildPath 'settings\global-settings.json'
if (-not (Test-Path -LiteralPath $profileGlobalSettings) -and (Test-Path -LiteralPath $defaultGlobalSettings)) {
    Copy-Item -LiteralPath $defaultGlobalSettings -Destination $profileGlobalSettings
    Write-Host 'Profile settings seeded from the default profile (Plan/Act mode, auto-approve).' -ForegroundColor DarkGray
}

# Global MCP servers stay visible: the isolated data dir would otherwise hide them. The default
# profile is only read here - Cline writes that file only when the MCP list is edited from the TUI.
$defaultMcp = Join-Path -Path $DefaultData -ChildPath 'settings\cline_mcp_settings.json'
$shareMcp = (-not $NoSharedMcp) -and (Test-Path -LiteralPath $defaultMcp)
$previousMcpPath = $env:CLINE_MCP_SETTINGS_PATH
if ($shareMcp) {
    $env:CLINE_MCP_SETTINGS_PATH = $defaultMcp
}

# --data-dir is the whole point: sessions and provider selection stay in the isolated profile.
$forward = @('--data-dir', $ProfileData)
if ($Workspace) {
    $forward += @('--cwd', (Resolve-Path -LiteralPath $Workspace).Path)
}
if ($ClineArgs) {
    $forward += $ClineArgs
}

Write-Host 'Launching Cline on the router profile (default profile untouched).' -ForegroundColor DarkGray
try {
    & $clineCommand @forward
} finally {
    if ($shareMcp) {
        if ($null -eq $previousMcpPath) { Remove-Item Env:CLINE_MCP_SETTINGS_PATH -ErrorAction SilentlyContinue }
        else { $env:CLINE_MCP_SETTINGS_PATH = $previousMcpPath }
    }
}

$clineExitCode = $LASTEXITCODE
if ($clineExitCode -ne 0) {
    Write-Host "Cline exited with code $clineExitCode." -ForegroundColor Yellow
}
