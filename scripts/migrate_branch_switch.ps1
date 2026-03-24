[CmdletBinding()]
param(
    [string]$FromBranch = "dev",
    [string]$ToBranch = "main",
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function Write-Step {
    param([string]$Message)

    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-Checked {
    param(
        [string]$Description,
        [string]$DisplayCommand,
        [scriptblock]$Command
    )

    Write-Step $Description
    Write-Host $DisplayCommand -ForegroundColor DarkGray

    if ($DryRun) {
        return
    }

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $DisplayCommand"
    }
}

function Get-PreferredCommand {
    param([string[]]$Names)

    foreach ($name in $Names) {
        $command = Get-Command $name -ErrorAction SilentlyContinue
        if ($null -ne $command) {
            return $command.Source
        }
    }

    return $null
}

function Get-DotEnvMap {
    $values = @{}
    $dotenvPath = Join-Path $repoRoot ".env"

    if (-not (Test-Path $dotenvPath)) {
        return $values
    }

    foreach ($line in Get-Content $dotenvPath) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }

        $delimiterIndex = $trimmed.IndexOf("=")
        if ($delimiterIndex -lt 1) {
            continue
        }

        $name = $trimmed.Substring(0, $delimiterIndex).Trim()
        $value = $trimmed.Substring($delimiterIndex + 1).Trim()

        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        $values[$name] = $value
    }

    return $values
}

function Get-EnvValue {
    param(
        [string]$Name,
        [hashtable]$DotEnv
    )

    $processValue = [Environment]::GetEnvironmentVariable($Name)
    if ($processValue) {
        return $processValue
    }

    if ($DotEnv.ContainsKey($Name)) {
        return $DotEnv[$Name]
    }

    return $null
}

$pythonExe = Get-PreferredCommand @("python", "python3")
if (-not $pythonExe) {
    throw "python/python3 is not available in PATH."
}

$gitExe = Get-PreferredCommand @("git")
if (-not $gitExe) {
    throw "git is not available in PATH."
}

$dockerExe = Get-PreferredCommand @("docker")
if (-not $dockerExe) {
    throw "docker is not available in PATH."
}

$dotEnv = Get-DotEnvMap
$postgresUser = Get-EnvValue -Name "POSTGRES_USER" -DotEnv $dotEnv
$postgresDb = Get-EnvValue -Name "POSTGRES_DB" -DotEnv $dotEnv

if (-not $postgresUser -or -not $postgresDb) {
    throw "POSTGRES_USER and POSTGRES_DB must be defined in the environment or in .env."
}

Write-Step "Resolving target-side migrations for $FromBranch -> $ToBranch"
$addedMigrations = @(
    & $gitExe diff --diff-filter=A --name-only "$FromBranch...$ToBranch" -- migrations
)
if ($LASTEXITCODE -ne 0) {
    throw "Unable to resolve migrations diff for $FromBranch...$ToBranch."
}

$modifiedMigrations = @(
    & $gitExe diff --diff-filter=M --name-only "$FromBranch...$ToBranch" -- migrations
)
if ($LASTEXITCODE -ne 0) {
    throw "Unable to resolve modified migrations diff for $FromBranch...$ToBranch."
}

$migrationFiles = @(
    $addedMigrations |
        Where-Object { $_ -and $_.EndsWith(".sql") -and -not $_.EndsWith(".rollback.sql") } |
        Sort-Object
)

if ($modifiedMigrations.Count -gt 0) {
    Write-Warning "Modified existing migration files are ignored by this temporary helper:"
    $modifiedMigrations | ForEach-Object { Write-Host " - $_" -ForegroundColor Yellow }
}

if ($migrationFiles.Count -gt 0) {
    Write-Host "Detected migrations:" -ForegroundColor Green
    $migrationFiles | ForEach-Object { Write-Host " - $_" -ForegroundColor Green }

    Invoke-Checked -Description "Starting db service for SQL migrations" `
        -DisplayCommand "docker compose up -d db" `
        -Command { docker compose up -d db }

    Invoke-Checked -Description "Stopping app services during schema changes" `
        -DisplayCommand "docker compose stop tracker bot" `
        -Command { docker compose stop tracker bot }

    foreach ($migrationFile in $migrationFiles) {
        Invoke-Checked -Description "Applying $migrationFile" `
            -DisplayCommand "Get-Content $migrationFile | docker compose exec -T db psql -v ON_ERROR_STOP=1 -U $postgresUser -d $postgresDb" `
            -Command {
                Get-Content $migrationFile |
                    docker compose exec -T db psql -v ON_ERROR_STOP=1 -U $postgresUser -d $postgresDb
            }
    }
}
else {
    Write-Step "No new SQL migrations detected for $FromBranch -> $ToBranch"
}

Invoke-Checked -Description "Checking Python sources" `
    -DisplayCommand "$pythonExe -m compileall src" `
    -Command { & $pythonExe -m compileall src }

Invoke-Checked -Description "Validating docker compose configuration" `
    -DisplayCommand "docker compose config" `
    -Command { docker compose config }

Invoke-Checked -Description "Running unit tests" `
    -DisplayCommand "$pythonExe -m unittest discover -s tests -p `"test_*.py`"" `
    -Command { & $pythonExe -m unittest discover -s tests -p "test_*.py" }

Invoke-Checked -Description "Starting application containers" `
    -DisplayCommand "docker compose up -d --build --force-recreate --remove-orphans" `
    -Command { docker compose up -d --build --force-recreate --remove-orphans }

Invoke-Checked -Description "Showing container status" `
    -DisplayCommand "docker compose ps" `
    -Command { docker compose ps }

Invoke-Checked -Description "Showing recent tracker logs" `
    -DisplayCommand "docker compose logs --tail=200 tracker" `
    -Command { docker compose logs --tail=200 tracker }

Invoke-Checked -Description "Showing recent bot logs" `
    -DisplayCommand "docker compose logs --tail=200 bot" `
    -Command { docker compose logs --tail=200 bot }

Write-Step "Branch switch workflow completed"
