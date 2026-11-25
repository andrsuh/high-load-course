param([int]$HttpTimeoutMs = 2500, [switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#=]+)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
    }
}
$env:POSTGRES_ADDRESS = 'localhost'
$env:POSTGRES_PORT = $env:POSTGRES_CONTAINER_EXPOSED_PORT
$env:PAYMENT_HOST = 'localhost'
$env:PAYMENT_PORT = '1234'
$env:PAYMENT_ACCOUNTS = 'acc-7'
$env:PAYMENT_HTTP_TIMEOUT_MS = $HttpTimeoutMs.ToString()
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw 'Infrastructure startup failed' }
if (-not $SkipBuild) {
    docker run --rm -v "${PWD}:/app" -v highload-maven-cache:/root/.m2 -w /app maven:3.9.9-eclipse-temurin-17 mvn -q package
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
}
if (Get-NetTCPConnection -State Listen -LocalPort 8081 -ErrorAction SilentlyContinue) {
    throw 'Port 8081 is already occupied. Stop the existing application before restarting.'
}
$jar = Get-ChildItem target/*.jar | Select-Object -First 1
$process = Start-Process java -ArgumentList @('-jar', $jar.FullName) -WindowStyle Hidden -PassThru -RedirectStandardOutput local-app.log -RedirectStandardError local-app-error.log
Write-Output "Application PID: $($process.Id). Logs: local-app.log"
