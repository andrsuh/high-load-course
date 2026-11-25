param([int]$TestCount = 1000, [int]$RatePerSecond = 5)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$config = @{}
Get-Content .env | ForEach-Object {
    if ($_ -match '^([^#=]+)=(.*)$') { $config[$matches[1]] = $matches[2] }
}
$body = @{
    serviceName = $config['PAYMENT_SERVICE_NAME']
    token = $config['PAYMENT_TOKEN']
    ratePerSecond = $RatePerSecond
    testCount = $TestCount
    processingTimeMillis = 6000
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://localhost:1234/test/run' -ContentType 'application/json' -Body $body -TimeoutSec 900
