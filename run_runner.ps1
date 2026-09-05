$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$env:PYTHONUTF8 = "1"
$env:ROBOT_CONSOLE_DETAIL = "compact"
$logDir = Join-Path $PSScriptRoot "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$env:ROBOT_LOG_FILE = Join-Path $logDir "runner_$stamp.log"

Write-Host "Robot Elliot starting in compact mode."
Write-Host "UTF-8 console log: $env:ROBOT_LOG_FILE"
Write-Host "Detailed data: $PSScriptRoot\debug and analysis_archive"

& ".\.venv\Scripts\python.exe" -X utf8 -u runner.py
