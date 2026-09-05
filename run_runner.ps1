$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$env:ROBOT_CONSOLE_DETAIL = "compact"
$logDir = Join-Path $PSScriptRoot "logs"
New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logDir "runner_$stamp.log"

Write-Host "Robot Elliot запускается в компактном режиме."
Write-Host "Журнал консоли: $logPath"
Write-Host "Подробные данные: $PSScriptRoot\debug и analysis_archive"

& ".\.venv\Scripts\python.exe" -X utf8 -u runner.py 2>&1 |
    Tee-Object -FilePath $logPath
