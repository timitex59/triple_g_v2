# One-off setup: registers a Windows Scheduled Task that runs sync_repo.ps1
# every 20 minutes to keep the local clone from drifting behind origin/main
# (see scripts/sync_repo.ps1 for why this repo needs it).
#
# Run this once, interactively, as the user who owns d:\chfjpy_strategy:
#   powershell -ExecutionPolicy Bypass -File scripts\register_sync_task.ps1

$ErrorActionPreference = "Stop"

$taskName   = "CHFJPY_Strategy_GitSync"
$scriptPath = Join-Path $PSScriptRoot "sync_repo.ps1"

$action  = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""

$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes 20) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable -MultipleInstances IgnoreNew

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Description "Keeps d:\chfjpy_strategy in sync with origin/main (CI bot pushes every ~30min)." `
    -Force

Write-Host "Scheduled task '$taskName' registered: runs every 20 minutes."
Write-Host "Logs: $PSScriptRoot\sync_logs\"
Write-Host "To remove: Unregister-ScheduledTask -TaskName '$taskName' -Confirm:`$false"
