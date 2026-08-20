# Keeps the local clone of triple_g_v2 in sync with origin/main.
# The CI workflow (triple_g_workflow.yml) pushes bot commits to main roughly
# every 30 minutes during trading hours, so this repo drifts behind quickly
# if nobody pulls. This script fetches + fast-forwards, and is safe to run
# unattended: it aborts instead of overwriting anything if local commits or
# uncommitted changes would make the pull non-trivial.
#
# Intended to be run on a schedule (see scripts/register_sync_task.ps1).

$ErrorActionPreference = "Stop"
Set-Location -Path (Split-Path -Parent $PSScriptRoot)

$logDir = Join-Path $PSScriptRoot "sync_logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$logFile = Join-Path $logDir ("sync_{0}.log" -f (Get-Date -Format "yyyy-MM"))

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $logFile -Value $line
}

try {
    $status = git status --porcelain
    if ($status) {
        Write-Log "Skipped: local working tree is dirty, resolve manually."
        exit 0
    }

    git fetch origin main *>> $logFile

    $behind = (git rev-list --count HEAD..origin/main).Trim()
    $ahead  = (git rev-list --count origin/main..HEAD).Trim()

    if ($behind -eq "0") {
        Write-Log "Up to date (0 behind)."
        exit 0
    }

    if ($ahead -ne "0") {
        Write-Log "Skipped: local has $ahead unpushed commit(s) ahead of origin/main; resolve manually (pull --rebase by hand)."
        exit 0
    }

    git merge --ff-only origin/main *>> $logFile
    Write-Log "Fast-forwarded $behind commit(s) from origin/main."
}
catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    exit 1
}
