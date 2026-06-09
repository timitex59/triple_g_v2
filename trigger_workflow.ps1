<#
.SYNOPSIS
  Declenche le workflow GitHub "Triple G Indicators Scan" via l'API workflow_dispatch.

.DESCRIPTION
  Le declencheur `schedule` de GitHub Actions est "best-effort": il est retarde
  et souvent supprime sous charge. L'API workflow_dispatch, elle, est traitee
  quasi immediatement. Ce script POST sur l'endpoint dispatch pour lancer un run
  fiable a la demande.

  Usage typique:
    - Test manuel:        .\trigger_workflow.ps1 -Token "github_pat_xxx"
    - Via variable d'env: $env:GH_DISPATCH_TOKEN="github_pat_xxx"; .\trigger_workflow.ps1
    - Planificateur (cron-job.org / Task Scheduler) appelant ce script chaque heure.

.NOTES
  Le token doit etre un Fine-grained PAT limite au repo triple_g_v2 avec la
  permission "Actions: Read and write".
#>
param(
    [string]$Token = $env:GH_DISPATCH_TOKEN,
    [string]$Owner = "timitex59",
    [string]$Repo = "triple_g_v2",
    [string]$Workflow = "triple_g_workflow.yml",
    [string]$Ref = "main"
)

if (-not $Token) {
    Write-Error "Token manquant. Passe -Token 'github_pat_...' ou definis `$env:GH_DISPATCH_TOKEN."
    exit 1
}

$uri = "https://api.github.com/repos/$Owner/$Repo/actions/workflows/$Workflow/dispatches"
$headers = @{
    "Authorization"        = "Bearer $Token"
    "Accept"               = "application/vnd.github+json"
    "X-GitHub-Api-Version" = "2022-11-28"
    "User-Agent"           = "triple-g-trigger"
}
$body = @{ ref = $Ref } | ConvertTo-Json

try {
    Invoke-RestMethod -Uri $uri -Method Post -Headers $headers -Body $body -ContentType "application/json" -ErrorAction Stop
    Write-Host "OK - run declenche sur $Owner/$Repo ($Workflow @ $Ref) a $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
}
catch {
    Write-Error "Echec du declenchement: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) { Write-Error $_.ErrorDetails.Message }
    exit 1
}
