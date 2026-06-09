# Déclenchement horaire fiable du workflow

Le déclencheur `schedule` de GitHub Actions est **best-effort** : sous charge, GitHub
retarde (5–40 min) et **supprime** une grande partie des exécutions planifiées. D'où les
runs « saccadés » au lieu d'une cadence horaire.

**Solution recommandée** : un planificateur **externe** appelle l'API `workflow_dispatch`
de GitHub chaque heure. Cet appel est traité quasi immédiatement (pas la file best-effort).

Le workflow expose déjà `workflow_dispatch:`, donc rien à changer côté workflow.

---

## Étape 1 — Créer un token (Fine-grained PAT)

1. https://github.com/settings/personal-access-tokens/new
2. **Repository access** → *Only select repositories* → `triple_g_v2`
3. **Permissions** → *Repository permissions* → **Actions: Read and write**
   (laisse le reste sur *No access*; `Metadata: Read` se met seul)
4. Expiration : 1 an (à renoter dans ton agenda pour le renouveler)
5. **Generate token** → copie `github_pat_...` (visible une seule fois)

---

## Étape 2 — Configurer le planificateur cloud (cron-job.org, gratuit, toujours actif)

1. Compte sur https://cron-job.org → **Create cronjob**
2. **URL** :
   ```
   https://api.github.com/repos/timitex59/triple_g_v2/actions/workflows/triple_g_workflow.yml/dispatches
   ```
3. **Request method** : `POST`
4. **Request headers** (onglet Advanced) :
   ```
   Authorization: Bearer github_pat_xxxxxxxx
   Accept: application/vnd.github+json
   X-GitHub-Api-Version: 2022-11-28
   Content-Type: application/json
   ```
5. **Request body** :
   ```json
   {"ref":"main"}
   ```
6. **Schedule** : reproduis ta fenêtre actuelle (UTC) — minute **15**, heures **4→22**,
   jours **lundi→vendredi**. Mets le fuseau du cronjob sur **UTC** pour coller au cron
   d'origine. (cron-job.org permet de cocher heures/jours précis.)
7. **Save**. La réponse attendue de GitHub est **HTTP 204** (succès, sans corps).

> Test immédiat : bouton **Run now** sur cron-job.org → un run doit apparaître dans
> l'onglet Actions du repo en quelques secondes.

---

## Alternative — depuis ton PC (script fourni)

`trigger_workflow.ps1` fait le même appel. Pour un test manuel :

```powershell
.\trigger_workflow.ps1 -Token "github_pat_xxxxxxxx"
```

Pour l'automatiser via le **Planificateur de tâches Windows** (si ton PC reste allumé) :

```powershell
$action  = New-ScheduledTaskAction -Execute "powershell.exe" `
  -Argument '-NoProfile -File "E:\chfjpy_strategy\trigger_workflow.ps1"'
$trigger = New-ScheduledTaskTrigger -Once -At 6:15am `
  -RepetitionInterval (New-TimeSpan -Hours 1)
$env:GH_DISPATCH_TOKEN = "github_pat_xxxxxxxx"  # ou stocke-le dans les variables d'env utilisateur
Register-ScheduledTask -TaskName "TripleG hourly trigger" -Action $action -Trigger $trigger
```

> ⚠️ cron-job.org (cloud) est plus fiable qu'un PC personnel qui peut être éteint/en veille.

---

## Faut-il garder le `schedule` dans le workflow ?

- **Oui, garde-le** comme filet de sécurité : il ne coûte rien et le bloc
  `concurrency` (`cancel-in-progress: false`) sérialise les runs, donc pas de
  double exécution gênante si les deux déclencheurs tombent ensemble.
- Si tu vois trop de doublons, supprime le bloc `schedule:` une fois le
  déclencheur externe confirmé.

## Pourquoi le week-end était vide

Le cron `15 4-22 * * 1-5` finit par `1-5` = **lundi→vendredi**. Aucun run samedi/dimanche,
c'est voulu. Si tu veux le week-end, ajuste les jours côté cron-job.org.
