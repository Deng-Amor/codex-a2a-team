param(
  [Parameter(Mandatory = $true)][string]$Title,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Repository,
  [switch]$AutoMerge
)

$repoPath = (Resolve-Path -LiteralPath $Repository).Path
$listening = Get-NetTCPConnection -LocalPort 4318 -State Listen -ErrorAction SilentlyContinue
if (-not $listening) {
  Start-Process -FilePath powershell -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', "$PSScriptRoot\run-team.ps1", '-Repository', $repoPath) -WindowStyle Hidden
  foreach ($attempt in 1..40) {
    if (Get-NetTCPConnection -LocalPort 4318 -State Listen -ErrorAction SilentlyContinue) { break }
    Start-Sleep -Milliseconds 250
  }
}
if (-not (Get-NetTCPConnection -LocalPort 4318 -State Listen -ErrorAction SilentlyContinue)) { throw 'A2A Broker did not start on port 4318.' }
$payload = @{ title = $Title; request = $Request; repository = $repoPath; autoMerge = [bool]$AutoMerge } | ConvertTo-Json
$workflow = Invoke-RestMethod 'http://127.0.0.1:4318/api/workflows' -Method Post -ContentType 'application/json' -Body $payload
Invoke-RestMethod "http://127.0.0.1:4318/api/workflows/$($workflow.id)/confirm" -Method Post | Out-Null
Start-Process 'http://127.0.0.1:4318'
$workflow.id
