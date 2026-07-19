param(
  [Parameter(Mandatory = $true)][string]$Repository
)

$env:A2A_REPOSITORY = (Resolve-Path -LiteralPath $Repository)
$roles = (Get-Content -Raw "$PSScriptRoot\agents.json" | ConvertFrom-Json).psobject.Properties.Name
New-Item -ItemType Directory -Force -Path "$PSScriptRoot\data" | Out-Null
$pids = foreach ($role in $roles) {
  (Start-Process -FilePath node -ArgumentList "worker.mjs $role" -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru).Id
}
$pids | ConvertTo-Json | Set-Content "$PSScriptRoot\data\worker-pids.json"
node "$PSScriptRoot\server.mjs"
