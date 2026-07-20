param(
  [Parameter(Mandatory = $true)][string]$Title,
  [Parameter(Mandatory = $true)][string]$Request,
  [Parameter(Mandatory = $true)][string]$Repository
)

if (-not (Test-Path -LiteralPath $Repository)) { throw "Repository not found: $Repository" }
if (-not (Get-NetTCPConnection -LocalPort 8010 -State Listen -ErrorAction SilentlyContinue)) {
  throw 'FastAPI is not running on port 8010. Start it with the README command first.'
}
if (-not (Get-NetTCPConnection -LocalPort 20002 -State Listen -ErrorAction SilentlyContinue)) {
  throw 'Dashboard is not running on port 20002. Start Vite with the README command first.'
}

$payload = @{ title = $Title; request = $Request; engine = 'langgraph_v1' } | ConvertTo-Json
$workflow = Invoke-RestMethod 'http://127.0.0.1:8010/api/workflows' -Method Post -ContentType 'application/json' -Body $payload
Start-Process 'http://127.0.0.1:20002'
$workflow.id
