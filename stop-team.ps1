$pidFile = "$PSScriptRoot\data\worker-pids.json"
if (Test-Path $pidFile) {
  Get-Content -Raw $pidFile | ConvertFrom-Json | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
  Remove-Item -LiteralPath $pidFile -Force
}
