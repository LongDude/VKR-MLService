param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("tasks", "worker", "data", "ml")]
    [string]$Category,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)

$Root = Split-Path -Parent $PSScriptRoot
$Candidates = @(
    (Join-Path $Root ".venv\Scripts\python.exe"),
    (Join-Path $Root ".win-dev\Scripts\python.exe"),
    (Join-Path $Root ".modern-utils\Scripts\python.exe"),
    (Join-Path $Root ".venv-lin\Scripts\python.exe"),
    (Join-Path $Root ".venv-lin\bin\python")
)
$Python = $Candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1

if (-not $Python) {
    throw "Python virtual environment not found. Run scripts/init-win-venv.ps1 first."
}

$env:PYTHONPATH = Join-Path $Root "src"
Push-Location $Root
try {
    & $Python -m "cli.$Category" @CliArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
