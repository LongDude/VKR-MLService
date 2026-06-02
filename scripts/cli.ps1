param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("tasks", "worker", "data", "ml")]
    [string]$Category,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)

$Container = if ($env:ML_CONTAINER) { $env:ML_CONTAINER } else { "vkr-ml-api" }

& docker exec $Container python -m "cli.$Category" @CliArgs
exit $LASTEXITCODE
