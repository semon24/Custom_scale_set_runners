[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateNotNullOrEmpty()]
    [string]$Tag = "latest"
)

$ErrorActionPreference = "Stop"

$ImageRepository = "registry.ft-soft.ru/devops/scale_set_runners"
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$BuildContext = Join-Path $ProjectRoot "scale_set_runners_image"
$Dockerfile = Join-Path $BuildContext "Dockerfile"

if ($Tag -notmatch '^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$') {
    throw "Invalid Docker tag: '$Tag'"
}

if (-not (Test-Path -LiteralPath $Dockerfile)) {
    throw "Dockerfile not found: $Dockerfile"
}

$Image = "${ImageRepository}:$Tag"

Write-Host "Building $Image"
& docker build --file $Dockerfile --tag $Image $BuildContext
if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed with exit code $LASTEXITCODE"
}

Write-Host "Pushing $Image"
& docker push $Image
if ($LASTEXITCODE -ne 0) {
    throw "Docker push failed with exit code $LASTEXITCODE"
}

Write-Host "Published $Image"
