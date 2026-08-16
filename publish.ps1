#Requires -Version 7.0
<#
.SYNOPSIS
    Builds this package in Release configuration, packs it, and pushes it to NuGet.org.
.PARAMETER ApiKey
    NuGet.org API key. Defaults to $env:NUGET_API_KEY.
.PARAMETER OutputDirectory
    Where the .nupkg/.snupkg land. Defaults to ./artifacts.
.PARAMETER DryRun
    Build and pack, but skip the final `dotnet nuget push`.
#>
[CmdletBinding()]
param(
    [string]$ApiKey = $env:NUGET_API_KEY,
    [string]$OutputDirectory = (Join-Path $PSScriptRoot "artifacts"),
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$solution = Get-ChildItem -Path $PSScriptRoot -Filter "*.slnx" | Select-Object -First 1
if (-not $solution) {
    throw "No .slnx file found in $PSScriptRoot"
}

if (-not $DryRun -and [string]::IsNullOrWhiteSpace($ApiKey)) {
    throw "No NuGet API key provided. Pass -ApiKey, set `$env:NUGET_API_KEY, or use -DryRun to build/pack without publishing."
}

Write-Host "==> Publishing $($solution.BaseName)" -ForegroundColor Cyan

if (Test-Path $OutputDirectory) {
    Remove-Item $OutputDirectory -Recurse -Force
}
New-Item -ItemType Directory -Path $OutputDirectory | Out-Null

Write-Host "==> Restoring workloads" -ForegroundColor Cyan
dotnet workload restore $solution.FullName
if ($LASTEXITCODE -ne 0) { throw "dotnet workload restore failed" }

Write-Host "==> Restoring packages" -ForegroundColor Cyan
dotnet restore $solution.FullName
if ($LASTEXITCODE -ne 0) { throw "dotnet restore failed" }

Write-Host "==> Building (Release)" -ForegroundColor Cyan
dotnet build $solution.FullName --no-restore --configuration Release
if ($LASTEXITCODE -ne 0) { throw "dotnet build failed" }

Write-Host "==> Packing (Release)" -ForegroundColor Cyan
dotnet pack $solution.FullName --no-build --configuration Release --output $OutputDirectory
if ($LASTEXITCODE -ne 0) { throw "dotnet pack failed" }

$packages = @(Get-ChildItem -Path $OutputDirectory -Filter "*.nupkg")
if ($packages.Count -eq 0) {
    throw "No .nupkg produced in $OutputDirectory"
}

foreach ($package in $packages) {
    Write-Host "==> Packed $($package.Name)" -ForegroundColor Green
}

if ($DryRun) {
    Write-Host "==> Dry run: skipping nuget push" -ForegroundColor Yellow
    exit 0
}

Write-Host "==> Pushing to NuGet.org" -ForegroundColor Cyan
foreach ($package in $packages) {
    # nuget.org auto-discovers and pushes a matching .snupkg alongside the .nupkg, no separate step needed.
    dotnet nuget push $package.FullName --api-key $ApiKey --source https://api.nuget.org/v3/index.json --skip-duplicate
    if ($LASTEXITCODE -ne 0) { throw "dotnet nuget push failed for $($package.Name)" }
}

Write-Host "==> Done" -ForegroundColor Green
