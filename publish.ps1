#Requires -Version 7.0
<#
.SYNOPSIS
    Builds and packs a local candidate without publishing it.
.PARAMETER OutputDirectory
    Where the .nupkg/.snupkg land. Defaults to ./artifacts.
.DESCRIPTION
    Public releases are created only by the protected workflow_dispatch path in
    .github/workflows/build.yml. This helper never pushes packages or tags.
#>
[CmdletBinding()]
param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot "artifacts")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$solution = Get-ChildItem -Path $PSScriptRoot -Filter "*.slnx" | Select-Object -First 1
if (-not $solution) {
    throw "No .slnx file found in $PSScriptRoot"
}

Write-Host "==> Building local candidate for $($solution.BaseName)" -ForegroundColor Cyan

if (Test-Path $OutputDirectory) {
    Remove-Item $OutputDirectory -Recurse -Force
}
New-Item -ItemType Directory -Path $OutputDirectory | Out-Null

dotnet restore $solution.FullName
if ($LASTEXITCODE -ne 0) { throw "dotnet restore failed" }

dotnet build $solution.FullName --no-restore --configuration Release
if ($LASTEXITCODE -ne 0) { throw "dotnet build failed" }

dotnet test --solution $solution.FullName --no-build --configuration Release
if ($LASTEXITCODE -ne 0) { throw "dotnet test failed" }

dotnet pack $solution.FullName --no-build --configuration Release --output $OutputDirectory
if ($LASTEXITCODE -ne 0) { throw "dotnet pack failed" }

$packages = @(Get-ChildItem -Path $OutputDirectory -Filter "*.nupkg")
if ($packages.Count -eq 0) {
    throw "No .nupkg produced in $OutputDirectory"
}

foreach ($package in $packages) {
    Write-Host "==> Packed $($package.Name)" -ForegroundColor Green
}

Write-Host "==> Local candidate only. Use the protected workflow_dispatch release path to publish." -ForegroundColor Yellow
