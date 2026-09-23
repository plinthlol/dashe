# dshe installer — downloads the latest release binary and adds it to your PATH
# usage: irm https://plinthlol.github.io/dashe/install.ps1 | iex
$ErrorActionPreference = "Stop"

$repo = "plinthlol/dashe"
$asset = "dshe-windows-x86_64.zip"
$url = "https://github.com/$repo/releases/latest/download/$asset"

$tmp = Join-Path $env:TEMP "dshe-install"
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
$zip = Join-Path $tmp $asset

Write-Host "downloading $asset..."
Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
Expand-Archive -Path $zip -DestinationPath $tmp -Force

$exe = Get-ChildItem -Path $tmp -Recurse -Filter "dshe.exe" | Select-Object -First 1
if (-not $exe) {
    Write-Host "error: binary not found in archive" -ForegroundColor Red
    exit 1
}

$installDir = Join-Path $env:LOCALAPPDATA "Programs\dshe"
New-Item -ItemType Directory -Force -Path $installDir | Out-Null
Copy-Item $exe.FullName (Join-Path $installDir "dshe.exe") -Force

# add to the user PATH (persists across sessions)
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ";") -notcontains $installDir) {
    $newPath = if ([string]::IsNullOrEmpty($userPath)) { $installDir } else { "$userPath;$installDir" }
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    $env:Path += ";$installDir"
    Write-Host "added $installDir to your user PATH (restart your terminal to pick it up)"
}

& (Join-Path $installDir "dshe.exe") --version
Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
Write-Host "installed: $installDir\dshe.exe"
