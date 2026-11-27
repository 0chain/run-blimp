# blimp installer for Windows 10 (2004+) / 11 — run in PowerShell:
#
#   irm https://raw.githubusercontent.com/0chain/run-blimp/main/install.ps1 | iex
#
# Installs the blimp CLI for this user (no administrator needed):
#   - uv and a prebuilt Python 3.12, both under %USERPROFILE%\.blimp (no system Python needed)
#   - the blimp-memory package (release wheel) in %USERPROFILE%\.blimp\venv
#   - %USERPROFILE%\.blimp\bin\blimp.cmd, added to your user PATH
# Then:  blimp login   and   blimp launch --local   (sets up WSL2 on first run; that step asks
# for administrator permission once and may need one restart).
#
# Overrides: $env:BLIMP_NODE_PKG (wheel URL or path), $env:BLIMP_NODE_HOME.

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'   # Invoke-WebRequest is very slow with the progress bar

$BlimpHome = if ($env:BLIMP_NODE_HOME) { $env:BLIMP_NODE_HOME } else { Join-Path $env:USERPROFILE '.blimp' }
$Pkg = if ($env:BLIMP_NODE_PKG) { $env:BLIMP_NODE_PKG } else {
  'https://github.com/0chain/run-blimp/releases/download/v0.1.3/blimp_memory-0.1.3-py3-none-any.whl' }
# The node's Python comes from uv (one prebuilt binary, checksum pinned) and uv's prebuilt
# CPython, both under %USERPROFILE%\.blimp: no Python install, winget or python.org installer,
# and no administrator. The same path as the macOS/Linux blimp script.
$UvVer = '0.13.10'
$UvSha = @{
  'x86_64-pc-windows-msvc'  = '297b3881fe1adfb922becc2c1613202abb41dc83ea5ff43d472aa087ca1f8683'
  'aarch64-pc-windows-msvc' = 'f223bcd3fa010174ffeeb885cc1d5334bb81388640c65baa5b8b42b47017337e'
}

function Say($m) { Write-Host "[install] $m" -ForegroundColor Cyan }

if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') {
  Write-Warning 'Windows on ARM: the CLI installs, but `blimp launch --local` is not supported yet (no ARM build of the node images).'
}

New-Item -ItemType Directory -Force -Path (Join-Path $BlimpHome 'bin') | Out-Null
$uv = Join-Path $BlimpHome 'bin\uv.exe'
$have = if (Test-Path $uv) { ((& $uv --version) -split ' ')[1] } else { '' }
if ($have -ne $UvVer) {
  $t = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'aarch64-pc-windows-msvc' } else { 'x86_64-pc-windows-msvc' }
  $zip = Join-Path $env:TEMP "uv-$UvVer-$t.zip"
  Say "fetching uv $UvVer ($t)"
  Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/astral-sh/uv/releases/download/$UvVer/uv-$t.zip" -OutFile $zip
  $got = (Get-FileHash -Algorithm SHA256 $zip).Hash.ToLower()
  if ($got -ne $UvSha[$t]) { throw "uv download checksum mismatch ($got, expected $($UvSha[$t]))" }
  $dir = Join-Path $env:TEMP "uv-$UvVer-$t"
  Expand-Archive -Path $zip -DestinationPath $dir -Force
  Copy-Item -Force (Get-ChildItem -Path $dir -Recurse -Filter 'uv.exe' | Select-Object -First 1).FullName $uv
  Remove-Item $zip -Force -ErrorAction SilentlyContinue
}
# uv's Python installs and cache stay under the node home, never in the user's own uv setup
$env:UV_PYTHON_INSTALL_DIR = Join-Path $BlimpHome 'python'
$env:UV_CACHE_DIR = Join-Path $BlimpHome 'cache\uv'
$env:UV_NO_CONFIG = '1'

New-Item -ItemType Directory -Force -Path $BlimpHome | Out-Null
$venvPy = Join-Path $BlimpHome 'venv\Scripts\python.exe'
if ((Test-Path $venvPy) -and -not (& { try { & $venvPy -c '' 2>$null; $LASTEXITCODE -eq 0 } catch { $false } })) {
  # the venv's base Python is gone: set it aside (kept, not deleted) and build a new one
  $aside = Join-Path $BlimpHome ("venv.broken-" + (Get-Date -Format 'yyyyMMddHHmmss'))
  Say "the memory node's Python no longer runs; moving the venv to $aside"
  Move-Item (Join-Path $BlimpHome 'venv') $aside
}
if (-not (Test-Path $venvPy)) {
  Say "creating $BlimpHome\venv (Python 3.12, prebuilt)"
  & $uv venv -q --python 3.12 --managed-python (Join-Path $BlimpHome 'venv')
  if ($LASTEXITCODE -ne 0) { throw 'creating the venv failed' }
}
Say "installing blimp-memory ($Pkg)"
& $uv pip install -q --python $venvPy --upgrade $Pkg
if ($LASTEXITCODE -ne 0) { throw "installing blimp-memory failed ($Pkg)" }

$bin = Join-Path $BlimpHome 'bin'
New-Item -ItemType Directory -Force -Path $bin | Out-Null
$shim = Join-Path $bin 'blimp.cmd'
Set-Content -Path $shim -Encoding ASCII -Value @(
  '@echo off',
  "set BLIMP_NODE_HOME=$BlimpHome",
  "`"$venvPy`" -m blimp_memory.cli %*"
)

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not ($userPath -split ';' | Where-Object { $_ -eq $bin })) {
  $new = if ($userPath) { "$userPath;$bin" } else { $bin }
  [Environment]::SetEnvironmentVariable('Path', $new, 'User')
  Say "added $bin to your user PATH"
}
if (-not ($env:Path -split ';' | Where-Object { $_ -eq $bin })) { $env:Path = "$env:Path;$bin" }

& $shim --help | Out-Null
Say 'installed: blimp'
Say 'next:  blimp login        (paste the API TOKEN from the Nodes page)'
Say '       blimp launch --local'
