# blimp installer for Windows 10 (2004+) / 11 — run in PowerShell:
#
#   irm https://raw.githubusercontent.com/0chain/run-blimp/main/install.ps1 | iex
#
# Installs the blimp CLI for this user (no administrator needed):
#   - Python 3.10+ (uses an existing one; else winget; else the python.org installer, per-user)
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
$PyVersion = '3.12.10'

function Say($m) { Write-Host "[install] $m" -ForegroundColor Cyan }

if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') {
  Write-Warning 'Windows on ARM: the CLI installs, but `blimp launch --local` is not supported yet (no ARM build of the node images).'
}

function Test-Python($exe, [string[]]$pre) {
  try {
    & $exe @pre -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>$null
    return ($LASTEXITCODE -eq 0)
  } catch { return $false }
}

function Find-Python {
  $cands = @(
    @{ exe = 'py'; pre = @('-3.12') }, @{ exe = 'py'; pre = @('-3.13') }, @{ exe = 'py'; pre = @('-3.11') },
    @{ exe = 'py'; pre = @('-3.10') }, @{ exe = 'python'; pre = @() },
    @{ exe = (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python312\python.exe'); pre = @() }
  )
  foreach ($c in $cands) {
    if ((Get-Command $c.exe -ErrorAction SilentlyContinue) -or (Test-Path $c.exe)) {
      # skip the Microsoft Store alias stub (it opens the Store instead of running)
      $src = (Get-Command $c.exe -ErrorAction SilentlyContinue).Source
      if ($src -and $src -like '*WindowsApps*') { continue }
      if (Test-Python $c.exe $c.pre) { return $c }
    }
  }
  return $null
}

$py = Find-Python
if (-not $py) {
  if (Get-Command winget -ErrorAction SilentlyContinue) {
    Say 'installing Python 3.12 with winget (per user)'
    winget install -e --id Python.Python.3.12 --scope user --silent --accept-package-agreements --accept-source-agreements | Out-Null
  } else {
    $arch = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'arm64' } else { 'amd64' }
    $url = "https://www.python.org/ftp/python/$PyVersion/python-$PyVersion-$arch.exe"
    $exe = Join-Path $env:TEMP "python-$PyVersion-$arch.exe"
    Say "downloading $url"
    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile $exe
    $sig = Get-AuthenticodeSignature $exe
    if ($sig.Status -ne 'Valid' -or $sig.SignerCertificate.Subject -notmatch 'Python Software Foundation') {
      throw "the Python installer is not signed by the Python Software Foundation ($($sig.Status))"
    }
    Say 'installing Python (per user, no administrator)'
    Start-Process -FilePath $exe -ArgumentList '/quiet', 'InstallAllUsers=0', 'PrependPath=0', 'Include_launcher=1', 'Include_test=0' -Wait
    Remove-Item $exe -Force -ErrorAction SilentlyContinue
  }
  $py = Find-Python
  if (-not $py) { throw 'Python 3.10+ was not found after installing it; open a new PowerShell and run this again.' }
}
Say ("using Python: {0} {1}" -f $py.exe, ($py.pre -join ' '))

New-Item -ItemType Directory -Force -Path $BlimpHome | Out-Null
$venvPy = Join-Path $BlimpHome 'venv\Scripts\python.exe'
if (-not (Test-Path $venvPy)) {
  Say "creating $BlimpHome\venv"
  & $py.exe @($py.pre) -m venv (Join-Path $BlimpHome 'venv')
  if ($LASTEXITCODE -ne 0) { throw 'python -m venv failed' }
}
& $venvPy -m pip install -q --upgrade pip
Say "installing blimp-memory ($Pkg)"
& $venvPy -m pip install -q --upgrade $Pkg
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
