#!/usr/bin/env sh
# blimp installer — one command, open repo, no Docker required.
#
#   curl -fsSL https://raw.githubusercontent.com/0chain/run-blimp/main/install.sh | sh
#
# Fetches the kit into $BLIMP_HOME (default /opt/blimp) and links `blimp` onto
# your PATH. Then `blimp --setup` installs its own remaining prerequisites
# (docker for the catalog, python+pyiceberg, aws, duckdb) — so a bare node goes
# from nothing to a wired, validated Blimp source with two commands total.
#
# Overrides: BLIMP_REPO, BLIMP_REF (branch/tag), BLIMP_HOME, BLIMP_BIN.
set -eu

REPO="${BLIMP_REPO:-https://github.com/0chain/run-blimp}"
REF="${BLIMP_REF:-main}"
HOME_DIR="${BLIMP_HOME:-/opt/blimp}"
BIN_DIR="${BLIMP_BIN:-/usr/local/bin}"

say(){ printf '\033[1m[install]\033[0m %s\n' "$*"; }
SUDO=''; [ "$(id -u)" -eq 0 ] || SUDO='sudo'

say "installing blimp from $REPO@$REF into $HOME_DIR"
$SUDO mkdir -p "$HOME_DIR"

# Prefer git; fall back to a codeload tarball so a node without git still works.
if command -v git >/dev/null 2>&1; then
  if [ -d "$HOME_DIR/.git" ]; then
    $SUDO git -C "$HOME_DIR" fetch -q origin "$REF" && $SUDO git -C "$HOME_DIR" reset -q --hard "origin/$REF"
  else
    $SUDO rm -rf "$HOME_DIR"; $SUDO git clone -q --branch "$REF" --depth 1 "$REPO" "$HOME_DIR"
  fi
else
  say "git not found — downloading tarball"
  tmp="$(mktemp -d)"
  curl -fsSL "$REPO/archive/refs/heads/$REF.tar.gz" -o "$tmp/kit.tgz" \
    || curl -fsSL "$REPO/archive/refs/tags/$REF.tar.gz" -o "$tmp/kit.tgz"
  tar -xzf "$tmp/kit.tgz" -C "$tmp"
  $SUDO rm -rf "$HOME_DIR"; $SUDO mkdir -p "$HOME_DIR"
  $SUDO cp -R "$tmp"/*/* "$HOME_DIR"/; rm -rf "$tmp"
fi

$SUDO chmod +x "$HOME_DIR/blimp" "$HOME_DIR"/*.sh 2>/dev/null || true
$SUDO mkdir -p "$BIN_DIR"
$SUDO ln -sf "$HOME_DIR/blimp" "$BIN_DIR/blimp"

say "installed: $($BIN_DIR/blimp >/dev/null 2>&1 && echo ok) -> $BIN_DIR/blimp"
say "next: blimp --setup   (it installs docker/python/aws/duckdb as needed)"
