#!/bin/sh
# dshe installer — downloads the latest release binary into ~/.local/bin
# usage: curl -fsSL https://plinthlol.github.io/dashe/install.sh | sh
set -e

REPO="plinthlol/dashe"

os=$(uname -s)
arch=$(uname -m)

case "$os" in
    Linux) os_id="linux" ;;
    Darwin) os_id="macos" ;;
    *) echo "error: unsupported OS '$os'" >&2; exit 1 ;;
esac

case "$arch" in
    x86_64|amd64) arch_id="x86_64" ;;
    aarch64|arm64) arch_id="aarch64" ;;
    *) echo "error: unsupported architecture '$arch'" >&2; exit 1 ;;
esac

if [ "$os_id" = "macos" ] && [ "$arch_id" = "x86_64" ]; then
    echo "note: only apple silicon builds are published; installing the aarch64 build (runs via rosetta)" >&2
    arch_id="aarch64"
fi

asset="dshe-${os_id}-${arch_id}.tar.gz"
url="https://github.com/${REPO}/releases/latest/download/${asset}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "downloading ${asset}..."
if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$tmp/dshe.tar.gz"
elif command -v wget >/dev/null 2>&1; then
    wget -qO "$tmp/dshe.tar.gz" "$url"
else
    echo "error: need curl or wget to download" >&2
    exit 1
fi

tar -xzf "$tmp/dshe.tar.gz" -C "$tmp"
bin_path=$(find "$tmp" -type f -name dshe | head -1)
[ -n "$bin_path" ] || { echo "error: binary not found in archive" >&2; exit 1; }

mkdir -p "$HOME/.local/bin"
mv "$bin_path" "$HOME/.local/bin/dshe"
chmod +x "$HOME/.local/bin/dshe"

echo "installed: $HOME/.local/bin/dshe"

case ":$PATH:" in
    *":$HOME/.local/bin:"*) ;;
    *)
        echo ""
        echo "note: $HOME/.local/bin is not in your PATH."
        echo "add this to your shell profile (~/.bashrc, ~/.zshrc, ...):"
        echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
        ;;
esac

"$HOME/.local/bin/dshe" --version
