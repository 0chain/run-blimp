# Homebrew formula for the blimp CLI.
#   brew tap 0chain/blimp https://github.com/0chain/run-blimp
#   brew install blimp
class Blimp < Formula
  desc "Memory for Claude/OpenAI and Blimp nodes on your own computer"
  homepage "https://blimp.software"
  url "https://github.com/0chain/run-blimp/archive/refs/tags/v0.1.5.tar.gz"
  sha256 "14e1b678dd28d158e63be7e9154a8f35098479759de50088e27b2085fc2faaec"
  version "0.1.5"

  # No Python dependency: the node commands fetch uv and a prebuilt Python 3.12 into ~/.blimp
  # on first use. Depending on python@3.12 made `brew upgrade blimp` compile Python from source
  # on Intel Macs (Homebrew ships no Intel bottles), 20-40 minutes for a script update.

  def install
    libexec.install Dir["*"]
    bin.write_exec_script libexec/"blimp"
  end

  def caveats
    <<~EOS
      Run a Blimp node on this computer (sets up a Lima VM, no ports to open):
        blimp login
        blimp launch --local
      Memory for Claude Code / Claude Desktop / Codex:
        blimp install && blimp connect claude-code
    EOS
  end

  test do
    assert_match "blimp", shell_output("#{bin}/blimp 2>&1")
  end
end
