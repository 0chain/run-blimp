# Homebrew formula for the blimp CLI.
#   brew tap 0chain/blimp https://github.com/0chain/run-blimp
#   brew install blimp
class Blimp < Formula
  desc "Memory for Claude/OpenAI and Blimp nodes on your own computer"
  homepage "https://blimp.software"
  url "https://github.com/0chain/run-blimp/archive/refs/tags/v0.1.1.tar.gz"
  sha256 "b604db2868e5fcb699d161c9d9e753565d6c30ca954c7c3467c4d39c8aaa2602"
  version "0.1.1"

  depends_on "python@3.12"

  def install
    libexec.install Dir["*"]
    # The node commands (login, launch, install, ...) create their Python venv under
    # ~/.blimp on first use; give them Homebrew's Python (macOS's python3 is 3.9).
    (bin/"blimp").write_env_script libexec/"blimp",
      BLIMP_VENV_PYTHON: Formula["python@3.12"].opt_bin/"python3.12"
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
