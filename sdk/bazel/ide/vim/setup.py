import argparse
import sys
from pathlib import Path

import ide_setup

EXRC = Path(".exrc")
SHADOWING_FILES = (".nvim.lua", ".nvimrc", ".vimrc", "_vimrc")


def vim_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render(hls_bin: Path) -> str:
    path = vim_string(str(hls_bin))
    return (
        f"if index(split($PATH, ':'), {path}) < 0\n"
        f"  let $PATH = {path} . ':' . $PATH\n"
        "endif\n"
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="bazelisk run //:vim_setup --")
    parser.add_argument("--remove", action="store_true", help=f"delete the generated {EXRC}")
    args = parser.parse_args(argv)

    root = ide_setup.workspace()
    hls_bin = root / ide_setup.HLS_BIN
    target = root / EXRC
    content = render(hls_bin)
    signature = str(hls_bin)

    if args.remove:
        result = ide_setup.remove_generated_file(target, content, signature)
        print(f"{result} {target}")
        return 1 if result == "conflict" else 0

    result = ide_setup.install_generated_file(target, content, signature)
    print(f"{result} {target}")
    if result == "conflict":
        print(f"{target} was not written by this setup; add these lines to it:")
        for line in content.splitlines():
            print(f"  {line}")

    shadowing = [name for name in SHADOWING_FILES if (root / name).exists()]
    if shadowing:
        print(f"note: {', '.join(shadowing)} in {root} is read instead of {EXRC}; add the lines above there")

    print()
    ide_setup.announce(root, ide_setup.record_bazel(root))
    print(f"{EXRC} puts {hls_bin} first on Vim's PATH, so any LSP client that runs")
    print("`haskell-language-server-wrapper` (nvim-lspconfig hls, coc.nvim, ALE, vim-lsp) picks it up.")
    print("Project files are only read with 'exrc' enabled:")
    print("  Neovim: vim.o.exrc = true   (then answer the :trust prompt once)")
    print("  Vim:    set exrc secure")
    print(f"Start the editor from {root}.")
    return 1 if result == "conflict" else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
