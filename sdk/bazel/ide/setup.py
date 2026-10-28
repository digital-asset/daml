import ide_setup


def main() -> None:
    root = ide_setup.workspace()
    ide_setup.announce(root, ide_setup.record_bazel(root))
    print()
    print("Point your editor's Haskell language server at:")
    print(f"  {root / ide_setup.HLS_WRAPPER} --lsp")
    print(f"or add {root / ide_setup.HLS_BIN} to the editor's PATH, since most clients run")
    print("`haskell-language-server-wrapper --lsp` by default.")
    print()
    print("Ready-made setups: bazelisk run //:vscode_setup | //:vim_setup | //:emacs_setup | //:intellij_setup")


if __name__ == "__main__":
    main()
