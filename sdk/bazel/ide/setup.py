# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import setup_ide


def main() -> None:
    root = setup_ide.workspace()
    setup_ide.announce(root, setup_ide.record_bazel(root))
    print()
    print("Point your editor's Haskell language server at:")
    print(f"  {root / setup_ide.HLS_WRAPPER} --lsp")
    print(f"or add {root / setup_ide.HLS_BIN} to the editor's PATH, since most clients run")
    print("`haskell-language-server-wrapper --lsp` by default.")
    print()
    print("Ready-made setups: bazelisk run //:setup_vscode | //:setup_vim | //:setup_emacs | //:setup_intellij")


if __name__ == "__main__":
    main()
