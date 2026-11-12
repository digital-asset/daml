# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import sys
from pathlib import Path

import setup_ide

DIR_LOCALS = Path(".dir-locals.el")
MODES = ("haskell-mode", "haskell-ts-mode")


def render(wrapper: Path) -> str:
    path = json.dumps(str(wrapper))
    body = (
        f"  . ((eval . (setq-local lsp-haskell-server-path {path}))\n"
        f"     (eval . (setq-local eglot-server-programs\n"
        f"                         (cons (list major-mode {path} \"--lsp\")\n"
        f"                               (bound-and-true-p eglot-server-programs))))))"
    )
    entries = [f"({mode}\n{body}" for mode in MODES]
    return "(" + "\n ".join(entries) + ")\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="bazelisk run //:setup_emacs --")
    parser.add_argument("--remove", action="store_true", help=f"delete the generated {DIR_LOCALS}")
    args = parser.parse_args(argv)

    root = setup_ide.workspace()
    wrapper = root / setup_ide.HLS_WRAPPER
    target = root / DIR_LOCALS
    content = render(wrapper)
    signature = str(wrapper)

    if args.remove:
        result = setup_ide.remove_generated_file(target, content, signature)
        print(f"{result} {target}")
        return 1 if result == "conflict" else 0

    result = setup_ide.install_generated_file(target, content, signature)
    print(f"{result} {target}")
    if result == "conflict":
        print(f"{target} was not written by this setup; merge this into it:")
        for line in content.splitlines():
            print(f"  {line}")

    print()
    setup_ide.announce(root, setup_ide.record_bazel(root))
    print(f"{DIR_LOCALS} points lsp-haskell (lsp-mode) and eglot at it for {' and '.join(MODES)} buffers.")
    print("Emacs asks once whether to apply these local variables; answer `!` to trust them permanently.")
    print("Then start the client as usual: M-x eglot or M-x lsp.")
    return 1 if result == "conflict" else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
