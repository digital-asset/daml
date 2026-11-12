# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import shutil
import subprocess
import sys

from python.runfiles import runfiles

hook_name = "pre-commit"
setup_command = "bazelisk run //:setup_git_hooks"
ownership_marker = "//:setup_git_hooks"
checks_assignment = "checks="
variants = {"fast": "//bazel/checks:fast", "full": "//bazel/checks"}
default_variant = "fast"
usage = "usage: git-hooks [deploy|update] [fast|full]\n       git-hooks remove"
git = os.environ.get("GIT", "git")


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" not in os.environ:
        sys.exit(f"this writes into the checkout's git hooks; run it as: {setup_command}")
    return os.environ["BUILD_WORKSPACE_DIRECTORY"]


def hooks_directory():
    configured = subprocess.run(
        [git, "config", "--get", "core.hooksPath"],
        capture_output=True,
        encoding="utf-8",
    ).stdout.strip()
    if configured:
        sys.exit(f"core.hooksPath is set to {configured}, so git ignores .git/hooks; unset it to use this setup")
    common_directory = subprocess.check_output([git, "rev-parse", "--git-common-dir"], encoding="utf-8").strip()
    return os.path.join(os.path.abspath(common_directory), "hooks")


def read_text(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def write_hook(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    staging = path + ".tmp"
    with open(staging, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    os.chmod(staging, 0o755)
    os.replace(staging, path)


def back_up(path):
    backup = path + ".bak"
    shutil.copy2(path, backup)
    return backup


def hook_content(template, variant):
    return "".join(
        f"{checks_assignment}{variants[variant]}\n" if line.startswith(checks_assignment) else line
        for line in template.splitlines(keepends=True)
    )


def deployed_variant(path):
    if os.path.islink(path) or not os.path.isfile(path):
        return None
    assignments = {f"{checks_assignment}{checks}": variant for variant, checks in variants.items()}
    return next((assignments[line] for line in read_text(path).splitlines() if line in assignments), None)


def installed_state(path, content):
    if not os.path.lexists(path):
        return "absent"
    if os.path.islink(path) or not os.path.isfile(path):
        return "foreign"
    current = read_text(path)
    if current == content:
        return "current"
    if ownership_marker in current:
        return "outdated"
    return "foreign"


def report_foreign(path):
    print(f"{path} was not deployed by this setup, so it was left alone.")
    print("Move it away, then run:")
    print(setup_command)
    return 1


def deploy(path, content, variant):
    state = installed_state(path, content)
    if state == "foreign":
        return report_foreign(path)
    if state == "current":
        print(f"The {variant} {hook_name} hook at {path} is up to date.")
        return 0
    if state == "outdated":
        print(f"The {hook_name} hook at {path} differs from the {variant} version. To replace it, run:")
        print(f"{setup_command} -- update {variant}")
        return 1
    write_hook(path, content)
    print(f"Deployed the {variant} {hook_name} hook to {path}.")
    return 0


def update(path, content, variant):
    if installed_state(path, content) != "outdated":
        return deploy(path, content, variant)
    backup = back_up(path)
    write_hook(path, content)
    print(f"Updated the {hook_name} hook at {path} to the {variant} version; the previous one is in {backup}.")
    return 0


def remove(path, content, variant):
    state = installed_state(path, content)
    if state == "foreign":
        return report_foreign(path)
    if state == "absent":
        print(f"There is no {hook_name} hook at {path}; nothing to remove.")
        return 0
    if state == "outdated":
        print(f"The {hook_name} hook at {path} differed from the current {variant} version; it is saved in {back_up(path)}.")
    os.remove(path)
    print(f"Removed the {hook_name} hook from {path}.")
    return 0


actions = {"deploy": deploy, "update": update, "remove": remove}


def parsed_arguments():
    arguments = sys.argv[1:]
    action = arguments.pop(0) if arguments and arguments[0] in actions else "deploy"
    allowed_variant_count = 0 if action == "remove" else 1
    if len(arguments) > allowed_variant_count or any(argument not in variants for argument in arguments):
        sys.exit(usage)
    return actions[action], next(iter(arguments), None)


def main():
    action, requested_variant = parsed_arguments()
    template = read_text(runfiles.Create().Rlocation(os.environ["HOOK"]))
    os.chdir(workspace_directory())
    path = os.path.join(hooks_directory(), hook_name)
    variant = requested_variant or deployed_variant(path) or default_variant
    status = action(path, hook_content(template, variant), variant)
    if status == 0 and action is not remove and shutil.which("bazelisk") is None:
        print("bazelisk is not on PATH; the hook blocks commits until it is installed.")
    return status


if __name__ == "__main__":
    sys.exit(main())
