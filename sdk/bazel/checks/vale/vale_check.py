# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
import shlex
import subprocess
import sys
import tempfile

from python.runfiles import runfiles

vale_config = ".vale-ci.ini"
documentation_patterns = ["docs/sharable/*.rst"]
min_alert_level = "warning"
git = os.environ.get("GIT", "git")
exit_code_ok = 0
exit_code_errors_found = 1


def workspace_directory():
    if "BUILD_WORKSPACE_DIRECTORY" in os.environ:
        return os.environ["BUILD_WORKSPACE_DIRECTORY"]
    module_file = runfiles.Create().Rlocation("_main/MODULE.bazel")
    return os.path.dirname(os.path.realpath(module_file))


def tracked_documentation_files():
    result = subprocess.run(
        [git, "ls-files", "-z", "--", *documentation_patterns],
        capture_output=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        sys.exit(f"git ls-files failed:\n{result.stderr}")
    return [path for path in result.stdout.split("\0") if path]


def write_rst2html_tools(directory, files):
    rst2html = files.Rlocation(os.environ["RST2HTML"])
    wrapper = os.path.join(directory, "rst2html")
    with open(wrapper, "w", encoding="utf-8") as f:
        f.write(f"#!/bin/sh\nexec {shlex.quote(rst2html)} \"$@\"\n")
    os.chmod(wrapper, 0o755)
    os.symlink(sys.executable, os.path.join(directory, "python3"))


def run_vale(paths):
    files = runfiles.Create()
    vale = files.Rlocation(os.environ["VALE"])
    with tempfile.TemporaryDirectory(prefix="vale-tools-") as tools:
        write_rst2html_tools(tools, files)
        result = subprocess.run(
            [vale, f"--config={vale_config}", f"--minAlertLevel={min_alert_level}", "--output=JSON", *paths],
            capture_output=True,
            encoding="utf-8",
            env={**os.environ, **files.EnvVars(), "PATH": tools + os.pathsep + os.environ.get("PATH", "")},
        )
    if result.returncode not in (exit_code_ok, exit_code_errors_found):
        sys.exit(f"vale failed:\n{result.stdout}{result.stderr}")
    try:
        return json.loads(result.stdout), result.returncode == exit_code_errors_found
    except json.JSONDecodeError:
        sys.exit(f"vale returned unexpected output:\n{result.stdout}{result.stderr}")


def describe(path, alert):
    return f'  {path}:{alert["Line"]}:{alert["Span"][0]}: {alert["Severity"]}: {alert["Message"]} ({alert["Check"]})'


def main():
    if sys.argv[1:2] != ["check"]:
        sys.exit("usage: vale check [PATH...]")

    os.chdir(workspace_directory())
    paths = sys.argv[2:] or tracked_documentation_files()
    if not paths:
        print("No documentation files to check.")
        return 0
    missing = [path for path in paths if not os.path.exists(path)]
    if missing:
        sys.exit("No such files: " + ", ".join(missing))

    alerts_by_path, errors_found = run_vale(paths)
    alerts = [(path, alert) for path, path_alerts in sorted(alerts_by_path.items()) for alert in path_alerts]

    if not alerts:
        print(f"No vale alerts in {len(paths)} checked files.")
        return 0

    if errors_found:
        print(f"vale check failed: {len(alerts)} alerts in {len(alerts_by_path)} of {len(paths)} checked files.")
    else:
        print(f"vale found {len(alerts)} non-blocking alerts in {len(alerts_by_path)} of {len(paths)} checked files.")
    print()
    print(f"Alerts ({len(alerts)}):")
    for path, alert in alerts:
        print(describe(path, alert))
    print()
    if errors_found:
        print(f"Fix them by hand, or adjust the rules in {vale_config}.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
