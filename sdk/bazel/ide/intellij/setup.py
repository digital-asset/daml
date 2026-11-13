# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import setup_ide

PLUGIN = "boo.fox.haskelllsp"
PLUGIN_NAME = "Haskell LSP"
PROJECT_DIR = Path(".idea")
REQUIRED_PLUGINS = PROJECT_DIR / "externalDependencies.xml"
COMPONENT = "ExternalDependencies"
SETUP_COMMAND = "bazelisk run //:setup_intellij"
REQUIRED_PLUGINS_SETTINGS = "Settings | Appearance & Behavior | Required Plugins"


def load(path: Path) -> ET.Element | None:
    if not path.exists():
        return ET.Element("project", version="4")
    try:
        return ET.parse(path).getroot()
    except ET.ParseError:
        return None


def merge(project: ET.Element, remove: bool) -> bool:
    dependencies = project.find(f"component[@name='{COMPONENT}']")
    if dependencies is None:
        if remove:
            return False
        dependencies = ET.SubElement(project, "component", name=COMPONENT)
    required = [plugin for plugin in dependencies.findall("plugin") if plugin.get("id") == PLUGIN]
    if remove:
        for plugin in required:
            dependencies.remove(plugin)
        if len(dependencies) == 0:
            project.remove(dependencies)
        return bool(required)
    if required:
        return False
    ET.SubElement(dependencies, "plugin", id=PLUGIN)
    return True


def render(project: ET.Element) -> str:
    ET.indent(project, "  ")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(project, encoding="unicode") + "\n"


def update(path: Path, remove: bool) -> tuple[bool, list[str]]:
    project = load(path)
    if project is None:
        return False, [f"Skipped {path}: it is not valid XML."]
    if not merge(project, remove):
        state = "does not require" if remove else "already requires"
        return True, [f"{path} {state} {PLUGIN_NAME}; unchanged."]
    if len(project) == 0:
        path.unlink()
        return True, [f"Removed {path}: it only required {PLUGIN_NAME}."]
    details = []
    if path.exists():
        details.append(f"Backed up {path} to {setup_ide.backup(path).name}.")
    setup_ide.write_text(path, render(project))
    change = "no longer" if remove else "now"
    details.append(f"Updated {path}: {PLUGIN_NAME} is {change} a required plugin of this project.")
    return True, details


def print_section(title: str, items: list[str], numbered: bool) -> None:
    print(f"{title}:")
    for index, item in enumerate(items, 1):
        marker = f"{index}." if numbered else "-"
        first, *rest = item.split("\n")
        print(f"  {marker} {first}")
        for line in rest:
            print(f"  {' ' * len(marker)} {line}")


def report(steps: list[str], details: list[str]) -> None:
    if steps:
        print_section("Next steps", steps, numbered=True)
    if steps and details:
        print()
    if details:
        print_section("Details", details, numbered=False)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog=f"{SETUP_COMMAND} --")
    parser.add_argument("--remove", action="store_true", help=f"stop requiring {PLUGIN_NAME} for this project")
    args = parser.parse_args(argv)

    root = setup_ide.workspace()
    if args.remove:
        ok, details = update(root / REQUIRED_PLUGINS, remove=True)
        report([] if ok else [f"Remove {PLUGIN_NAME} in {REQUIRED_PLUGINS_SETTINGS}."], details)
        return 0 if ok else 1

    if not (root / PROJECT_DIR).is_dir():
        print(f"Open {root} in IntelliJ once and let the Bazel plugin import it. Then run:")
        print(f"  {SETUP_COMMAND}")
        print("The Bazel plugin only imports a folder without .idea, so this setup leaves creating .idea to IntelliJ.")
        print(f"That command then makes {PLUGIN_NAME} a required plugin, so IntelliJ offers to install it.")
        return 1

    ok, details = update(root / REQUIRED_PLUGINS, remove=False)
    bazel = setup_ide.record_bazel(root)
    if bazel:
        details.append(f"Editors started outside a shell run Bazel through {bazel}.")
    else:
        details.append("Warning: neither bazelisk nor bazel is on PATH; editors must find one on their own PATH.")
    details.append(
        "The server path setting covers every project; outside this checkout the wrapper hands off to the\n"
        "next haskell-language-server-wrapper on PATH."
    )

    if ok:
        plugin_step = f"If IntelliJ reports {PLUGIN_NAME} as a missing required plugin, install it; LSP4IJ comes with it."
    else:
        plugin_step = f"Add {PLUGIN_NAME} in {REQUIRED_PLUGINS_SETTINGS}."
    steps = [
        f"Reopen {root} in IntelliJ.",
        plugin_step,
        f"Set Settings | Tools | {PLUGIN_NAME} | Haskell Language Server path to:\n  {root / setup_ide.HLS_WRAPPER}",
        "Open a Haskell file. The first start builds the HLS cradle; the Language Servers tool window\n"
        "shows progress.",
    ]
    report(steps, details)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
