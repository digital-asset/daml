import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import ide_setup

PLUGIN = "boo.fox.haskelllsp"
PLUGIN_NAME = "Haskell LSP"
PROJECT_DIR = Path(".idea")
REQUIRED_PLUGINS = PROJECT_DIR / "externalDependencies.xml"
COMPONENT = "ExternalDependencies"


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


def update(path: Path, remove: bool) -> bool:
    project = load(path)
    if project is None:
        print(f"skipped {path}: it is not valid XML")
        print(f"  {'remove' if remove else 'add'} {PLUGIN_NAME} in Settings | Appearance & Behavior | Required Plugins")
        return False
    if not merge(project, remove):
        print(f"unchanged {path}")
        return True
    if len(project) == 0:
        path.unlink()
        print(f"removed {path}")
        return True
    if path.exists():
        print(f"backed up {path} to {ide_setup.backup(path)}")
    ide_setup.write_text(path, render(project))
    print(f"updated {path}")
    return True


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="bazelisk run //:intellij_setup --")
    parser.add_argument("--remove", action="store_true", help=f"stop requiring {PLUGIN_NAME} for this project")
    args = parser.parse_args(argv)

    root = ide_setup.workspace()
    if args.remove:
        return 0 if update(root / REQUIRED_PLUGINS, remove=True) else 1

    if (root / PROJECT_DIR).is_dir():
        ok = update(root / REQUIRED_PLUGINS, remove=False)
    else:
        print(f"skipped {root / REQUIRED_PLUGINS}: open {root} in IntelliJ once, then run this again")
        ok = False

    wrapper = root / ide_setup.HLS_WRAPPER
    print()
    ide_setup.announce(root, ide_setup.record_bazel(root))
    print(f"Reopen {root} in IntelliJ and install {PLUGIN_NAME} when it reports the missing required plugin;")
    print("LSP4IJ is installed along with it.")
    print(f"Then set Settings | Tools | {PLUGIN_NAME} | Haskell Language Server path to:")
    print(f"  {wrapper}")
    print("That setting covers every project; outside this checkout the wrapper hands off to the next")
    print("haskell-language-server-wrapper on PATH.")
    print("The first start builds the HLS cradle; progress shows in the Language Servers tool window.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
