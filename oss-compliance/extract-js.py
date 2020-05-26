#!/usr/bin/env runpipenv
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# coding: utf8

#
# This script extracts the licenses of the dependencies of an NODE.JS project
#

import re
import subprocess
import sys
import os.path
import json

import extract
import licensedata
import argparse

# Approved open source licenses
# TODO: Use something like https://pypi.python.org/pypi/license-expression/0.93
# and only specify literals in this list
LICENSE_WHITELIST = [
    "BSD", "BSD*", "MIT", "MIT*", "Apache-2.0", "Unlicense",
    "(MIT AND CC-BY-3.0)", "ISC", "BSD-3-Clause OR MIT",
    "BSD-4-Clause", "BSD-3-Clause", "BSD-2-Clause", "MIT/X11",
    "Public Domain", "WTFPL", "(MIT OR GPL-2.0)", "Apache*",
    "CC-BY-3.0","CC-BY-4.0", "BSD-like", "(MIT OR Apache-2.0)",
    "CC0-1.0","(BSD-2-Clause OR MIT OR Apache-2.0)","(MIT AND BSD-3-Clause)",
    "(MIT AND Zlib)"]

# Substitute a license for a package
def substitute_license(name, info):

    if name.startswith("@da/"):
        # Licenses are not produced for internal packages
        return None

    elif name.startswith("da_theme"):
        # Licenses are not produced for internal packages
        return None

    elif name in licensedata.NPM.keys():
        # Hardcoded license data for projects that don't store the license
        # at a default location.
        return licensedata.NPM[name]['text']

    elif name.startswith("@types/"):
        # DefinitelyTyped project is under an MIT license
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/DefinitelyTyped/DefinitelyTyped/master/LICENSE")

    elif name.startswith("dom-helpers"):
        # dom-helpers project is under an MIT license but need to get from root package
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/react-bootstrap/dom-helpers/master/LICENSE")

    elif name.startswith("get-caller-file"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/stefanpenner/get-caller-file/master/LICENSE.md")

    elif name.startswith("invert-kv"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/sindresorhus/invert-kv/master/license")

    elif name.startswith("isarray"):
        return extract.read_license(info['path'] + "/" + "README.md")

    elif name.startswith("popper.js"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/FezVrasta/popper.js/master/docs/LICENSE.txt")

    elif name.startswith("requirejs"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/requirejs/requirejs/master/LICENSE")

    elif name.startswith("ansi-regex"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/chalk/ansi-regex/master/license")

    elif name.startswith("ansi-styles"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/chalk/ansi-styles/master/licensex")

    elif name.startswith("async-each"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/paulmillr/mit/master/README.md")

    elif name.startswith("browserify-cache"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/browserify/browserify/master/LICENSE")

    elif name.startswith("eventemitter"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/EventEmitter2/EventEmitter2/master/LICENSE.txt")

    elif name.startswith("grunt-broserify"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/jmreidy/grunt-browserify/master/LICENSE-MIT")

    elif name.startswith("grunt-cli"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/gruntjs/grunt-cli/master/LICENSE-MIT")

    elif name.startswith("grunt-contrib-clean"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/gruntjs/grunt-contrib-clean/master/LICENSE-MIT")

    elif name.startswith("grunt-contrib-connect"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/gruntjs/grunt-contrib-connect/master/LICENSE-MIT")

    elif name.startswith("grunt-contrib-copy"):
        return extract.retrieve_url('node', name.replace('/', '_'), "https://raw.githubusercontent.com/gruntjs/grunt-contrib-copy/master/LICENSE-MIT")


    else:
        # For the rest, retrieve the license using the licenseFile
        filenames = [ 'LICENSE', 'LICENSE.txt', 'LICENSE-MIT.txt',
                      'LICENSE-MIT', 'LICENSE.md', 'COPYING', 'COPYING.txt', 'LICENCE',
                      'license.txt', 'license', 'README.markdown', 'LICENSE.BSD' ]
        for filename in filenames:
            path = info['path'] + '/' + filename
            if os.path.isfile(path):
                return extract.read_license(path)

        print "license file missing for %s!" % name
        if 'licenseFile' in info: print "  license info might be available in %s" % info['licenseFile']
        sys.exit(1)


# Retrieve the list of packages used in the extension
def list_packages(project_path, extract_path):
    print "Retrieving licenses..."
    if not os.path.exists('cache/reports'):
        os.makedirs('cache/reports')

    oldcwd = os.getcwd()
    os.chdir(project_path)
    filename = oldcwd+extract_path
    output = subprocess.check_output(["make", "report-licenses", "OUT="+filename])
    os.chdir(oldcwd)
    try:
        return json.loads(extract.read_utf8_file(filename))
    except Exception as e:
        print "ERROR: %s" % e
        print "report-licenses output was:"
        print output
        sys.exit(1)


def verify_licenses(packages):
    for package in licensedata.NPM.keys():
        if package not in packages.keys():
            print "Warning: hardcoded info for %s not used" % package

    for package, info in packages.iteritems():
        if package.startswith("@da/") or package.startswith("da_theme"):
            continue

        elif package in licensedata.NPM.keys() is not None:
            return licensedata.NPM[package]['text']

        if isinstance(info['licenses'], list):
            licenses = info['licenses']
        else:
            licenses = [info['licenses']]

        if all([x not in LICENSE_WHITELIST for x in licenses]):
            print "Unapproved license on package %s: %s!" % (package, licenses)
            sys.exit(1)


def get_homepage(name, info):
    if 'repository' in info:
        return info['repository']
    elif 'email' in info:
        return info['email']
    elif 'publisher' in info:
        return info['publisher']
    else:
        return ""


def get_name(name, info):
    return name


def get_key(name, info):
    match = re.search('((?P<group>.*)/)?(?P<name>.*)@(?P<version>.*)', name)
    nameGroup = match.group('group')
    nameName = match.group('name')
    nameVersion = match.group('version')
    if nameGroup is not None:
        return nameGroup
    else:
        return ""


def get_header_name(name, info):
    match = re.search('((?P<group>.*)/)?(?P<name>.*)@(?P<version>.*)', name)
    nameGroup = match.group('group')
    nameName = match.group('name')
    nameVersion = match.group('version')
    if nameGroup is not None:
        return nameGroup
    else:
        return nameName


def generate(args):
    component=args.component
    anchor_name=args.anchor_name
    project_path=args.project_path
    extract_path=args.extract_path
    packages = list_packages(project_path, extract_path)

    verify_licenses(packages)

    # Extract the licenses for each package

    licenses = [
        { 'homepage': get_homepage(name, info),
          'name': get_name(name, info),
          'key': get_key(name, info),
          'headerName': get_header_name(name, info),
          'license': license
          }
        for (name, info) in sorted(packages.iteritems(), key=lambda x: x[0])
        for license in [substitute_license(name, info)] if license is not None
    ]

    extract.create_licenses_pages(component, anchor_name, licenses, TEMPLATES)


# Check: Regenerate the pages and check for changes with git.
def check(args):
    generate(args)
    for (_, outfile) in TEMPLATES:
        if subprocess.call(["git", "diff", "--quiet", outfile]) == 1:
            print "File %s out-of-date, please re-generate the licenses page by running extract-js.py in licenses/licenses!" % outfile
            sys.exit(1)

##########

if not os.path.isfile('extract-js.py'):
    print "ERROR: Please run this script in the 'licenses' directory"
    sys.exit(1)


def args_parsing():
    parser = argparse.ArgumentParser(description='Extract and validate license for 3rd party libraries')
    parser.add_argument('--component', dest='component', required=True,
                        help='Component name')
    parser.add_argument('--anchor-name', dest='anchor_name', required=True,
                        help='Name of anchor to component license page')
    parser.add_argument('--project-path', dest='project_path', required=True,
                        help='Relative path to project to scan for licenses')
    parser.add_argument('--extract-path', dest='extract_path', required=True,
                        help='Path to extracted license file')

    return parser.parse_args(sys.argv[1:])


args=args_parsing()
TEMPLATES = [('third-party-licenses.rst.tmpl', '../source/frontend-licenses.rst')]

if len(sys.argv) > 1 and sys.argv[1] == 'check':
    check(args)
else:
    generate(args)
