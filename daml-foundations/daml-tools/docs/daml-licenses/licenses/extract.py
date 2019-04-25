#!/usr/bin/env runpipenv
# Copyright (c) 2019, Digital Asset (Switzerland) GmbH and/or its affiliates.
# All rights reserved.

#
# This script extracts the licenses of our third-party Haskell dependencies.
#

import pystache
import requests
import re
import subprocess
import sys
import os.path
import codecs
import string
import HTMLParser

# Approved open source licenses
LICENSE_WHITELIST = [ "Apache-2.0", "BSD2", "BSD3", "MIT", "MPL-2.0", "PublicDomain", "ISC", "BSD-3-Clause", "LicenseRef-PublicDomain", "BSD-2-Clause"]

TEMPLATES = [ ('damlc.rst.tmpl', '../source/licensing/damlc.rst'),
              ('licensing.md.tmpl', 'licensing.md') ]

checkingMode = False

# Substitute a license for a package
def substitute_license(package, version):
  # Licenses are not produced for internal packages
  if package.startswith("da-"):
    return None

  # Pull unlicense for public domain project that is public domain but does not have specific license file
  elif package == "control-monad-omega":
     return retrieve_license(package, version, "http://hackage.haskell.org/package/JustParse-2.1/src/LICENSE")

  # erf is BSD3 but do not have a license file as per http://hackage.haskell.org/package/erf -- use BSD3 license from another library from same author where he was better behaved
  elif package == "erf":
      return retrieve_license(package, version, "https://hackage.haskell.org/package/geniplate-0.6.0.5/src/LICENSE")

  # monad-loops  is now public domain and used to be BSD3 -- reverting to prior known license since it no longer includes on
  elif package == "monad-loops":
    return retrieve_license("monad-loops", "0.3.3.0")

  # Forked libraries, under nix/third-party.
  elif package == "grpc-haskell" or package == "grpc-haskell-core":
    return read_license("../../../../../nix/third-party/gRPC-haskell/LICENSE")

  # For the rest, retrieve the license from Hackage.
  else:
    return retrieve_license(package, version)


def package_homepage(name):
    return "http://hackage.haskell.org/package/%s" % name

def create_licenses_pages(licenses):
  for (tmpl, outfile) in TEMPLATES:
    print "Creating %s => %s" % (tmpl, outfile)
    with codecs.open(tmpl, mode='r', encoding='utf-8') as f:
      tmpl = f.read()
      with codecs.open(outfile, mode='w', encoding='utf-8') as of:
          of.write(HTMLParser.HTMLParser().unescape(pystache.render(tmpl, licenses, escape=lambda u: u)))

def code_blockify(text):
  lines = text.split('\n')
  return string.join(map(lambda x: "  " + x, lines), '\n')

def retrieve_license(name, version, url = None):
  cachefile = "cache/%s-%s" % (name, version)
  if os.path.isfile(cachefile):
    return read_license(cachefile)
  else:
    if checkingMode:
        raise Exception("In checking mode, refusing to download license text for %s-%s!  Run extract.py to populate cache and check in new files." % (name, version))

    print "Retrieving license for %s-%s..." % (name, version)

    if url is None:
      filenames = [ 'LICENSE', 'LICENSE.txt', 'LICENSE.md', 'COPYING', 'LICENCE', 'license.txt' ]
      for filename in filenames:
        r = requests.get( "http://hackage.haskell.org/package/%s-%s/src/%s" % (name, version, filename))
        if r.status_code == 200:
          break
    else:
      r = requests.get(url)

    r.raise_for_status()

    with codecs.open(cachefile, mode='w', encoding='utf-8') as f:
      f.write(r.text)

    return code_blockify(r.text)

SKIP_PACKAGE = set(
    [ 'ghc', 'ghc-boot', 'ghc-boot-th', 'ghc-heap', 'ghci', 'libiserv', 'rts', 'ghc-lib'
    ])

# Retrieve a list of packages to produce the licenses for.
def list_packages():
  print "Listing dependencies..."
  output = subprocess.check_output(
    ["ghc-pkg", "field", "*", "name,version,license", "--simple-output"]).strip().splitlines()

  pkgs = []
  for index in range(0, len(output), 3):
    name = output[index]
    version = output[index+1]
    license = output[index+2]

    # skip packages that are covered in GHC license
    if name not in SKIP_PACKAGE:
      pkgs.append({ 'name': name, 'version': version, 'license': license })
  pkgs.sort(key = lambda x: x['name'])
  return pkgs

# Verify the license for a package
def verify_license(package, license, version):
  # Allow internal packages with any license
  if package.startswith("da-"):
    return

  # MonadRandom with the version we use has a BSD-like license, but
  # marked as "OtherLicense".
  # https://hackage.haskell.org/package/MonadRandom-0.4.2.3/src/LICENSE
  # NOTE(JM): We're pinning this exception to a specific version, you'll need
  # to update the version here if MonadRandom is upgraded.
  # if package == "MonadRandom" and license == "OtherLicense" and version == "0.4.2.3":
  #   return

  # The GHC base packages have "<unknown>" license, but are covered by
  # the GHC license.
  if package in ["base", "ghc-prim", "integer-simple", "integer-gmp", "template-haskell"]:
    return

  # Otherwise we check against the whitelist
  if license not in LICENSE_WHITELIST:
    print "Unapproved license on package %s: %s!" % (package, license)
    sys.exit(1)

# Verify licenses against a whitelist
def verify_licenses(packages):
  for pkg in packages:
    verify_license(pkg['name'], pkg['license'], pkg['version'])

def read_utf8_file(filename):
  with codecs.open(filename, mode='r', encoding='utf-8') as f:
    return f.read()
    return code_blockify(f.read())

def read_license(filename):
  return code_blockify(read_utf8_file(filename))

def generate():
  packages = list_packages()
  verify_licenses(packages)

  # Extract the licenses for each package
  licenses = [
      { 'homepage': package_homepage(pkg['name']),
        'name': pkg['name'], 'license': license
      }
      for pkg in packages
      for license in [substitute_license(pkg['name'], pkg['version'])]
    ]

  create_licenses_pages(
      { 'licenses': licenses,
        'da-sdk-license': read_utf8_file('../source/da-sdk-license.rst'),
        'ghc': read_license('GHC-LICENSE'),
        'glibc': read_license('glibc-LICENSE'),
        'openssl': read_license('OpenSSL-LICENSE'),
        'zlib': read_license('zlib-LICENSE'),
        'libc++': read_license('libc++-LICENSE'),
        'libcares': read_license('libcares-LICENSE'),
        'ncurses': read_license('ncurses-LICENSE'),
        'lzma': read_license('lzma-LICENSE'),
        'libsystem': read_license('libsystem-LICENSE')
        }
  )

def to_names(pkgs):
  return map(lambda pkg: "%s-%s" % (pkg['name'], pkg['version']), pkgs)

# Check: Regenerate the pages and check for changes with git.
def check():
  global checkingMode
  checkingMode = True
  generate()
  for (_, outfile) in TEMPLATES:
    if subprocess.call(["git", "diff", "--quiet", outfile]) == 1:
      print "File %s out-of-date, please re-generate the licenses page by running extract.py in daml-licenses/licenses!" % outfile
      sys.exit(1)

##########

if not os.path.isfile('extract.py'):
  print "ERROR: Please run this script in the 'licenses' directory"
  sys.exit(1)

if len(sys.argv) > 1 and sys.argv[1] == 'check':
  check()
else:
  generate()
