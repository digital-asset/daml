#!/usr/bin/env runpipenv
# coding: utf8

#
# This script extracts the licenses of the dependencies of Navigator backend
#

import re
import subprocess
import sys
import os.path
import csv

import extract
import licensedata

# Approved open source licenses
# License category reported by the sbt plugin is very unreliable
# Licenses are therefore whitelisted by their URL
LICENSE_WHITELIST = [
  # Apache 2
  "http://www.apache.org/licenses/LICENSE-2.0.txt",
  "http://www.apache.org/licenses/LICENSE-2.0.html",
  "http://www.apache.org/licenses/LICENSE-2.0",
  "https://opensource.org/licenses/Apache-2.0",
  "http://www.opensource.org/licenses/Apache-2.0",

  # BSD
  "http://asm.objectweb.org/license.html",
  "http://www.scala-lang.org/license.html",
  "http://opensource.org/licenses/BSD-3-Clause",
  "https://opensource.org/licenses/BSD-3-Clause",
  "http://www.opensource.org/licenses/bsd-license.php",
  "https://github.com/scodec/scodec/blob/master/LICENSE",
  "https://github.com/scodec/scodec-bits/blob/master/LICENSE",
  # CC0
  "http://creativecommons.org/publicdomain/zero/1.0/",
  # CDDL
  "http://www.opensource.org/licenses/cddl1.php",
  "http://glassfish.java.net/public/CDDL+GPL_1_1.html", # dual licensed CDDL/GPL
  "https://oss.oracle.com/licenses/CDDL+GPL-1.1",
  "http://glassfish.dev.java.net/nonav/public/CDDL+GPL.html",
  "https://glassfish.java.net/nonav/public/CDDL+GPL_1_1.html",

  # MIT
  "http://www.opensource.org/licenses/mit-license.php",
  "http://www.opensource.org/licenses/mit-license.html",
  "http://www.opensource.org/licenses/mit-license",
  "http://www.opensource.org/licenses/mit-license.php",
  "http://opensource.org/licenses/MIT",
  "https://projectlombok.org/LICENSE",
  # MPL 2
  "https://www.mozilla.org/MPL/2.0/",
  #Mozilla 1.1
  "http://www.mozilla.org/MPL/MPL-1.1.html",
  # JSON
  "http://json.org/license.html", # TODO: Check if ok (morality clause)
  #GPL2 classpath exception
  "http://openjdk.java.net/legal/gplv2+ce.html",
  #Eclipse
  "http://www.eclipse.org/legal/epl-v10.html",
  "http://www.eclipse.org/legal/epl-v20.html",
  "LICENSE.txt",
  "http://www.bouncycastle.org/licence.html",
  "http://github.com/mockito/mockito/blob/master/LICENSE",
  "http://www.slf4j.org/license.html",
  #ASM
  "http://asm.objectweb.org/license.html"
]

TEMPLATES = [ ('third-party-licenses.rst.tmpl', '../source/backend-licenses.rst') ]

PROJECT_PATH = '../../backend/'

# Substitute a license for a package
def substitute_license(name, info):

  if name.startswith("com.digitalasset"):
    # Licenses are not produced for internal packages
    return None

  elif name.startswith("com.daml"):
    # Licenses are not produced for internal packages
    return None

  elif info['licenseUrl'] in licensedata.SBT.keys():
    # Hardcoded license data for projects that don't store the license
    # at a default location.
    return licensedata.SBT[info['licenseUrl']]

  elif name.startswith("ch.qos.logback"):
      return extract.retrieve_url('sbt', name.replace('/', '_'), "https://raw.githubusercontent.com/qos-ch/logback/master/LICENSE.txt")

  elif name.startswith("org.scala-sbt # test-interface"):
      return extract.retrieve_url('sbt', name.replace('/', '_'), "https://raw.githubusercontent.com/sbt/test-interface/master/LICENSE")

  elif name.startswith("org.checkerframework # checker"):
      return extract.retrieve_url('sbt', name.replace('/', '_'), "https://raw.githubusercontent.com/reprogrammer/checker-framework/master/LICENSE.txt")

  elif name.startswith("org.pcollections # pcollections"):
      return extract.retrieve_url('sbt', name.replace('/', '_'), "https://raw.githubusercontent.com/hrldcpr/pcollections/master/LICENSE")

  elif name.startswith("org.scalacheck # scalacheck"):
      return extract.retrieve_url('sbt', name.replace('/', '_'), "https://raw.githubusercontent.com/rickynils/scalacheck/master/LICENSE")

  else:
    # For the rest, retrieve the license from the URL
    if 'licenseUrl' in info:
      return extract.retrieve_url('sbt', name.replace(' # ', '_'), info['licenseUrl'])
    else:
      print "license URL missing for %s!" % name
      print "  license info is %s" % info['license']
      sys.exit(1)

# Retrieve the list of packages used in the extension
def list_packages():
  print "Retrieving licenses..."
  if not os.path.exists('cache/reports'):
      os.makedirs('cache/reports')
  oldcwd = os.getcwd()

  os.chdir(PROJECT_PATH)
  filename = oldcwd+"/cache/reports/sbt.csv"
  output = subprocess.check_output(["make", "report-licenses", "OUT="+filename])
  os.chdir(oldcwd)
  try:
    return read_csv_file(filename)
  except Exception as e:
    print "ERROR: %s" % e
    print "make report-licenses output was:"
    print output
    sys.exit(1)

def verify_licenses(packages):
  packagesUrls = [packages[k]['licenseUrl'] for k in packages.keys()]
  for package in licensedata.SBT.keys():
    if package not in packagesUrls:
      print "Warning: hardcoded info for %s not used" % package

  for package, info in packages.iteritems():
    if package.startswith("com.digitalasset") or package.startswith("com.daml"):
      continue

    licenseUrl = info['licenseUrl']

    if licenseUrl not in LICENSE_WHITELIST:
      print "Unapproved license on package %s: %s!" % (package, licenseUrl)
      sys.exit(1)

def read_csv_file(filename):
  with open(filename, 'rb') as csvfile:
    reader = csv.reader(csvfile, quoting=csv.QUOTE_MINIMAL, strict=True, delimiter=",")
    reader.next()
    l = list(reader)
    return {row[2]: convert_row(row) for row in l}

def convert_row(row):
  name = row[2]
  licenseMatch = re.search('["]?(.*)[(](.*)[)]', row[1])
  if licenseMatch:
    licenseUrl = licenseMatch.group(2)
    licenseName = licenseMatch.group(1).strip()
  nameMatch = re.search('(.*) # (.*) # (.*)', row[2])
  if nameMatch:
    namePath = nameMatch.group(1).strip()
    nameLib = nameMatch.group(2).strip()
    nameVersion = nameMatch.group(3).strip()
  else:
    print "ERROR: Could not parse library name %s" % row[2]
    sys.exit(1)
  return {
    'category': row[0],
    'licenseUrl': licenseUrl,
    'license': licenseName,
    'nameLib': nameLib,
    'namePath': namePath,
    'nameVersion': nameVersion
  }

def get_homepage(name, info):
  return "https://mvnrepository.com/artifact/%s" % name.replace(' # ', "/")

def get_name(name, info):
  return "%s/%s@%s" % (info['namePath'], info['nameLib'], info['nameVersion'])

# Licenses are only grouped if they have the same key
def get_key(name, info):
  return ''

def get_header_name(name, info):
  return info['namePath']

def generate():
  packages = list_packages()
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

  extract.create_licenses_pages('Navigator backend', 'navigator_backend_licenses', licenses, TEMPLATES)

# Check: Regenerate the pages and check for changes with git.
def check():
  generate()
  for (_, outfile) in TEMPLATES:
    if subprocess.call(["git", "diff", "--quiet", outfile]) == 1:
      print "File %s out-of-date, please re-generate the licenses page by running extract-sbt.py in licenses/licenses!" % outfile
      sys.exit(1)

##########

if not os.path.isfile('extract-js.py'):
  print "ERROR: Please run this script in the 'licenses' directory"
  sys.exit(1)


if len(sys.argv) > 1 and sys.argv[1] == 'check':
  check()
else:
  generate()
