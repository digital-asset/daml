#!/usr/bin/env runpipenv
# coding: utf8
#
# This script extracts the licenses of the dependencies of the DAML Studio
# extension
#

import pystache
import requests
import re
import subprocess
import sys
import os.path
import codecs
import string
import json
import textwrap
import HTMLParser

# Approved open source licenses
LICENSE_WHITELIST = [
  "BSD", "MIT", "MIT*", "Apache-2.0", "Unlicense",
  "(MIT AND CC-BY-3.0)", "ISC", "BSD-3-Clause OR MIT",
  "BSD-4-Clause", "BSD-3-Clause", "BSD-2-Clause", "MIT/X11",
  "Public Domain", "WTFPL", "(MIT OR GPL-2.0)", "Apache*" ]

TEMPLATES = [ ('extension-licenses.rst.tmpl', '../source/licensing/extension-licenses.rst') ]

# Substitute a license for a package
def substitute_license(name, info):

  # Licenses are not produced for internal packages
  if name.startswith("da-"):
    return None

  elif name == "@types/node@6.0.64":
    # DefinitelyTyped project is under an MIT license
    return retrieve_url(name, "https://raw.githubusercontent.com/DefinitelyTyped/DefinitelyTyped/master/LICENSE")

  elif name == "diff@1.4.0":
    # License is behind an URL, which is declared in the package.json file.
    return retrieve_url(name, "https://raw.githubusercontent.com/kpdecker/jsdiff/v1.4.0/LICENSE")

  elif name == "filename-regex@2.0.0":
    # License is behind an URL, which is declared in the package.json file.
    return retrieve_url(name, "https://raw.githubusercontent.com/regexhq/filename-regex/2.0.0/LICENSE")

  elif name == "assert-plus@0.2.0":
    return code_blockify(u'''
The MIT License (MIT)
Copyright (c) 2012 Mark Cavage

Permission is hereby granted, free of charge, to any person obtaining a copy
of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of
the Software, and to permit persons to whom the Software is furnished to do
so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE
SOFTWARE. ''')

  elif name == "debug@2.2.0":
    return code_blockify(u'''
(The MIT License)

Copyright (c) 2014 TJ Holowaychuk &lt;tj@vision-media.ca&gt;

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
''')

  elif name == "growl@1.9.2":
    # Has its license in the readme, so just reproduce it here.
    return code_blockify(u'''
(The MIT License)

Copyright (c) 2009 TJ Holowaychuk <tj@vision-media.ca>
Copyright (c) 2016 Joshua Boy Nicolai Appelman <joshua@jbna.nl>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
    'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
''')

  elif name == "queue@3.1.0":
    # License in readme, reproduce it here.
    return code_blockify(u'''
Copyright © 2014 Jesse Tane <jesse.tane@gmail.com>

This work is free. You can redistribute it and/or modify it under the
terms of the [WTFPL](http://www.wtfpl.net/txt/copying).

No Warranty. The Software is provided "as is" without warranty of any kind,
either express or implied, including without limitation any implied warranties
of condition, uninterrupted use, merchantability, fitness for a particular
purpose, or non-infringement.''')

  elif name == "isarray@1.0.0":
    # License in readme, reproduce it here.
    return code_blockify(u'''
(MIT)

Copyright (c) 2013 Julian Gruber &lt;julian@juliangruber.com&gt;

Permission is hereby granted, free of charge, to any person obtaining a copy
of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE
SOFTWARE.
''')

  elif name == "assert-plus@1.0.0":
    return code_blockify(u'''The MIT License (MIT)
Copyright (c) 2012 Mark Cavage

Permission is hereby granted, free of charge, to any person obtaining a copy
of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of
the Software, and to permit persons to whom the Software is furnished to do
so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE
SOFTWARE.
''');

  elif name == "jsonify@0.0.0":
    return "public domain (https://github.com/substack/jsonify)"

  elif name == "json-schema@0.2.3":
    # Same license in newer version, but this old version doesn't
    # have a git tag.
    retrieve_url(name, "https://raw.githubusercontent.com/dojo/dojo/1.12.1/LICENSE")

  elif name == "node.extend@1.1.6":
    return code_blockify('''Copyright 2011, John Resig
Dual licensed under the MIT or GPL Version 2 licenses.
http://jquery.org/license)
''')

  elif name == "commander@0.6.1" or name == "commander@2.3.0":
    # License in readme, reproduce it here.
    return code_blockify('''
(The MIT License)

Copyright (c) 2011 TJ Holowaychuk &lt;tj@vision-media.ca&gt;

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.''');

  else:
    # For the rest, retrieve the license using the licenseFile
    if 'licenseFile' not in info:
      print "license file missing for %s!" % name
      sys.exit(1)
    else:
      return read_license(info['licenseFile'])

def create_licenses_pages(licenses):
  for (tmpl, outfile) in TEMPLATES:
    print "Creating %s => %s" % (tmpl, outfile)
    with codecs.open(tmpl, mode='r', encoding='utf-8') as f:
      tmpl = f.read()
      with codecs.open(outfile, mode='w', encoding='utf-8') as of:
        of.write(HTMLParser.HTMLParser().unescape(pystache.render(tmpl, licenses, escape=lambda u: u)))

def code_blockify(text):
  lines = string.join(map(textwrap.fill, text.split('\n')), '\n').split('\n')
  return string.join(map(lambda x: "  " + x, lines), '\n')

def retrieve_url(name, url):
  name = name.replace('/', '_')
  cachefile = "cache/node-%s" % name
  if os.path.isfile(cachefile):
    return read_license(cachefile)
  else:
    print "Retrieving license for %s..." % name
    r = requests.get(url)
    r.raise_for_status()
    with codecs.open(cachefile, mode='w', encoding='utf-8') as f:
      f.write(r.text)
    return code_blockify(r.text)

# Retrieve the list of packages used in the extension
def list_packages():
  print "Retrieving licenses..."
  oldcwd = os.getcwd()
  os.chdir(EXT_PATH)
  output = subprocess.check_output(["license-checker", "--production", "--json"])
  os.chdir(oldcwd)

  try:
    return json.loads(output)
  except Exception as e:
    print "ERROR: %s" % e
    print "license-checker output was:"
    print output
    sys.exit(1)


def verify_licenses(packages):
  for package, info in packages.iteritems():
    if package.startswith("da-"):
      continue

    elif package == "tweetnacl@0.14.3":
      # license text says SEE LICENSE IN COPYING.txt, which
      # has public domain license.
      continue

    # Dominic Tarr's packages are MIT licensed, but marked "UNKNOWN"
    elif package == "map-stream@0.1.0" or package == "split@0.2.10" \
        or package == "event-stream@3.1.7":
      continue

    if isinstance(info['licenses'], list):
      licenses = info['licenses']
    else:
      licenses = [info['licenses']]

    print "license on package %s, %s!" % (package, licenses)

    if all([x not in LICENSE_WHITELIST for x in licenses]):
      print "Unapproved license on package %s %s!" % (package, licenses)
      sys.exit(1)

def read_utf8_file(filename):
  with codecs.open(filename, mode='r', encoding='utf-8') as f:
    return f.read()
    return code_blockify(f.read())

def read_license(filename):
  return code_blockify(read_utf8_file(filename))

def get_homepage(info):
  if 'repository' in info:
    return info['repository']
  elif 'email' in info:
    return info['email']
  elif 'publisher' in info:
    return info['publisher']
  else:
    return ""


def generate():
  packages = list_packages()
  verify_licenses(packages)

  # Extract the licenses for each package

  licenses = [
      { 'homepage': get_homepage(info),
        'name': name, 'license': license
      }
      for (name, info) in sorted(packages.iteritems(), key=lambda x: x[0])
      for license in [substitute_license(name, info)] if license is not None
    ]

  create_licenses_pages(
      { 'licenses': licenses
      })

def to_names(pkgs):
  return map(lambda pkg: "%s-%s" % (pkg['name'], pkg['version']), pkgs)

# Check: Regenerate the pages and check for changes with git.
def check():
  generate()
  for (_, outfile) in TEMPLATES:
    if subprocess.call(["git", "diff", "--quiet", outfile]) == 1:
      print "File %s out-of-date, please re-generate the licenses page by running extract-js.py in daml-licenses/licenses!" % outfile
      sys.exit(1)

##########

if not os.path.isfile('extract-js.py'):
  print "ERROR: Please run this script in the 'licenses' directory"
  sys.exit(1)


if len(sys.argv) > 1 and sys.argv[1] == 'check':
  check()
else:
  generate()
