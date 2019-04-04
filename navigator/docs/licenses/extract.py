#
# This script implements common functions for license extraction scripts
#

import pystache
import requests
import sys
import os.path
import codecs
import string
import textwrap
import HTMLParser

CACHE_DIR = 'cache'

def code_blockify(text):
  lines = string.join(map(textwrap.fill, text.split('\n')), '\n').split('\n')
  return string.join(map(lambda x: "  " + x, lines), '\n')

def read_utf8_file(filename):
  with codecs.open(filename, mode='r', encoding='utf-8') as f:
    return f.read()
    return code_blockify(f.read())

def read_license(filename):
  return code_blockify(read_utf8_file(filename))

def retrieve_url(prefix, name, url):
  cachefile = "%s/%s-%s" % (CACHE_DIR, prefix, name)
  if os.path.isfile(cachefile):
    return read_license(cachefile)
  else:
    print "Retrieving license for %s..." % name
    r = requests.get(url)
    r.raise_for_status()
    with codecs.open(cachefile, mode='w', encoding='utf-8') as f:
      f.write(r.text)
    return code_blockify(r.text)

def group_licenses(licenses):
  # Group by group and license text
  groups = {}
  for license in licenses:
    key = u"%s:%s" % (license['key'], license['license'])
    if groups.has_key(key):
      groups[key]['names'].append(license['name'])
      groups[key]['homepages'].append(license['homepage'])
      groups[key]['headerNames'].append(license['headerName'])
    else:
      groups[key] = {
        'names': [license['name']],
        'headerNames': [license['headerName']],
        'homepages': [license['homepage']],
        'license': license['license']
      }
  for key in groups:
    groups[key]['headerName'] = ', '.join(sorted(list(set(groups[key]['headerNames']))))

  # Convert back to list
  return [
    {
      'names': '\n'.join([
        "* `%s <%s>`_" % x for x in zip(info['names'], info['homepages'])
        ]),
      'license': info['license'],
      'headerName': info['headerName'],
      'headerLine': '^' * len(info['headerName'])
    }
    for (key, info) in sorted(groups.iteritems(), key=lambda x: x[1]['headerName'])
  ]

def create_licenses_pages(module, label, licenses, templates):
  data = {
    'module': module,
    'label': label,
    'licenses': group_licenses(licenses)
  }
  for (tmpl, outfile) in templates:
    print "Creating %s => %s" % (tmpl, outfile)
    with codecs.open(tmpl, mode='r', encoding='utf-8') as f:
      tmpl = f.read()
      with codecs.open(outfile, mode='w', encoding='utf-8') as of:
        of.write(HTMLParser.HTMLParser().unescape(pystache.render(tmpl, data, escape=lambda u: u)))
