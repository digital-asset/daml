#!/usr/bin/env python3
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from subprocess import call, STDOUT
from shutil import copyfile
import sys
import os
import fileinput

ORIGINAL_FIX_VERSION_HS = "gen-source/Version.hs.template"
GENERATED_VERSION_HS = "DA/Sdk/Cli/Version.hs"

ORIGINAL_MOCKSERVER_HS = "gen-source/Mockserver.hs.template"
GENERATED_MOCKSERVER_HS = "Mockserver.hs"

def main(version=None):
  if version is None:
    version = "HEAD"
  print("Stack Builder started...")
  try:
    basedir = os.path.dirname(os.path.realpath(__file__))
    gen_vsn_hs = "%s/%s" % (basedir, GENERATED_VERSION_HS)
    print("Generating %s..." % GENERATED_VERSION_HS)
    copyfile("%s/%s" % (basedir, ORIGINAL_FIX_VERSION_HS), gen_vsn_hs)
    replace_template_var(gen_vsn_hs, "<VERSION-VAR>", version)
    print("Generating %s..." % GENERATED_MOCKSERVER_HS)
    copyfile("%s/%s" % (basedir, ORIGINAL_MOCKSERVER_HS), "%s/%s" % (basedir, GENERATED_MOCKSERVER_HS))

    print("Running stack build...")
    call(["stack", "build"], stderr=sys.stderr, stdout=sys.stdout)
  finally:
    try:
      print("Removing generated files...")
      os.remove(GENERATED_VERSION_HS)
      os.remove(GENERATED_MOCKSERVER_HS)
    except OSError:
      pass

def replace_template_var(template_file, var, value):
  with fileinput.FileInput(template_file, inplace=True, backup='.bak') as file:
      for line in file:
          print(line.replace(var, value), end='')

if __name__== "__main__":
  if len(sys.argv) > 1:
    version = sys.argv[1]
  else:
    version = None
  main(version)