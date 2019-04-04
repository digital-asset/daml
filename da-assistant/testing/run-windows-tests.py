#!/usr/bin/env python3
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from subprocess import call, check_output, STDOUT
from shutil import copyfile, copytree
import sys
import os
import tempfile
import time

sys.path.insert(1, os.path.realpath(os.path.pardir))
import stack_build

INSTALLER_WIX = "Product.wxs"
INSTALLER_WIXOBJ = "Product.wixobj"

class OsNotWIndows(Exception):
  pass

class SetupDirExists(Exception):
  pass

def main(email, username, password):
  if os.name != 'nt':
    raise OsNotWIndows("The operating system is NOT Windows! " + 
                       "The installer can only built on Windows.")
  localappdata = os.environ["localappdata"]
  setupdir = "%s\\damlsdk" % localappdata
  if os.path.isdir(setupdir):
    raise SetupDirExists(("Setup directory already exists: %s " + 
                         "- To run the test, one needs to uninstall/move it.") % setupdir)
  basedir = os.path.dirname(os.path.realpath(__file__))
  print("Installer Build started...")
  try:
    stack_build.main("777-aaa")
    output_dir = check_output(["stack", "path", "--local-install-root"]).rstrip().decode("utf-8")
    output_exe = "%s\\%s" % (output_dir, "bin\\da-cli-exe.exe")
    
    with tempfile.TemporaryDirectory() as td:
        cwd = os.getcwd()
        os.chdir(td)

        print("Temporary directory for testing: %s" % td)
        cli_dir = "%s\\cli" % setupdir
        target_exe = "%s\\da.exe" % cli_dir
        os.environ["PATH"] = ("%s;%s;%s" % (cli_dir, td, os.environ["PATH"]))
        print("Copying tests...")
        copytree("%s\\tests-windows" % basedir, "%s\\tests-windows" % td)

        print("Creating an input file for setup...")
        with open("%s\\input_file.txt" % td, "w", newline='\n') as inf:
            inf.write("%s\n" % email)
            inf.write("%s\n" % username)
            inf.write("%s\n" % password)

        print("Running DA Assistant tests...")
        for fn in os.listdir("tests-windows"):
          print("Test: %s" % fn)
          print("Copying the SDK Assistant executable...")
          if not os.path.isfile(target_exe):
            os.makedirs(cli_dir)
            copyfile(output_exe, target_exe)
          call(["shelltest.exe", "tests-windows\\%s" % fn, "-p", "-c", "-d", "-a"], stderr=sys.stderr, stdout=sys.stdout)
        # If we are in tempdir, it cannot be deleted, we need to cd back.
        os.chdir(cwd)
  except FileNotFoundError as fnf:
    if "shelltest.exe" in str(fnf):
      print("Shell Test Runner is NOT installed. Shelltest.exe is not found.")
    else:
      raise fnf
  finally:
    pass
  
INPUT_FILE = "test_creds"
if __name__== "__main__":
  if os.path.isfile(INPUT_FILE):
    with open(INPUT_FILE, "r") as f:
      [email, username, password] = f.read().splitlines()
  else:
    email = input("Your e-mail?")
    username = input("Your Bintray username?")
    password = input("Your Bintray password?")
  main(email, username, password)