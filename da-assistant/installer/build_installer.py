#!/usr/bin/env python3
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from subprocess import call, check_output, STDOUT
from shutil import copyfile
import sys
import os

sys.path.insert(1, os.path.realpath(os.path.pardir))
import stack_build

INSTALLER_WIX = "SdkAssistantInstaller.wxs"
INSTALLER_WIXOBJ = "SdkAssistantInstaller.wixobj"

class OsNotWIndows(Exception):
  pass

def main():
  if os.name != 'nt':
    raise OsNotWIndows("The operating system is NOT Windows! " + 
                       "The installer can only built on Windows.")
  basedir = os.path.dirname(os.path.realpath(__file__))
  target_exe = "%s\\da.exe" % basedir
  print("Installer Build started...")
  try:
    stack_build.main()

    print("Copying the SDK Assistant executable...")
    output_dir = check_output(["stack", "path", "--local-install-root"]).rstrip().decode("utf-8")
    output_exe = "%s\\%s" % (output_dir, "bin\\da-cli-exe.exe")
    copyfile(output_exe, target_exe)

    print("Running Wix Tools...")
    call(["candle.exe", "-ext", "WixUtilExtension", INSTALLER_WIX], stderr=sys.stderr, stdout=sys.stdout)
    call(["light.exe", "-ext", "WixUtilExtension", INSTALLER_WIXOBJ], stderr=sys.stderr, stdout=sys.stdout)
  except FileNotFoundError as fnf:
    if "candle.exe" in str(fnf) or "light.exe" in str(fnf):
      print("Wix Toolset is NOT installed. Candle.exe/light.exe is not found.")
    else:
      raise fnf
  finally:
    try:
      print("Removing the copied exe...")
      os.remove(target_exe)
    except OSError:
      pass
  
if __name__== "__main__":
  main()