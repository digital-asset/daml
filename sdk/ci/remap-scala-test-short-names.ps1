# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if (Test-Path sdk) {
  cd sdk
}

# $ErrorActionPreference = 'Stop' causes the script to fail because Bazel writes to stderr.
$ErrorActionPreference = 'Continue'

[string[]]$scala_test_targets = bazel.exe query "kind(scala_test, deps(kind(test_suite, //...), 1))"
if ($lastexitcode -ne 0) {
  throw "bazel query returned non-zero exit code: $lastexitcode"
}

if ($scala_test_targets.count -gt 0) {

  try {

    # Bazel writes the output of --experimental_show_artifacts to stderr
    # instead of stdout. In Powershell this means that these outputs are not
    # plain strings, but instead error objects. Simply redirecting these to
    # stdout and piping them into further processing will lead to
    # indeterministically missing items or indeterministically introduced
    # additional newlines which may break paths.
    #
    # To work around this we extract the error message from error objects,
    # introduce appropriate newlines, and write the output to a temporary file
    # before further processing.
    #
    # This solution is taken and adapted from
    # https://stackoverflow.com/a/48671797/841562
    $bazelexitcode = 0
    $tmp = New-TemporaryFile
    try {
      $append = $false
      $out = [System.IO.StreamWriter]::new($tmp, $append)
      bazel.exe build `
        "--aspects=//bazel_tools:scala.bzl%da_scala_test_short_name_aspect" `
        "--output_groups=scala_test_info" `
        "--experimental_show_artifacts" `
        @scala_test_targets `
        2>&1 | % {
          if ($_ -is [System.Management.Automation.ErrorRecord]) {
            if ($_.TargetObject -ne $null) {
              $out.WriteLine();
            }
            $out.Write($_.Exception.Message)
          } else {
            $out.WriteLine($_)
          }
        }
      $bazelexitcode = $lastexitcode
    } finally {
      $out.Close()
    }

    if ($bazelexitcode -ne 0) {
      $errmsg = Get-Content $tmp
      Write-Error -Message "$errmsg"
      throw "bazel build returned non-zero exit code: $lastexitcode"
    }

    Get-Content $tmp |
      % { if ( $_ -match ">>>(?<filename>.*)" ) { Get-Content $Matches.filename } } |
      jq -acsS "map({key:.short_label,value:.long_label})|from_entries"

    if ($lastexitcode -ne 0) {
      throw "jq returned non-zero exit code: $lastexitcode"
    }
  } finally {
    Remove-Item $tmp
  }

}
