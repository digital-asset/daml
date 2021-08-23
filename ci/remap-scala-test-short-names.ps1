# $ErrorActionPreference = 'Stop' causes the script to fail because Bazel writes to stderr.
$ErrorActionPreference = 'Continue'

[string[]]$scala_test_targets = bazel.exe query "kind(scala_test, deps(kind(test_suite, //...), 1))"
if ($lastexitcode -ne 0) {
  throw "bazel query returned non-zero exit code: $lastexitcode"
}

if ($scala_test_targets.count -gt 0) {

  [string]$errmsg = ""

  $(& bazel.exe build `
        `-`-aspects=//bazel_tools:scala.bzl%da_scala_test_short_name_aspect `
        `-`-output_groups=scala_test_info `
        `-`-experimental_show_artifacts `
        @scala_test_targets `
        2>&1; $bazelexitcode=$lastexitcode)`
    | foreach { if ( $_ -match ">>>(?<filename>.*)" ) { Get-Content $Matches.filename } else { $errmsg += $_ } } `
    | jq -acsS "map({key:.short_label,value:.long_label})|from_entries"

  if ($lastexitcode -ne 0) {
    throw "jq returned non-zero exit code: $lastexitcode"
  }
  if ($bazelexitcode -ne 0) {
    Write-Error -Message "$errmsg"
    throw "bazel build returned non-zero exit code: $lastexitcode"
  }

}
