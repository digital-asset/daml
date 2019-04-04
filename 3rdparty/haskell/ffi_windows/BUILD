# vim: ft=python
#
# This simply re-exports the headers bundled with GHC.
# NOTES:
#   * This only works on Windows because the nixpkgs-provided GHC does not
#       include the `ffi` headers.
#   * We _must_ alias because the name is used when preparing the dependency
#       list. If we simply re-use rts-headers as ffi the build complains that
#       rts-headers appears twice in the list.
#

package(default_visibility = ["//visibility:public"])

load("@os_info//:os_info.bzl", is_windows = "is_windows")

alias(name = "ffi", actual = "@io_tweag_rules_haskell_ghc_windows_amd64//:rts-headers") if is_windows else None
