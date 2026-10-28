#!/usr/bin/env bash
set -uo pipefail; set +e; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e

resolve() {
  local path="$1" dir
  while [[ -L "$path" ]]; do
    dir="$(cd -P "$(dirname "$path")" && pwd)"
    path="$(readlink "$path")"
    [[ "$path" == /* ]] || path="$dir/$path"
  done
  printf '%s/%s\n' "$(cd -P "$(dirname "$path")" && pwd)" "$(basename "$path")"
}

hls="$(resolve "$(rlocation "$DAML_IDE_HLS")")"
ghc_bin="$(dirname "$(resolve "$(rlocation "$DAML_IDE_GHC")")")"

if [[ "${1-}" == "--project-ghc-version" ]]; then
  exec "$ghc_bin/ghc" --numeric-version
fi

libdir="$("$ghc_bin/ghc" --print-libdir)"
library_path=""
for dir in "$libdir"/*/; do
  library_path="$library_path${dir%/}:"
done
library_path="${library_path%:}"

tool_path="$ghc_bin"
for root in "$libdir/.." "$libdir/../.."; do
  if [[ -d "$root/llvm-backend/bin" ]]; then
    tool_path="$tool_path:$(cd "$root/llvm-backend/bin" && pwd)"
    if [[ -d "$root/llvm-backend/lib" ]]; then
      library_path="$library_path:$(cd "$root/llvm-backend/lib" && pwd)"
    fi
    break
  fi
done

export DAML_IDE_HLS_HOST_PATH="$PATH"
export DAML_IDE_HLS_HOST_LD_LIBRARY_PATH="${LD_LIBRARY_PATH-}"
export PATH="$tool_path:$PATH"
case "$(uname -s)" in
  Darwin) export DYLD_LIBRARY_PATH="$library_path${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" ;;
  *) export LD_LIBRARY_PATH="$library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" ;;
esac

cd "${DAML_IDE_HLS_CWD:-${BUILD_WORKING_DIRECTORY:-$PWD}}"
exec "$hls" "$@"
