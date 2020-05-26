#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# package-app <binary> <output file> <resources...>

# On Linux and MacOS we handle dynamically linked binaries as follows:
# 1. Create a temporary directory and lib directory
# 2. Copy the binary to the temporary directory
# 3. Use platform specific tools to extract names of the
#    dynamic libraries and copy them to lib directory.
# 4. Create a wrapper or patch the binary.
# 5. Create tarball out of the temporary directory.
# 6. Clean up.

# On Windows we only handle statically linked binaries
# (Haskell binaries are linked statically on Windows) so we
# just copy the binary and the resources and create a tarball from that.
set -eou pipefail

WORKDIR="$(mktemp -d)"
trap "rm -rf $WORKDIR" EXIT

SRC=$1
OUT=$2
shift 2
NAME=$(basename $SRC)
mkdir -p $WORKDIR/$NAME/lib
export ORIGIN=$(dirname $(readlink -f $SRC)) # for rpaths relative to binary

# Copy in resources, if any.
if [ $# -gt 0 ]; then
  mkdir -p $WORKDIR/$NAME/resources
  for res in $*; do
    if [[ "$res" == *.tar.gz ]]; then
      # If a resource is a tarball, e.g., because it originates from another
      # rule we extract it.
      tar xf "$res" --strip-components=1 -C "$WORKDIR/$NAME/resources"
    else
      cp -aL "$res" "$WORKDIR/$NAME/resources"
    fi
  done
fi

# Copy the binary, patch it, and create a wrapper script if necessary.
if [ "$(uname -s)" == "Linux" ]; then
  binary="$WORKDIR/$NAME/lib/$NAME"
  cp $SRC $binary
  chmod u+w $binary
  rpaths_binary=$(patchelf --print-rpath "$binary"|tr ':' ' ')
  function copy_deps {
    local from target needed libOK rpaths
    from=$1
    target=$2
    needed=$(patchelf --print-needed "$from")
    rpaths="$(patchelf --print-rpath "$from"|tr ':' ' ') $rpaths_binary"

    for lib in $needed; do
      if [ ! -f "$target/$lib" ]; then
        libOK=0
        for rpath in $rpaths; do
          rpath="$(eval echo $rpath)" # expand variables, e.g. $ORIGIN
          if [ -e "$rpath/$lib" ]; then
            libOK=1
            cp "$rpath/$lib" "$target/$lib"
            chmod u+w "$target/$lib"
            if [ "$lib" != "ld-linux-x86-64.so.2" ]; then
              # clear the old rpaths (silence stderr as it always warns
              # with "working around a Linux kernel bug".
              patchelf --set-rpath '$ORIGIN' "$target/$lib" 2> /dev/null
            fi
            copy_deps "$rpath/$lib" "$target"
            break
          fi
        done
        if [ $libOK -ne 1 ]; then
          echo "ERROR: Dynamic library $lib for $from not found from RPATH!"
          echo "RPATH=$rpaths"
          return 1
        fi
      fi
    done
  }

  # Copy the binary's dynamic library dependencies.
  copy_deps "$binary" "$WORKDIR/$NAME/lib"

  # Workaround for dynamically loaded name service switch libraries
  (shopt -s nullglob
   for rpath in $rpaths_binary; do
     for nss in $rpath/libnss_dns* $rpath/libnss_files* $rpath/libresolv*; do
       cp -af "$nss" "$WORKDIR/$NAME/lib"
     done
   done
  )

  patchelf --set-rpath '$ORIGIN/lib' "$binary"
  patchelf --set-interpreter ld-undefined.so "$binary"

  # Link resources directory to lib, as that'll be the actual
  # binary's location.
  test -d "$WORKDIR/$NAME/resources" &&
    ln -s "../resources" "$WORKDIR/$NAME/lib/resources"

  # Create the wrapper script
  wrapper="$WORKDIR/$NAME/$NAME"
  cat > "$wrapper" <<EOF
#!/usr/bin/env sh
SOURCE_DIR="\$(cd \$(dirname \$(readlink -f "\$0")); pwd)"
LIB_DIR="\$SOURCE_DIR/lib"
# Execute the wrapped application through the provided dynamic linker
exec \$LIB_DIR/ld-linux-x86-64.so.2 --library-path "\$LIB_DIR" "\$LIB_DIR/$NAME" "\$@"
EOF
  chmod a+x "$wrapper"
elif [[ "$(uname -s)" == "Darwin" ]]; then
  cp $SRC $WORKDIR/$NAME/$NAME
  chmod u+w $WORKDIR/$NAME/$NAME
  function copy_deps() {
    local from_original=$(readlink -f $1)
    local from_copied=$2
    local needed="$(/usr/bin/otool -L "$from_copied" | sed -n -e '1d' -e 's/^\s*\([^ ]*\).*$/\1/p')"
    # Note that it is crucial that we resolve loader_path relative to from_original instead of from_copied
    loader_path="$(dirname $from_original)"
    local rpaths="$(/usr/bin/otool -l $from_original | sed -n '/cmd LC_RPATH/{n;n;p;}' | sed -n -e 's/^ *path \([^ ]*\).*$/\1/p' | sed -e "s|@loader_path|$loader_path|")"
    for lib in $needed; do
      local libName="$(basename $lib)"
      if [[ "$libName" == "libSystem.B.dylib" ]]; then
          /usr/bin/install_name_tool -change "$lib" "/usr/lib/$libName" "$from_copied"
      elif [ -e "/usr/lib/system/$libName" ]; then
          /usr/bin/install_name_tool -change "$lib" "/usr/lib/system/$libName" "$from_copied"
      elif [[ "$lib" == @rpath/* ]]; then
          libName="${lib#@rpath/}"
          /usr/bin/install_name_tool -change "$lib" "@rpath/$(basename $libName)" "$from_copied"
          local to="$WORKDIR/$NAME/lib/$(basename $libName)"
          if [[ ! -f "$to" ]]; then
              libOK=0
              for rpath in $rpaths; do
                  if [[ -e "$rpath/$libName" ]]; then
                      libOK=1
                      cp "$rpath/$libName" "$to"
                      chmod 0755 "$to"
                      /usr/bin/install_name_tool -add_rpath "@loader_path/lib" "$to"
                      copy_deps "$rpath/$libName" "$to"
                      break
                  fi
              done
              if [[ $libOK -ne 1 ]]; then
                  echo "ERROR: Dynamic library $lib for $from_original not found from RPATH!"
                  echo "RPATH=$rpaths"
                  return 1
              fi
          fi
      else
        /usr/bin/install_name_tool -change "$lib" "@rpath/$libName" "$from_copied"

        local to="$WORKDIR/$NAME/lib/$libName"
        if [ ! -f "$to" ]; then
          cp "$lib" "$to"
          chmod 0755 "$to"
          /usr/bin/install_name_tool -add_rpath "@loader_path/lib" "$to"

          # Traverse the library as well to find the closure
          copy_deps "$lib" "$to"
        fi
      fi
    done
  }
  # Set the dynamic library load path to the relative lib/ directory.
  /usr/bin/install_name_tool -add_rpath "@loader_path/lib" $WORKDIR/$NAME/$NAME

  # Copy all dynamic library dependencies referred to from our binary
  copy_deps $SRC $WORKDIR/$NAME/$NAME
else
    cp "$SRC" "$WORKDIR/$NAME/$NAME"
fi
cd $WORKDIR && tar c $NAME \
    --owner=0 --group=0 --numeric-owner --mtime=2000-01-01\ 00:00Z --sort=name \
    | gzip -n > $OUT
