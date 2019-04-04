#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# package-app <binary> <output file> <resources...>
# 1. Create a temporary directory and lib directory
# 2. Copy the binary to the temporary directory
# 3. Use platform specific tools to extract names of the
#    dynamic libraries and copy them to lib directory.
# 4. Create a wrapper or patch the binary.
# 5. Create tarball out of the temporary directory.
# 6. Clean up.
set -e

WORKDIR="$(mktemp -d)"
trap "rm -rf $WORKDIR" EXIT

SRC=$1
OUT=$2
shift 2
NAME=$(basename $SRC)
mkdir -p $WORKDIR/$NAME/lib
export ORIGIN=$(dirname $SRC) # for rpaths relative to binary

# Copy in resources, if any.
if [ $# -gt 0 ]; then
  mkdir -p $WORKDIR/$NAME/resources
  for res in $*; do
    cp -aL "$res" "$WORKDIR/$NAME/resources"
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
else
  cp $SRC $WORKDIR/$NAME/$NAME
  chmod u+w $WORKDIR/$NAME/$NAME
  function copy_deps() {
    local from=$1
    local needed="$(/usr/bin/otool -L "$from" | sed -n -e '1d' -e '/\/nix\/store/ s/^.*\(\/nix\/store[^ ]*\).*/\1/p')"
    for lib in $needed; do
      local libName="$(basename $lib)"
      if [[ "$libName" == "libSystem.B.dylib" ]]; then
        /usr/bin/install_name_tool -change "$lib" "/usr/lib/$libName" "$from"
      else
        /usr/bin/install_name_tool -change "$lib" "@rpath/$libName" "$from"

        local to="$WORKDIR/$NAME/lib/$libName"
        if [ ! -f "$to" ]; then
          cp "$lib" "$to"
          chmod 0755 "$to"
          /usr/bin/install_name_tool -add_rpath "@loader_path/lib" "$to"

          # Traverse the library as well to find the closure
          copy_deps "$to"
        fi
      fi
    done
  }
  # Set the dynamic library load path to the relative lib/ directory.
  /usr/bin/install_name_tool -add_rpath "@loader_path/lib" $WORKDIR/$NAME/$NAME

  # Copy all dynamic library dependencies referred to from our binary
  copy_deps $WORKDIR/$NAME/$NAME
fi
cd $WORKDIR && tar czf $OUT $NAME

