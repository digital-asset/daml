# This is based on the version that is in nixpkgs (commit 5d19e3e78fb89f01e7fb48acc341687e54d6888f)
# but we patch ghcWithPackages so that it provides package config files with extra-libraries
# instead of hs-libraries which is required for GHCi and TH to pick up static Haskell libs.
{ lib, targetPlatform, ghc, llvmPackages, packages, symlinkJoin, makeWrapper
, withLLVM ? false
, postBuild ? ""
, ghcLibdir ? null # only used by ghcjs, when resolving plugins
}:

assert ghcLibdir != null -> (ghc.isGhcjs or false);

# This wrapper works only with GHC 6.12 or later.
assert lib.versionOlder "6.12" ghc.version || ghc.isGhcjs || ghc.isHaLVM;

# It's probably a good idea to include the library "ghc-paths" in the
# compiler environment, because we have a specially patched version of
# that package in Nix that honors these environment variables
#
#   NIX_GHC
#   NIX_GHCPKG
#   NIX_GHC_DOCDIR
#   NIX_GHC_LIBDIR
#
# instead of hard-coding the paths. The wrapper sets these variables
# appropriately to configure ghc-paths to point back to the wrapper
# instead of to the pristine GHC package, which doesn't know any of the
# additional libraries.
#
# A good way to import the environment set by the wrapper below into
# your shell is to add the following snippet to your ~/.bashrc:
#
#   if [ -e ~/.nix-profile/bin/ghc ]; then
#     eval $(grep export ~/.nix-profile/bin/ghc)
#   fi

let
  isGhcjs       = ghc.isGhcjs or false;
  isHaLVM       = ghc.isHaLVM or false;
  ghc761OrLater = isGhcjs || isHaLVM || lib.versionOlder "7.6.1" ghc.version;
  packageDBFlag = if ghc761OrLater then "--global-package-db" else "--global-conf";
  ghcCommand'    = if isGhcjs then "ghcjs" else "ghc";
  ghcCommand = "${ghc.targetPrefix}${ghcCommand'}";
  ghcCommandCaps= lib.toUpper ghcCommand';
  libDir        = if isHaLVM then "$out/lib/HaLVM-${ghc.version}" else "$out/lib/${ghcCommand}-${ghc.version}";
  docDir        = "$out/share/doc/ghc/html";
  packageCfgDir = "${libDir}/package.conf.d";
  paths         = lib.filter (x: x ? isHaskellLibrary) (lib.closePropagation packages);
  hasLibraries  = lib.any (x: x.isHaskellLibrary) paths;
  # CLang is needed on Darwin for -fllvm to work:
  # https://downloads.haskell.org/~ghc/latest/docs/html/users_guide/codegens.html#llvm-code-generator-fllvm
  llvm          = lib.makeBinPath
                  ([ llvmPackages.llvm ]
                   ++ lib.optional targetPlatform.isDarwin llvmPackages.clang);
in
symlinkJoin {
  # this makes computing paths from the name attribute impossible;
  # if such a feature is needed, the real compiler name should be saved
  # as a dedicated drv attribute, like `compiler-name`
  name = ghc.name + "-with-packages";
  preferLocalBuild = false;
  allowSubstitutes = true;
  paths = paths ++ [ghc];
  postBuild = ''
    . ${makeWrapper}/nix-support/setup-hook

    # wrap compiler executables with correct env variables

    for prg in ${ghcCommand} ${ghcCommand}i ${ghcCommand}-${ghc.version} ${ghcCommand}i-${ghc.version}; do
      if [[ -x "${ghc}/bin/$prg" ]]; then
        rm -f $out/bin/$prg
        makeWrapper ${ghc}/bin/$prg $out/bin/$prg                           \
          --add-flags '"-B$NIX_${ghcCommandCaps}_LIBDIR"'                   \
          --set "NIX_${ghcCommandCaps}"        "$out/bin/${ghcCommand}"     \
          --set "NIX_${ghcCommandCaps}PKG"     "$out/bin/${ghcCommand}-pkg" \
          --set "NIX_${ghcCommandCaps}_DOCDIR" "${docDir}"                  \
          --set "NIX_${ghcCommandCaps}_LIBDIR" "${libDir}"                  \
          ${lib.optionalString (ghc.isGhcjs or false)
            ''--set NODE_PATH "${ghc.socket-io}/lib/node_modules"''
          } \
          ${lib.optionalString withLLVM ''--prefix "PATH" ":" "${llvm}"''}
      fi
    done

    for prg in runghc runhaskell; do
      if [[ -x "${ghc}/bin/$prg" ]]; then
        rm -f $out/bin/$prg
        makeWrapper ${ghc}/bin/$prg $out/bin/$prg                           \
          --add-flags "-f $out/bin/${ghcCommand}"                           \
          --set "NIX_${ghcCommandCaps}"        "$out/bin/${ghcCommand}"     \
          --set "NIX_${ghcCommandCaps}PKG"     "$out/bin/${ghcCommand}-pkg" \
          --set "NIX_${ghcCommandCaps}_DOCDIR" "${docDir}"                  \
          --set "NIX_${ghcCommandCaps}_LIBDIR" "${libDir}"
      fi
    done

    for prg in ${ghcCommand}-pkg ${ghcCommand}-pkg-${ghc.version}; do
      if [[ -x "${ghc}/bin/$prg" ]]; then
        rm -f $out/bin/$prg
        makeWrapper ${ghc}/bin/$prg $out/bin/$prg --add-flags "${packageDBFlag}=${packageCfgDir}"
      fi
    done

  '' + ''
    # Patch the package configs shipped with GHC so that it treats Haskell
    # libraries like C libraries which allows linking against static Haskell libs from
    # TH and GHCi.
    local packageConfDir="$out/lib/${ghc.name}/package.conf.d";
    for f in $packageConfDir/*.conf; do
      filename="$(basename $f)"
        if [ "$filename" != "rts.conf" ]; then
          cp $f $f-tmp
          rm $f
          sed -e "s/hs-libraries/extra-libraries/g" -e "s,${ghc},$out,g" $f-tmp > $f
          rm $f-tmp
        fi
    done
  '' + ''
    $out/bin/${ghcCommand}-pkg recache
    ${# ghcjs will read the ghc_libdir file when resolving plugins.
      lib.optionalString (isGhcjs && ghcLibdir != null) ''
      mkdir -p "${libDir}"
      rm -f "${libDir}/ghc_libdir"
      printf '%s' '${ghcLibdir}' > "${libDir}/ghc_libdir"
    ''}
    $out/bin/${ghcCommand}-pkg check
  '' + postBuild;
  passthru = {
    preferLocalBuild = true;
    inherit (ghc) version meta;
  };
}
