{ stdenv
, binutils
, bintools
, buildEnv
, coreutils
, darwin
, lib
, libiconv
, llvmPackages
, makeWrapper
, wrapCCWith
, overrideCC
, runCommand
, writeTextFile
, sigtool
}:


# XXX On Darwin, workaround
# https://github.com/NixOS/nixpkgs/issues/42059. See also
# https://github.com/NixOS/nixpkgs/pull/41589.
let
  postLinkSignHook =
    writeTextFile {
      name = "post-link-sign-hook";
      executable = true;

      text = ''
        CODESIGN_ALLOCATE=${darwin.cctools}/bin/codesign_allocate \
          ${sigtool}/bin/codesign -f -s - "$linkerOutput"
      '';
    };
  darwinBinutils = darwin.binutils.override { inherit postLinkSignHook; };
  cc-darwin =
    wrapCCWith rec {
      cc = stdenv.cc.cc;
      bintools = stdenv.cc.bintools.override { inherit postLinkSignHook; };
      extraBuildCommands =
        with darwin.apple_sdk.frameworks;
        ''
          echo "-Wno-unused-command-line-argument" >> $out/nix-support/cc-cflags
          echo "-Wno-elaborated-enum-base" >> $out/nix-support/cc-cflags
          echo "-isystem ${llvmPackages.libcxx.dev}/include/c++/v1" >> $out/nix-support/cc-cflags
          echo "-isystem ${llvmPackages.clang-unwrapped.lib}/lib/clang/${cc.version}/include" >> $out/nix-support/cc-cflags
          echo "-F${CoreFoundation}/Library/Frameworks" >> $out/nix-support/cc-cflags
          echo "-F${CoreServices}/Library/Frameworks" >> $out/nix-support/cc-cflags
          echo "-F${Security}/Library/Frameworks" >> $out/nix-support/cc-cflags
          echo "-F${Foundation}/Library/Frameworks" >> $out/nix-support/cc-cflags
          echo "-F${SystemConfiguration}/Library/Frameworks" >> $out/nix-support/cc-cflags
          echo "-L${llvmPackages.libcxx}/lib" >> $out/nix-support/cc-cflags
          echo "-L${libiconv}/lib" >> $out/nix-support/cc-cflags
          echo "-L${darwin.libobjc}/lib" >> $out/nix-support/cc-cflags
          echo "-resource-dir=${stdenv.cc}/resource-root" >> $out/nix-support/cc-cflags
          echo "-D_DNS_SD_LIBDISPATCH" >> $out/nix-support/cc-cflags # Needed for DNSServiceSetDispatchQueue to be available for gRPC
        '';
      nativeTools = false;
    };

  cc-linux =
    wrapCCWith {
      cc = stdenv.cc.overrideAttrs (oldAttrs: {
        hardeningUnsupportedFlags =
          ["fortify"] ++ oldAttrs.hardeningUnsupportedFlags or [];
      });
      extraBuildCommands = ''
        echo "-std=c++14" >> $out/nix-support/libcxx-cxxflags
        echo "-L${stdenv.cc.cc.lib}/lib" >> $out/nix-support/cc-cflags
      '';
    };

  customStdenv =
    if stdenv.isDarwin
    then overrideCC stdenv cc-darwin
    else overrideCC stdenv cc-linux;
in
  buildEnv (
    {
      name = "bazel-cc-toolchain";
      paths = [ customStdenv.cc ] ++ (if stdenv.isDarwin then [ darwinBinutils ] else [ binutils ]);
      ignoreCollisions = true;
      passthru = { isClang = customStdenv.cc.isClang; targetPrefix = customStdenv.cc.targetPrefix; };
    } // (lib.optionalAttrs stdenv.isDarwin {
      nativeBuildInputs = [ makeWrapper ];
      # only add tools from darwin.cctools, but don't overwrite existing tools
      postBuild = ''
        for tool in libtool objdump; do
           if [[ ! -e $out/bin/$tool ]]; then
             ln -s -t $out/bin ${darwin.cctools}/bin/$tool
           fi
        done

        wrapProgram $out/bin/cc --prefix PATH : ${coreutils}/bin
      '';
    })
  )
