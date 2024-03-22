{ stdenv
, binutils
, bintools
, buildEnv
, darwin
, llvmPackages_12
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
      cc = llvmPackages_12.clang;
      bintools = darwinBinutils;
      extraBuildCommands = with darwin.apple_sdk.frameworks; ''
        echo "-Wno-unused-command-line-argument" >> $out/nix-support/cc-cflags
        echo "-Wno-elaborated-enum-base" >> $out/nix-support/cc-cflags
        echo "-mmacosx-version-min=${cc.darwinMinVersion}" >> $out/nix-support/cc-cflags
        echo "-isystem ${llvmPackages_12.libcxx.dev}/include/c++/v1" >> $out/nix-support/cc-cflags
        echo "-isystem ${llvmPackages_12.clang-unwrapped.lib}/lib/clang/${cc.version}/include" >> $out/nix-support/cc-cflags
        echo "-F${CoreFoundation}/Library/Frameworks" >> $out/nix-support/cc-cflags
        echo "-F${CoreServices}/Library/Frameworks" >> $out/nix-support/cc-cflags
        echo "-F${Security}/Library/Frameworks" >> $out/nix-support/cc-cflags
        echo "-F${Foundation}/Library/Frameworks" >> $out/nix-support/cc-cflags
        echo "-L${llvmPackages_12.libcxx}/lib" >> $out/nix-support/cc-cflags
        echo "-L${llvmPackages_12.libcxxabi}/lib" >> $out/nix-support/cc-cflags
        echo "-L${darwin.libobjc}/lib" >> $out/nix-support/cc-cflags
        echo "-D_DNS_SD_LIBDISPATCH" >> $out/nix-support/cc-cflags # Needed for DNSServiceSetDispatchQueue to be available for gRPC
        echo "-std=c++14" >> $out/nix-support/libcxx-cxxflags
      '';
    };

  cc-linux =
    wrapCCWith {
      cc = stdenv.cc.overrideAttrs (oldAttrs: {
        hardeningUnsupportedFlags =
          ["fortify"] ++ oldAttrs.hardeningUnsupportedFlags or [];
      });
      extraBuildCommands = ''
        echo "-std=c++14" >> $out/nix-support/libcxx-cxxflags
      '';
    };

  customStdenv =
    if stdenv.isDarwin
    then overrideCC stdenv cc-darwin
    else overrideCC stdenv cc-linux;
in
buildEnv {
  name = "bazel-cc-toolchain";
  paths = [ customStdenv.cc ] ++ (if stdenv.isDarwin then [ darwinBinutils ] else [ binutils ]);
  ignoreCollisions = true;
  passthru = { isClang = customStdenv.cc.isClang; };
}
