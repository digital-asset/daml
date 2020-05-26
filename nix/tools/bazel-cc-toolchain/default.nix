{ stdenv
, binutils
, buildEnv
, darwin
, llvmPackages
, makeWrapper
, overrideCC
, runCommand
}:


# XXX On Darwin, workaround
# https://github.com/NixOS/nixpkgs/issues/42059. See also
# https://github.com/NixOS/nixpkgs/pull/41589.
let cc-darwin =
  with darwin.apple_sdk.frameworks;
  runCommand "cc-wrapper-bazel"
  {
    buildInputs = [ stdenv.cc makeWrapper ];
  }
  ''
    mkdir -p $out/bin

    # Copy the content of pkgs.stdenv.cc
    for i in ${stdenv.cc}/bin/*
    do
      ln -sf $i $out/bin
    done

    # Override cc
    rm $out/bin/cc $out/bin/clang $out/bin/clang++

    makeWrapper ${stdenv.cc}/bin/cc $out/bin/cc \
      --add-flags "-Wno-unused-command-line-argument \
                   -mmacosx-version-min=10.14 \
                   -isystem ${llvmPackages.libcxx}/include/c++/v1 \
                   -F${CoreFoundation}/Library/Frameworks \
                   -F${CoreServices}/Library/Frameworks \
                   -F${Security}/Library/Frameworks \
                   -F${Foundation}/Library/Frameworks \
                   -L${llvmPackages.libcxx}/lib \
                   -L${darwin.libobjc}/lib"
  '';

  cc-linux = runCommand "cc-wrapper-bazel" {
    buildInputs = [ makeWrapper ];
  }
  ''
    mkdir -p $out/bin

    # Copy the content of pkgs.stdenv.cc
    for i in ${stdenv.cc}/bin/*
    do
      ln -sf $i $out/bin
    done

    # Override gcc
    rm $out/bin/cc $out/bin/gcc $out/bin/g++

    # We disable the fortify hardening as it causes issues with some
    # packages built with bazel that set these flags themselves.
    makeWrapper ${stdenv.cc}/bin/cc $out/bin/cc \
      --set hardeningDisable fortify
  '';

  customStdenv =
    if stdenv.isDarwin
    then overrideCC stdenv cc-darwin
    else overrideCC stdenv cc-linux;
in
buildEnv {
  name = "bazel-cc-toolchain";
  paths = [ customStdenv.cc ] ++ (if stdenv.isDarwin then [ darwin.binutils ] else [ binutils ]);
}
