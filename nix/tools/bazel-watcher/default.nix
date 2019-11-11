{ buildBazelPackage
, fetchFromGitHub
, git
, go
, python
, stdenv
}:

buildBazelPackage rec {
  name = "bazel-watcher-${version}";
  version = "git-18bdb44";

  src = fetchFromGitHub {
    owner = "bazelbuild";
    repo = "bazel-watcher";
    rev = "18bdb44832ccc533e0ab3923ef80060eeb24582d";
    sha256 = "17z4nqqsdrainbh8fmhf6sgrxwf7aknadmn94z1yqpxa7kb9x33v";
  };

  nativeBuildInputs = [ go git python ];

  bazelTarget = "//ibazel";
  bazelFlags = [
    "--incompatible_string_join_requires_strings=false"
  ];

  fetchAttrs = {
    preBuild = ''
      patchShebangs .

      # tell rules_go to use the Go binary found in the PATH
      sed -e 's:go_register_toolchains():go_register_toolchains(go_version = "host"):g' -i WORKSPACE
    '';

    preInstall = ''
      # Remove the go_sdk (it's just a copy of the go derivation) and all
      # references to it from the marker files. Bazel does not need to download
      # this sdk because we have patched the WORKSPACE file to point to the one
      # currently present in PATH. Without removing the go_sdk from the marker
      # file, the hash of it will change anytime the Go derivation changes and
      # that would lead to impurities in the marker files which would result in
      # a different sha256 for the fetch phase.
      rm -rf $bazelOut/external/{go_sdk,\@go_sdk.marker}
      sed -e '/^FILE:@go_sdk.*/d' -i $bazelOut/external/\@*.marker

      # Remove the gazelle tools, they contain go binaries that are built
      # non-deterministically. As long as the gazelle version matches the tools
      # should be equivalent.
      rm -rf $bazelOut/external/{bazel_gazelle_go_repository_tools,\@bazel_gazelle_go_repository_tools.marker}
      sed -e '/^FILE:@bazel_gazelle_go_repository_tools.*/d' -i $bazelOut/external/\@*.marker
    '';

    sha256 = "151s3k2phkdb32yxdja1q0kbxgb4alz8l9wfyzgd2mbdrkq4a62q";
  };

  buildAttrs = {
    preBuild = ''
      patchShebangs .

      echo build "--incompatible_require_ctx_in_configure_features=false" >> .bazelrc

      # tell rules_go to use the Go binary found in the PATH
      sed -e 's:go_register_toolchains():go_register_toolchains(go_version = "host"):g' -i WORKSPACE
    '';

    installPhase = ''
      install -Dm755 bazel-bin/ibazel/*_pure_stripped/ibazel $out/bin/ibazel
    '';
  };

  meta = with stdenv.lib; {
    homepage = https://github.com/bazelbuild/bazel-watcher;
    description = "Tools for building Bazel targets when source files change.";
    license = licenses.asl20;
    maintainers = with maintainers; [ kalbasit ];
    platforms = platforms.all;
  };
}
