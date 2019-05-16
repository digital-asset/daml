# Based on upstream nixpkgs Nix expression for bazel-deps.
# Revision b36dc66bfea6b0a733cf13bed85d80462d39c736

{ stdenv, buildBazelPackage, lib, fetchFromGitHub, cacert, git, jre, makeWrapper }:

buildBazelPackage rec {
  name = "bazel-deps-${version}";
  __noChroot = true;
  version = "2019-03-05";

  meta = with stdenv.lib; {
    homepage = "https://github.com/johnynek/bazel-deps";
    description = "Generate bazel dependencies for maven artifacts";
    license = licenses.mit;
    platforms = platforms.all;
  };

  src = fetchFromGitHub {
    owner = "johnynek";
    repo = "bazel-deps";
    rev = "a53246efd3bcabc1362c830f53b6ac4818871b12";
    sha256 = "0fa66yz619lz07lygf7dfvnqbj3ai4g6dwk7l92j2l5c4kpbx29a";
  };

  patches = [./maven-coordinates.patch];

  bazelTarget = "//src/scala/com/github/johnynek/bazel_deps:parseproject_deploy.jar";

  buildInputs = [ git makeWrapper ];

  fetchAttrs = {
    preBuild = ''
      export GIT_SSL_CAINFO="${cacert}/etc/ssl/certs/ca-bundle.crt"
    '';
    preInstall = ''
      # Remove all built in external workspaces, Bazel will recreate them when building
      rm -rf $bazelOut/external/{bazel_tools,\@bazel_tools.marker,embedded_jre,\@embedded_jre.marker,local_*,\@local_*}
      # For each external workspace, remove all files that aren't referenced by Bazel
      # Many of these files are non-hermetic (for example .git/refs/remotes/origin/HEAD)
      files_to_delete=()
      for workspace in $(find $bazelOut/external -maxdepth 2 -name "WORKSPACE" -print0 | xargs -0L1 dirname); do
        workspaceOut="$NIX_BUILD_TOP/workspaces/$(basename workspace)/output"
        workspaceUserRoot="$NIX_BUILD_TOP/workspaces/$(basename workspace)/tmp"
        rm -rf $workspace/.git
        if ! targets_and_files=$(cd "$workspace" && bazel --output_base="$workspaceOut" --output_user_root="$workspaceUserRoot" query '//...:*' 2> /dev/null | sort -u); then
          continue
        fi
        if ! targets=$(cd "$workspace" && bazel --output_base="$workspaceOut" --output_user_root="$workspaceUserRoot" query '//...:all' 2> /dev/null | sort -u); then
          continue
        fi
        mapfile -t referenced_files < <(comm -23 <(printf '%s' "$targets_and_files") <(printf '%s' "$targets") | sed -e 's,^//:,,g' | sed -e 's,^//,,g' | sed -e 's,:,/,g')
        referenced_files+=( "WORKSPACE" )
        for referenced_file in "''${referenced_files[@]}"; do
          # Some of the referenced files are symlinks to non-referenced files.
          # The symlink targets have deterministic contents, but non-deterministic
          # paths. Copy them to the referenced path, which is deterministic.
          if target=$(readlink "$workspace/$referenced_file"); then
            rm "$workspace/$referenced_file"
            cp -a "$target" "$workspace/$referenced_file"
          fi
        done
        mapfile -t workspace_files_to_delete < <(find "$workspace" -type f -or -type l | sort -u | comm -23 - <(printf "$workspace/%s\n" "''${referenced_files[@]}" | sort -u))
        for workspace_file_to_delete in "''${workspace_files_to_delete[@]}"; do
          files_to_delete+=("$workspace_file_to_delete")
        done
        # We're running bazel in many different workspaces in a loop. Letting the
        # daemon shut down on its own would leave several daemons alive at the
        # same time, using up a lot of memory. Shut them down explicitly instead.
        bazel --output_base="$workspaceOut" --output_user_root="$workspaceUserRoot" shutdown 2> /dev/null
      done
      for file_to_delete in "''${files_to_delete[@]}"; do
        rm "$file_to_delete"
      done
      find . -type d -empty -delete
    '';

    sha256 = "0kxb8xh9lqf2b3fx5ln3dm7dbs2min172xa6rdpvqmcl4vy73vcp";
  };

  buildAttrs = {
    postPatch = ''
      # Configure Bazel to use JDK8
      cat >> .bazelrc <<EOF
      build --host_java_toolchain=@bazel_tools//tools/jdk:toolchain_hostjdk8
      build --java_toolchain=@bazel_tools//tools/jdk:toolchain_hostjdk8
      build --host_javabase=@local_jdk//:jdk
      build --javabase=@local_jdk//:jdk
      EOF
    '';
    preConfigure = ''
      export JAVA_HOME="${jre.home}"
    '';
    installPhase = ''
      mkdir -p $out/bin/bazel-bin/src/scala/com/github/johnynek/bazel_deps

      cp gen_maven_deps.sh $out/bin/bazel-deps
      wrapProgram "$out/bin/bazel-deps" --set JAVA_HOME "${jre.home}" --prefix PATH : ${lib.makeBinPath [ jre ]}
      cp bazel-bin/src/scala/com/github/johnynek/bazel_deps/parseproject_deploy.jar $out/bin/bazel-bin/src/scala/com/github/johnynek/bazel_deps
    '';
  };
}
