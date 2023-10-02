with import <nixpkgs> { config = {}; overlays = []; };

let
  flatten = builtins.concatMap (x: if builtins.isList x then x else [x]);
  env = buildEnv {
    name = "posix-toolchain";
    paths = flatten [ stdenv.initialPath ];
  };
  cmd_glob = "${env}/bin/*";
  os = if stdenv.isDarwin then "osx" else "linux";
in

runCommand "bazel-nixpkgs-posix-toolchain"
  { executable = false;
    # Pointless to do this on a remote machine.
    preferLocalBuild = true;
    allowSubstitutes = false;
  }
  ''
    n=$out/nixpkgs_sh_posix.bzl
    mkdir -p "$(dirname "$n")"

    cat >>$n <<EOF
    load("@rules_sh//sh:posix.bzl", "posix", "sh_posix_toolchain")
    discovered = {
    EOF
    for cmd in ${cmd_glob}; do
        if [[ -x $cmd ]]; then
            echo "    '$(basename $cmd)': '$cmd'," >>$n
        fi
    done
    cat >>$n <<EOF
    }
    def create_posix_toolchain():
        sh_posix_toolchain(
            name = "nixpkgs_sh_posix",
            cmds = {
                cmd: discovered[cmd]
                for cmd in posix.commands
                if cmd in discovered
            }
        )
    EOF
  ''
