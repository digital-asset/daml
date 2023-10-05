let
  system = builtins.currentSystem;
  src = builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/9a82a9b5248919805a2400266ebd881d5783df2a.tar.gz";
    sha256 = "142x1zq3cjadgmvfv0paydlq268pfinllqpq2vl0vxwdiq2nr9iz";
  };
  pkgs = import src {
    inherit system;

    config.allowUnfree = true;
    config.allowBroken = true;
  };
in {
  semver = pkgs.callPackage ./tools/semver-tool {};
}
