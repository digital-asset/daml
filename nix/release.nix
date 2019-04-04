# Build instructions for Hydra CI
{ damlSrc ? { outPath = "${toString ../.}"; rev = "dirty"; }
, releaseBuild ? false
, supportedSystems ? ["x86_64-linux" "x86_64-darwin"]
}:

let
  nixpkgs = import "${damlSrc}/nix/nixpkgs.nix" {};

  withSystem = nixpkgs.lib.genAttrs supportedSystems;

  packages = system:
    import "${damlSrc}/nix/packages.nix" {
      inherit system;
    };
in
with nixpkgs;
with nixpkgs.lib;

{
  tools = withSystem (system: (packages system).tools);
  cached = withSystem (system: (packages system).cached);
}
