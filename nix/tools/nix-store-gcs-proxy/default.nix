{ buildGoPackage }:
buildGoPackage {
  name = "nix-store-gcs-proxy";
  goPackagePath = "github.com/digital-asset/daml/nix/tools/nix-store-gcs-proxy";
  src = ./.;
  goDeps = ./deps.nix;
}
