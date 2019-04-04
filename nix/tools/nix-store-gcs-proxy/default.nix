{ buildGoPackage }:
buildGoPackage {
  name = "nix-store-gcs-proxy";
  goPackagePath = "github.com/DACH-NY/daml/nix/tools/nix-store-gcs-proxy";
  src = ./.;
  goDeps = ./deps.nix;
}
