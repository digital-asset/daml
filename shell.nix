{
  pkgs ? import ./nix/nixpkgs.nix { },
  x ? import ./nix/default.nix { inherit pkgs; },
}:
pkgs.mkShell {
  buildInputs = pkgs.lib.attrsets.mapAttrsToList (name: value: value) x.toolAttrs;
}
