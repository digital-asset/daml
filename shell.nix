{ pkgs ? import ./nix/nixpkgs.nix { }
, default ? import ./nix/default.nix { inherit pkgs; }
}:
pkgs.mkShell {
  buildInputs = pkgs.lib.attrsets.mapAttrsToList (name: value: value) default.toolAttrs;
}
