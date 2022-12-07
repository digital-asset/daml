{ pkgs ? import ./nix/nixpkgs.nix { }
, default ? import ./nix/default.nix { inherit pkgs; }
}:
pkgs.mkShell {
  buildInputs = pkgs.lib.attrsets.mapAttrsToList (name: value: value) default.toolAttrs;

  shellHook = ''
    # install pre-commit hook (opt-out by setting `DADE_NO_PRE_COMMIT`)
    [ -v DADE_NO_PRE_COMMIT ] || pre-commit install
  '';
}
