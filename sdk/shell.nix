let
  pkgs = import ./nix/new-nixpkgs.nix;
in
pkgs.mkShell {
  buildInputs = with pkgs; [
    bash
    curl
    jq
  ];
}
