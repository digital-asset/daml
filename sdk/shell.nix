let
  pkgs = import ./nix/new-nixpkgs.nix;
in
pkgs.mkShell {
  buildInputs = with pkgs; [
    bash
    curl
    jq
    vale
    python3.pkgs.docutils
    sphinx
  ];
}
