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
    yq
  ];

shellHook = ''
    export BAZEL_COMPLETION_PATH="${pkgs.bazel_7}/share/zsh/site-functions"
'';
}
