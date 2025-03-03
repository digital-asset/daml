{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    assistant.url = "github:DACH-NY/daml-assistant";
  };

  outputs = { self, nixpkgs, assistant }: {
    devShells = nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed (system:
      let
        pkgs = import nixpkgs { inherit system; };
        assistantBin = assistant.packages.${system}.assistant;
      in {
        default = pkgs.mkShell {
          packages = [ assistantBin ];
        };
      }
    );
  };
}
