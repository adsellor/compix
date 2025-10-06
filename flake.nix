{
  description = "compix";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    zig-overlay.url = "github:mitchellh/zig-overlay";

    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    flake-utils,
    ...
  }: let
    overlays = [
      (final: prev: {
        zigpkgs = inputs.zig-overlay.packages.${prev.system};
      })
    ];

    systems = builtins.attrNames inputs.zig-overlay.packages;
  in
    flake-utils.lib.eachSystem systems (
      system: let
        pkgs = import nixpkgs {inherit overlays system;};
        zig = inputs.zig-overlay.packages.${system}.default;
      in {
        devShells.default = pkgs.mkShell {
          packages = [
            zig
          ];
        };

        devShell = self.devShells.${system}.default;

      }
    );
}
