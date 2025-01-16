# use `nix run .#redis` to start redis server
{
  description = "Redis dev env with Nix";
  # Flake inputs
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
  };
  # Flake outputs
  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in {
        devShell = pkgs.mkShell {
          # Specify tools you want to use
          buildInputs = [
            pkgs.redis
          ];
          # Shell hook to install requirements automatically
          shellHook = ''
            echo "Dev shell with redis..."
            '';
        };
        # add redis in apps
        apps = {
          redis = {
            type = "app";
            program = "${pkgs.redis}/bin/redis-server";
          };
        };

        # packages = {
        #   redis = pkgs.redis;
        # };
      });
}
