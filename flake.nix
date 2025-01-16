{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay, ... }:
    let
      # Define the supported systems
      systems = [ "x86_64-darwin" "aarch64-darwin" ];
      
      # Helper function to create system-specific outputs
      forAllSystems = nixpkgs.lib.genAttrs systems;
      
      # Create pkgs for each system
      pkgsForSystem = system: import nixpkgs {
        inherit system;
        overlays = [ rust-overlay.overlays.default ];
      };
    in
    {
      devShells = forAllSystems (system: 
        let 
          pkgs = pkgsForSystem system;
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" ];
          };
        in
        {
          default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              rustToolchain
              binaryen
              flyctl
              openssl
              protobuf_21
            ] ++ (if pkgs.stdenv.isDarwin then [
              darwin.apple_sdk.frameworks.Foundation
              pkgs.darwin.libiconv
            ] else []);

            shellHook = ''
              export PATH=$PATH:$HOME/.cargo/bin
              export RUST_SRC_PATH=${rustToolchain}/lib/rustlib/src/rust/library
            '';
          };
        }
      );
    };
}