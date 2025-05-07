{
  description = "Apache Doris flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        
        # Select JDK (not JRE)
        jdk = pkgs.jdk8;

        # Runtime dependencies
        dorisRuntimeDeps = with pkgs; [
          jdk
          which
          coreutils
          gawk
          nettools
          procps
          curl
        ];

        # Fetch Doris tarball
        dorisTarball = pkgs.fetchurl {
          url = "https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-3.0.5-bin-x64.tar.gz";
          sha256 = "sha256-4d18YOeiCIE7gLts2IGbBr8tykBjIrUkQtWz1LPdPic=";
        };

        doris = pkgs.stdenv.mkDerivation {
          pname = "doris";
          version = "3.0.5";
          src = dorisTarball;

          nativeBuildInputs = [ pkgs.makeWrapper ];
          
          installPhase = ''
            mkdir -p $out/bin $out/share/doris
            tar -xzf $src --strip-components=1 -C $out/share/doris
            
            # Create wrapper scripts for FE and BE with proper JAVA_HOME
            makeWrapper $out/share/doris/fe/bin/start_fe.sh $out/bin/start_fe \
              --prefix PATH : ${pkgs.lib.makeBinPath dorisRuntimeDeps} \
              --set JAVA_HOME ${jdk} \
              --set DORIS_HOME $out/share/doris
              
            makeWrapper $out/share/doris/fe/bin/stop_fe.sh $out/bin/stop_fe \
              --prefix PATH : ${pkgs.lib.makeBinPath dorisRuntimeDeps} \
              --set JAVA_HOME ${jdk} \
              --set DORIS_HOME $out/share/doris
              
            makeWrapper $out/share/doris/be/bin/start_be.sh $out/bin/start_be \
              --prefix PATH : ${pkgs.lib.makeBinPath dorisRuntimeDeps} \
              --set JAVA_HOME ${jdk} \
              --set DORIS_HOME $out/share/doris
              
            makeWrapper $out/share/doris/be/bin/stop_be.sh $out/bin/stop_be \
              --prefix PATH : ${pkgs.lib.makeBinPath dorisRuntimeDeps} \
              --set JAVA_HOME ${jdk} \
              --set DORIS_HOME $out/share/doris
          '';
          
          # Create data directories and configuration
          postInstall = ''
            # Create setup script for initial configuration
            cat > $out/bin/doris-setup << EOF
            #!/bin/sh
            mkdir -p \$HOME/.doris/fe/meta \$HOME/.doris/be/storage
            
            # Create customized config files
            mkdir -p \$HOME/.doris/fe/conf \$HOME/.doris/be/conf
            
            # FE configuration
            if [ ! -f \$HOME/.doris/fe/conf/fe.conf ]; then
              cp $out/share/doris/fe/conf/fe.conf \$HOME/.doris/fe/conf/
              # Set meta dir path
              sed -i 's|# meta_dir.*|meta_dir = \$HOME/.doris/fe/meta|' \$HOME/.doris/fe/conf/fe.conf
            fi
            
            # BE configuration
            if [ ! -f \$HOME/.doris/be/conf/be.conf ]; then
              cp $out/share/doris/be/conf/be.conf \$HOME/.doris/be/conf/
              # Set storage path and JAVA_HOME
              sed -i 's|# storage_root_path.*|storage_root_path = \$HOME/.doris/be/storage|' \$HOME/.doris/be/conf/be.conf
              echo "JAVA_HOME=${jdk}" >> \$HOME/.doris/be/conf/be.conf
            fi
            
            echo "Doris setup complete. Data will be stored in \$HOME/.doris/"
            EOF
            chmod +x $out/bin/doris-setup
            
            # Add environment variables to wrapper scripts
            for script in start_fe stop_fe; do
              substituteInPlace $out/bin/$script \
                --replace "DORIS_HOME=$out/share/doris" \
                "DORIS_HOME=$out/share/doris\\nif [ -d \$HOME/.doris/fe/conf ]; then\\n  export DORIS_FE_CONF=\$HOME/.doris/fe/conf\\nfi"
            done
            
            for script in start_be stop_be; do
              substituteInPlace $out/bin/$script \
                --replace "DORIS_HOME=$out/share/doris" \
                "DORIS_HOME=$out/share/doris\\nif [ -d \$HOME/.doris/be/conf ]; then\\n  export DORIS_BE_CONF=\$HOME/.doris/be/conf\\nfi"
            done
          '';
        };
      in {
        packages.default = doris;

        devShells.default = pkgs.mkShell {
          buildInputs = dorisRuntimeDeps ++ [ doris ];
          shellHook = ''
            # Set JAVA_HOME directly in the shell
            export JAVA_HOME=${jdk}
            
            # Run setup script if it's the first time
            if [ ! -d "$HOME/.doris" ]; then
              doris-setup
            fi
            
            echo "Apache Doris development environment activated"
            echo ""
            echo "JAVA_HOME is set to: $JAVA_HOME"
            echo ""
            echo "To start Doris FE (Frontend): start_fe"
            echo "To stop Doris FE: stop_fe"
            echo "To start Doris BE (Backend): start_be"
            echo "To stop Doris BE: stop_be"
            echo ""
            echo "Data is stored in $HOME/.doris/"
            echo "Configuration files:"
            echo "  FE: $HOME/.doris/fe/conf/fe.conf"
            echo "  BE: $HOME/.doris/be/conf/be.conf"
            echo ""
            echo "Doris FE web UI: http://localhost:8030"
            echo ""
          '';
        };

        apps = {
          start-fe = {
            type = "app";
            program = "${doris}/bin/start_fe";
          };
          stop-fe = {
            type = "app";
            program = "${doris}/bin/stop_fe";
          };
          start-be = {
            type = "app";
            program = "${doris}/bin/start_be";
          };
          stop-be = {
            type = "app";
            program = "${doris}/bin/stop_be";
          };
          setup = {
            type = "app";
            program = "${doris}/bin/doris-setup";
          };
          default = self.apps.${system}.setup;
        };
      }
    );
}