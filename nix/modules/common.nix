{
  withSystem,
}:

{
  lib,
  pkgs,
}:

let
  settingsFormat = pkgs.formats.toml { };

  # TOML has no null: recursively drop null-valued keys (attrs and lists) so unset
  # options fall back to the Rust-side defaults (missing key == serde `Option::None`).
  stripNulls =
    value:
    if lib.isAttrs value then
      lib.filterAttrs (_: v: v != null) (lib.mapAttrs (_: stripNulls) value)
    else if lib.isList value then
      map stripNulls value
    else
      value;
in
{
  serviceOptions = {
    enable = lib.mkOption {
      type = lib.types.bool;
      description = "Whether to enable selector4nix";
      default = false;
      example = true;
    };

    package = lib.mkOption {
      type = lib.types.package;
      description = "The selector4nix package to use";
      default =
        pkgs.selector4nix
          or (withSystem pkgs.stdenv.hostPlatform.system ({ config, ... }: config.packages.selector4nix));
    };

    logLevel = lib.mkOption {
      type = lib.types.enum [
        "error"
        "warn"
        "info"
        "debug"
        "trace"
      ];
      description = "The verbosity of the logging output";
      default = "info";
      example = "debug";
    };

    settings = lib.mkOption {
      type = lib.types.submodule {
        freeformType = settingsFormat.type;
        options.server = {
          ip = lib.mkOption {
            type = lib.types.str;
            description = "The IP address that selector4nix listens on";
            default = "127.0.0.1";
            example = "127.0.0.1";
          };

          port = lib.mkOption {
            type = lib.types.port;
            description = "The port that selector4nix listens on";
            default = 5496;
            example = 5496;
          };
        };

        options.nar_disk_cache = {
          enabled = lib.mkOption {
            type = lib.types.bool;
            description = "Whether to cache NAR downloads on disk and resume interrupted ones from the watermark";
            default = false;
            example = true;
          };

          dir = lib.mkOption {
            type = lib.types.nullOr lib.types.str;
            description = "Absolute path of the cache directory (defaults to a subdirectory of the cache root when null)";
            default = null;
            example = "/var/cache/selector4nix/nar";
          };

          max_size_bytes = lib.mkOption {
            type = lib.types.nullOr lib.types.ints.unsigned;
            description = "Total size budget of cached partial NAR files";
            default = null;
            example = 10737418240;
          };

          partial_retention_secs = lib.mkOption {
            type = lib.types.nullOr lib.types.ints.unsigned;
            description = "How long an unfinished partial NAR survives without progress before eviction";
            default = null;
            example = 86400;
          };

          fsync_interval_bytes = lib.mkOption {
            type = lib.types.nullOr lib.types.ints.unsigned;
            description = "Checkpoint (fsync + meta update) interval in appended bytes";
            default = null;
            example = 16777216;
          };
        };
      };
      description = "The configuration that will be read by selector4nix";
      default = { };
      example = {
        server = {
          ip = "127.0.0.1";
        };
        substituters = [
          {
            url = "https://cache.nixos.org/";
          }
          {
            url = "https://mirrors.ustc.edu.cn/nix-channels/store/";
            priority = 45;
          }
          {
            url = "https://cache.garnix.io/";
            storage_url = "https://garnix-cache.com/";
          }
        ];
      };
    };

    credentialFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      description = "The credential file that will be read by selector4nix";
      default = null;
      example = "/path/to/your/credentials.toml";
    };

    enablePersistentCaching = lib.mkOption {
      type = lib.types.bool;
      description = "Whether to enable persistent caching";
      default = false;
      example = true;
    };

    configureSubstituter = lib.mkOption {
      type = lib.types.enum [
        "keep"
        "prepend"
        "overwrite"
      ];
      description = "Whether to configure the substituter list. by either prepending or rewriting";
      default = "keep";
      example = "overwrite";
    };
  };

  mkConfigFile =
    cfg:
    let
      rawConfigFile = settingsFormat.generate "selector4nix.raw.toml" (stripNulls cfg.settings);
    in
    pkgs.runCommand "selector4nix.toml" { } ''
      echo 'Checking the configuration file `selector4nix.toml` via `selector4nix check`'
      ${cfg.package}/bin/selector4nix --config-file "${rawConfigFile}" check && cp ${rawConfigFile} $out
    '';

  mkSubstituterConfig =
    cfg:
    lib.mkMerge [
      (lib.mkIf (cfg.enable && cfg.configureSubstituter == "prepend") {
        nix.settings.substituters = lib.mkBefore [
          "http://${cfg.settings.server.ip}:${toString cfg.settings.server.port}/"
        ];
      })

      (lib.mkIf (cfg.enable && cfg.configureSubstituter == "overwrite") {
        nix.settings.substituters = lib.mkForce [
          "http://${cfg.settings.server.ip}:${toString cfg.settings.server.port}/"
        ];
      })
    ];
}
