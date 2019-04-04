{ callPackage, lib, ... }:

lib.overrideDerivation (callPackage ./generic-v3.nix {
  version = "3.5.1";
  sha256 = "17cwwp2ja8rv7nrvaxrxsdb4a2f5gg7zdx85qn2vb92az1fc2lzn";
}) (attrs: { NIX_CFLAGS_COMPILE = "-Wno-error"; })
