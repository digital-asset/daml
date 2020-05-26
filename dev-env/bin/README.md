# dade-preload

This tool will force nix to build every derivation in the dev-env.

# Wrapped tools

Once the dev-env is set up (either through direnv (recommended), or through
manually sourcing one of `dev-env/profile_{z,ba}sh.sh`), the shell's `PATH`
will look through `dev-env/bin` first for its executables. The executables in
there are mostly scripts that redirect the call to the corresponding nix-built
executable.  However, having that script gives us an opportunity to wrap some
of those commands in special ways; this section is meant to document in what
ways the commands available in `dev-env/bin` have been modified from their
vanilla variants.
