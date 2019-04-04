# dade-nix-install

This tool installs DA-built Nix distribution for MacOS X or Linux. It downloads
the release from:
- http://hydra.da-int.net/jobset/nix/release-3

Runtime prerequisites:
- `uname`, `mktemp`, `tar`, `curl`, `awk` to download and perform installation
- `sudo` needed if and only if `/nix/store` is not present or is not owned by
  the current user.

Notes:
- the script checks for High Sierra induced failure of Nix and would reinstall
  Nix installation automatically in this case.

# dade-raw-preload

This tool is meant to be run from a root account and precache all relevant
dev-env provided tools on a given machine. It finds the right user to
impersonate and downloads most recently built tools from `hydra.da-int.net`.

Runtime prerequisites:
- Nix installed in a single-user mode (e.g. via `dade-nix-install`)
- `uname` and `awk` to detect OS and architecture
- `sudo` to perform the prefetch from under the owner of the `/nix/store`

It is used to preload dev-env caches on developers workstations. A developer can
create a file which would disable the automatic precaching:

    touch $HOME/.dade-raw-preload.skip

Note: downloaded tools are not added to Nix garbage collection roots, hence will
get deleted with next `nix-collect-garbage` invocation.

Non-code dependencies:
- Hydra jobsets
  - http://hydra.da-int.net/jobset/da/master-dade-linux
  - http://hydra.da-int.net/jobset/da/master-dade-darwin
  - http://hydra.da-int.net/jobset/da/dev-env-next-dade-linux
  - http://hydra.da-int.net/jobset/da/dev-env-next-dade-darwin

Used by:
- Casper policy to deploy jobs (owned by Edward Newman)
-- based of https://github.com/DACH-NY/da/blob/master/dev-env/bin/download-dade-service-script.sh

Tested by:
- https://github.com/DACH-NY/da/pipeline/jenkins/src/jobs/pipeline/dev-env/dadeRawPreload.Jenkinsjob
- http://ci.da-int.net/job/pipeline/job/dev-env/job/dade-raw-preload/

Implementation sketch:
- finds out the Nix store owner;
- checks for the skip file;
- finds the user's Nix profile and sources it;
- creates a temporary nix.conf to ensure `hydra.da-int.net` is used;
- sets up a temporary nix-shell with required tools (e.g. jq);
- fetches all store paths from all last evaluations of all jobsets;
- downloads them (aka "realizes" with `nix-store -r`).

# Wrapped tools

Once the dev-env is set up (either through direnv (recommended), or through
manually sourcing one of `dev-env/profile_{z,ba}sh.sh`), the shell's `PATH`
will look through `dev-env/bin` first for its executables. The executables in
there are mostly scripts that redirect the call to the corresponding nix-built
executable.  However, having that script gives us an opportunity to wrap some
of those commands in special ways; this section is meant to document in what
ways the commands available in `dev-env/bin` have been modified from their
vanilla variants.
