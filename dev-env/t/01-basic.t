PLAN 6

RUNS dade
OGREP "Usage"

NRUNS dade non-existent
OGREP "Unrecognized command"

RUNS dade assist
RUNS egrep -q -E "export NIX_PATH=nixpkgs=.*/nixpkgs-snapshot" $OSHT_STDOUT

# export NIX_CONF_DIR="/Users/gleber/code/da/dev-env/etc"
# export NIX_PATH=nixpkgs="/Users/gleber/code/da/dev-env/var/gc-roots/nixpkgs-snapshot"
# export PATH="/Users/gleber/code/da/dev-env/bin:$PATH"
# export PYTHONPATH=".:$PYTHONPATH"
# export DADE_REPO_ROOT=/Users/gleber/code/da
# export DADE_VAR_DIR=/Users/gleber/code/da/dev-env/var
# export DADE_DEVENV_DIR=/Users/gleber/code/da/dev-env
# export LC_ALL=en_US.UTF-8
# export LANG=en_US.UTF-8
