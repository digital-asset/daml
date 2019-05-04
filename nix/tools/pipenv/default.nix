# Install Pipenv as a Python virtualenv that lives directly in
# /nix/store. Using the normal native Nix packaging results in simple
# things like this failing:
#
#   $ pipenv lock
#   Creating a virtualenv for this project...
#   Pipfile: /Users/mg/tmp/Pipfile
#   Using
#   /nix/store/cj6q7rmibddj6m58qmcf0xmvrjf9v24c-python3-3.6.4/bin/python3.6m
#   (3.6.4) to create virtualenv...
#   /nix/store/cj6q7rmibddj6m58qmcf0xmvrjf9v24c-python3-3.6.4/bin/python3.6m:
#   Error while finding module specification for 'pipenv.pew'
#   (ModuleNotFoundError: No module named 'pipenv')
#
# This seems to be due to how Pipenv tries finds the interpreter and
# how Nix doesn't actually create proper virtual environments when you
# use pkgs.python36.withPackages.
#
# Please see the commits in https://github.com/DACH-NY/da/pull/15231.
{ stdenv, python3, python3Packages }:
stdenv.mkDerivation rec {
  name = "${pname}-${version}";
  pname = "pipenv";
  version = "2018.11.14";
  srcs = import ./srcs.nix { inherit python3Packages; };
  buildInputs = [ python3 ];
  phases = "installPhase fixupPhase";
  installPhase = ''
    # Pip needs a directory with the the original wheel names. The Nix
    # helper function stripHash can give us that.
    for src in $srcs; do
      ln -s $src $(stripHash $src)
    done
    python3 -m venv $out
    HOME=$TMP $out/bin/pip3 install --no-index --find-links . pipenv
  '';

  fixupPhase = ''
    patch -d $out -p1 < ${./pip-no-input.patch}
  '';
}
