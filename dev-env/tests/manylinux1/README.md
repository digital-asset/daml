# Check that Python conforms to manylinux1

The tests here check that the `python` binary produced by our Nix
expression conforms to the [manylinux1] platform tag from PEP-513.

The tests will only work on Linux since that is the only platform
supported by the manylinux1 standard.

* Run `make test-loading` to run a simple Python script which will
  attempt to load all required system libraries. Diagnostics is
  printed as the script runs and the exit code tells you the number of
  libraries that failed to load (if any).

* Run `make test-extension` on a machine with Docker to build a simple
  Python project with a C extension that depends on the `glib-2.0`
  library. The extension is compiled in the official `manylinux1`
  Docker container (which comes with all manylinux1 libraries) and
  afterwards we try loading the extension on the host system where Nix
  will provide the library.

* Run `make test-pip` to check that Pip will install manylinux1 wheels
  on your current platform. This tests that our patched patched Python
  works like a normal Linux Python.

[manylinux1]: https://www.python.org/dev/peps/pep-0513/#the-manylinux1-policy
