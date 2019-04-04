DAML Language Server integration tests
======================================

This package implements the integration tests of the DAML language server
and the Visual Studio Code language server client.

Running tests when developing:

``
$ make test
``

If the tests fail you can reset the expected results
with 'tests/reset.sh':

``
$ cd tests
tests$ ./reset.sh
``

Be sure to double-check that the results are correct before
committing.

