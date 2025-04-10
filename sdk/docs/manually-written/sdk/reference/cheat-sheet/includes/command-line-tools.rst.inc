Command line tools
------------------

.. list-table::

   * - Install the daml assistant
     - ``curl -sSL https://get.daml.com | sh -s <version>``
   * - Create a new Daml project
     - ``daml new <myproject>``
   * - Create a new Daml/React full stack project
     - ``daml new <myproject> --template create-daml-app``
   * - Start the IDE
     - ``daml studio``
   * - Build project
     - ``daml build``
   * - Build project, start the sandbox and JSON-API
     - ``daml start``
   * - Start the sandbox ledger (in wall-clock time-mode)
     - ``daml sandbox``
   * - Start the sandbox ledger (in static time-mode)
     - ``daml sandbox --static-time``
   * - Start the JSON-API server (requires a running ledger)
     - ``daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575``
   * - Upload a dar to the ledger
     - ``daml ledger upload-dar <dar file>``
   * - Run all test scripts and output test coverage report
     - ``daml test --show-coverage --all --files Test.daml``
   * - Project configuration file
     - ``daml.yaml``
