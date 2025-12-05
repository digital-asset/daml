Command line tools
------------------

.. list-table::

   * - Install DPM
     - ``curl -sSL https://get.digitalasset.com/install/install.sh | sh``
   * - Create a new Daml project
     - ``dpm new <myproject>``
   * - Start the IDE
     - ``dpm studio``
   * - Build project
     - ``dpm build``
   * - Start the sandbox ledger (in wall-clock time-mode)
     - ``dpm sandbox``
   * - Start the sandbox ledger (in static time-mode)
     - ``dpm sandbox --static-time``
   * - Run all test scripts and output test coverage report
     - ``dpm test --show-coverage --all --files Test.daml``
   * - Project configuration file
     - ``daml.yaml``
