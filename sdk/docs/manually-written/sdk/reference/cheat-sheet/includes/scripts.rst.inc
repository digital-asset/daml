Scripts
-------

Daml script is a scripting language to run Daml commands against a ledger. For example:

.. code-block::

    module Test where

    import Daml.Script

    test : Script ()
    test = do
      alice <- allocateParty "Alice"
      bob <- allocateParty "Bob"
      c <- submit alice $ createCmd NewContract with ...
      submit bob $ exerciseCmd c Accept with ...

Scripts are compiled like usual Daml code to a ``dar`` package with the ``daml build`` command.

.. list-table::

   * - Running a script
     - ``daml script --dar example-0.0.1.dar --script-name ModuleName:scriptFunction --ledger-host localhost --ledger-port 6865``
   * - Running a script with initial arguments given
     - ``daml script --dar example-0.0.1.dar --input-file arguments_in_damllf_json.json --script-name ModuleName:scriptFunction --ledger-host localhost --ledger-port 6865``
   * - Allocating a party on the ledger
     - ``alice <- allocateParty "Alice"``
   * - List all known parties on the ledger
     - ``parties <- listKnownParties``
   * - Query for a given contract template visible to a given party
     - ``query @ExampleTemplate alice``
   * - Create a new contract
     - ``createCmd ExampleTemplate with ...``
   * - Exercise a choice on a contract
     - ``exerciseCmd contractId ChoiceName with ...``
   * - Exercise a choice on a contract by contract key
     - ``exerciseByKeyCmd contractKey ChoiceName with ...``
   * - Create and then exercise a choice on the created contract
     - ``createAndExerciseCmd (ExampleTemplate with ... ) (ChoiceName with ...)``
   * - Pass time on the ledger (only applicable for a ledger running in STATIC TIME MODE, like the in-memory ledger of Daml Studio or ``daml test``)
     - ``passTime (hours 10)``
   * - Set time on the ledger (only applicable for a ledger running in STATIC TIME MODE, like the in-memory ledger of Daml Studio or ``daml test``)
     - ``setTime (time (date 2007 Apr 5) 14 30 05)``
