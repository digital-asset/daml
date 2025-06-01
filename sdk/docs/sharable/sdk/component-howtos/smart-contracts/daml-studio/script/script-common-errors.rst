Common Script Errors
********************

During Daml execution, errors can occur due to exceptions (e.g. use of "abort", or division by zero), or
due to authorization failures. You can expect to run into the following errors when writing Daml.

When a runtime error occurs in a script execution, the script result view shows the error
together with the following additional information, if available:

Location of the failed commit
   If the failing part of the script was a ``submitCmd``, the source location
   of the call to ``submitCmd`` will be displayed.
Stack trace
   A list of source locations that were encountered before the error occurred. The last encountered
   location is the first entry in the list.
Ledger time
   The ledger time at which the error occurred.
Partial transaction
   The transaction that is being constructed, but not yet committed to the ledger.
Committed transaction
   Transactions that were successfully committed to the ledger prior to the error.
Trace
   Any messages produced by calls to ``trace`` and ``debug``.

Abort, Assert, and Debug
========================

The ``abort``, ``assert`` and ``debug`` inbuilt functions can be used in updates and scripts. All three can be used to output messages, but ``abort`` and ``assert`` can additionally halt the execution:

.. literalinclude:: ../daml/Abort.daml
  :language: daml
  :start-after: -- BEGIN_ABORT_EXAMPLE
  :end-before: -- END_ABORT_EXAMPLE

.. code-block:: none

    Script execution failed:
      Unhandled exception:  DA.Exception.GeneralError:GeneralError with
                              message = "stop"

    Ledger time: 1970-01-01T00:00:00Z

    Trace:
      "hello, world!"


Missing Authorization on Create
===============================

If a contract is being created without approval from all authorizing
parties the commit will fail. For example:

.. literalinclude:: ../daml/CreateAuthFailure.daml
  :language: daml
  :start-after: -- BEGIN_MISSING_AUTH_EXAMPLE
  :end-before: -- END_MISSING_AUTH_EXAMPLE

Execution of the example script fails due to 'Bob' being a signatory
in the contract, but not authorizing the create:

.. code-block:: none

    Script execution failed:
      #0: create of CreateAuthFailure:Example at unknown source
          failed due to a missing authorization from 'Bob'

    Ledger time: 1970-01-01T00:00:00Z

    Partial transaction:
      Sub-transactions:
         #0
         └─> Alice creates CreateAuthFailure:Example
             with
               party1 = 'Alice'; party2 = 'Bob'

To create the "Example" contract one would need to bring both parties to
authorize the creation via a choice, for example 'Alice' could create a contract
giving 'Bob' the choice to create the 'Example' contract.


Missing Authorization on Exercise
=================================

Similarly to creates, exercises can also fail due to missing authorizations when a
party that is not a controller of a choice exercises it.


.. literalinclude:: ../daml/ExerciseAuthFailure.daml
  :language: daml
  :start-after: -- BEGIN_MISSING_AUTHORIZATION_EXAMPLE
  :end-before: -- END_MISSING_AUTHORIZATION_EXAMPLE

The execution of the example script fails when 'Bob' tries to exercise the
choice 'Consume' of which he is not a controller

.. code-block:: none

    Script execution failed:
      #1: exercise of Consume in ExerciseAuthFailure:Example at unknown source
          failed due to a missing authorization from 'Alice'

    Ledger time: 1970-01-01T00:00:00Z

    Partial transaction:
      Failed exercise:
        exercises Consume on #0:0 (ExerciseAuthFailure:Example)
        with
      Sub-transactions:
        0
        └─> 'Alice' exercises Consume on #0:0 (ExerciseAuthFailure:Example)
                    with

    Committed transactions:
      TX #0 1970-01-01T00:00:00Z (unknown source)
      #0:0
      │   disclosed to (since): 'Alice' (#0), 'Bob' (#0)
      └─> 'Alice' creates ExerciseAuthFailure:Example
                  with
                    owner = 'Alice'; friend = 'Bob'

From the error we can see that the parties authorizing the exercise ('Bob')
is not a subset of the required controlling parties.

Contract Not Visible
====================

Contract not being visible is another common error that can occur when a contract
that is being fetched or exercised has not been disclosed to the committing party.
For example:

.. literalinclude:: ../daml/NotVisibleFailure.daml
  :language: daml
  :start-after: -- BEGIN_NOT_VISIBLE_EXAMPLE
  :end-before: -- END_NOT_VISIBLE_EXAMPLE

In the above script the 'Example' contract is created by 'Alice' and makes no mention of
the party 'Bob' and hence does not cause the contract to be disclosed to 'Bob'. When 'Bob' tries
to exercise the contract the following error would occur:

.. code-block:: none

    Script execution failed:
      Attempt to fetch or exercise a contract not visible to the reading parties.
      Contract:  #0:0 (NotVisibleFailure:Example)
      actAs: 'Bob'
      readAs:
      Disclosed to: 'Alice'

    Ledger time: 1970-01-01T00:00:00Z

    Partial transaction:

    Committed transactions:
      TX #0 1970-01-01T00:00:00Z (unknown source)
      #0:0
      │   disclosed to (since): 'Alice' (#0)
      └─> 'Alice' creates NotVisibleFailure:Example
                  with
                    owner = 'Alice'

To fix this issue the party 'Bob' should be made a controlling party in one of the choices.
