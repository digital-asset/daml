.. _da-model-validity:

Valid Ledgers
*************

.. note::
   Extend with time monotonicity requirement

At the core is the concept of a *valid ledger*; changes
are permissible if adding the corresponding commit to the
ledger results in a valid ledger. **Valid ledgers** are
those that fulfill three conditions:

:ref:`da-model-consistency`
   Exercises and fetches on inactive contracts are not allowed, i.e.
   contracts that have not yet been created or have already been
   consumed by an exercise.
   A contract with a contract key can be created only if the key is not associated to another unconsumed contract,
   and all key assertions hold.
:ref:`da-model-conformance`
   Only a restricted set of actions is allowed on a given contract.
:ref:`da-model-authorization`
   The parties who may request a particular change are restricted.

Only the last of these conditions depends on the party (or
parties) requesting the change; the other two are general.
