.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Contract ID allocation schemes
==============================


Goals
^^^^^

* Allows ordering contract IDs and make Daml semantics depend on this
  order, e.g., for comparison builtin and maps using IDs as keys.
* Allows ledgers to store information about the contract and the
  creating transaction in the contract ID if necessary.


Requirements
^^^^^^^^^^^^

**Executability**: Daml engine can determine ordering of the contract
IDs before it hands out the transaction to the write service.

**Validation**: The allocation scheme commutes with transaction
projection. That is, the contract IDs for contracts created in the
projection of a transaction to a set of parties can be computed
solely from the projection and input seeds.

**Unlinkability**: It is computationally infeasible to link the
contract contents to the contract ID unless the create node is
witnessed or the input seeds are known. The contract contents include
the contract instance, the template ID, the stakeholders / signatories
/ maintainers, and the contract key.

**Freshness**: A contract ID created in a transaction has a high
probability of being fresh w.r.t. all other other contract IDs
that appeared in interpretation of the same transaction, where the
probability is taken over the seeds.

**Distinctness**: The ledger can enforce that the seeds are pairwise
different.


General principles
^^^^^^^^^^^^^^^^^^

Kinds of Contract IDs
---------------------

In a transaction we distinguish two kinds of contract IDs:

* The *local* contract IDs are the IDs of the contracts created by the
  transaction.

* The *global* contract IDs are the contract IDs that:

   * appear in the commands that produced the transaction. This
     includes the IDs of the exercised contract, together with all the
     IDs referenced in the arguments of the create and exercise
     commands;
   * that are fetched or looked up by key unless they are local;
   * are referenced in the input contracts.

Note that local contract IDs correspond to the IDs of output contracts
together with those contracts that have been both created and archived
in the transaction. On the other hand, global contract IDs do not only
reference IDs of some contracts that have been previously persisted on
the ledger, but also any arbitrary contract IDs that the submitter has
referenced in its submission.

When we focus on a subtransaction of a transaction, some schemes
distinguish three kinds of contract IDs:

* The *local* contract IDs are the IDs of the contracts created in the
  subtransaction of interest.

* The *relative* contract IDs are the IDs of contracts created in the
  same transactions, but not in the subtransaction.

* The *absolute* contract IDs are the global contract IDs of the
  transaction.


Contract ID uniqueness
----------------------

The ledger implementation transforms a local contract ID into a global,
relative, or absolute contract ID by adding a non-empty suffix.
For global/absolute contract IDs, this suffix must enforce global
uniqueness of all contract IDs referenced by the ledger, including IDs
of transient contracts. For relative contract IDs, the suffix must
enforce uniqueness of all contract IDs referenced in the transaction.


Contract ID Comparability
-------------------------

The so-called *contract ID comparability restriction* forbids two kinds
of comparisons:

 * A local contract ID must not be compared with a global/relative/absolute
   contract ID with the same prefix.

 * If the scheme uses relative contract IDs, a relative contract ID must
   not be compared with a relative or absolute contract ID with the same
   prefix and different suffix.

Any attempt to compare such IDs during interpretation, either explicitly
(using Generic equality and order builtins) or implicitly (through ordering of
keys in generic maps), must be rejected.

This ensures that only the prefixes, not the suffix are needed
to compare local and relative contract IDs with other contract IDs.
For global / absolute contract IDs, no restrictions apply: different global
contract IDs may have the same prefix, e.g., if they were created in two
different transactions.

Submission time
---------------

The submission time is used to derive the transaction seed and the local
contract IDs. In practice, it should be close to the original time at which the
submission was initiated. No particular requirement is made in this
specification. However a particular ledger implementation can require
this time to be within a particular time window around the commit
time. This allows, for instance, the committer of a central committer
ledger to enforce global uniqueness of the pair (submission seed,
submission time).


Node seeds
----------

From an initial seed, called in the following *submission seed*, seeds
for all Create and Exercise nodes in the transaction tree are derived as they are
computed.

We propose the following scheme, inspired by NIST’s HMAC_DRBG
construction. It assumes a hash-based MAC function HMAC, and defines
for each big-endian 32 bits integer ``n`` the derived seed:

  deriveSeed(seed, n) := HMAC(seed, n)

Submission seed
^^^^^^^^^^^^^^^

Any random number with sufficient entropy works as submission seed
``submission_seed`` (256 bit entropy).


Transaction seed construction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

From the submission, a so-called *transaction seed* is derived as follows::

  deriveTransactionSeed(submissionSeed, participantId, submissionTime) :=
     HMAC(submissionSeed, participantId ∥ submissionTime)

where

* ``submissionSeed`` is the submission seed;
* ``participantId`` is US-ASCII encoding of the participant ID
  prefixed with its size encoded as a 32 bits big-endian integer;
* ``submissionTime`` is the submission time in micro second encoded as
  a 64 bytes big-endian integer with UNIX epoch as the origin;


Derivation of seeds for root nodes of the transaction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For each root node of the transaction, a root seed `rootSeedᵢ` is
computed from the initial seed as follows::

  rootSeedᵢ = deriveSeed(transactionSeed, i)

where

* ``i`` is the 0-based index of the root node as a 64 bytes big-endian
  integer;


Derivation of seeds for the children of exercise nodes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For an exercise node with seed ``nodeSeed``, the seeds `childSeedᵢ` for the
children are derived as follows::

  childSeedᵢ = deriveSeed(nodeSeed, i)

where

* ``i`` is the 0-based index of the child node as a 64 bytes big-endian
  integer;


Contract ID Scheme V1
^^^^^^^^^^^^^^^^^^^^^

A *contract identifier* of version V1 (or contract ID V1 for short) is
a sequence of bytes defined as follows ::

  ContractID := versionPrefix ∥ discriminator ∥ suffix

where

* ``∥`` is the concatenation operation; 
* ``versionPrefix`` is 1 byte (equal to `0x00`) used to identify the
  contract ID V1 scheme;
* ``discriminator`` is a sequence of 32 bytes. It is like a random
  UUID, but generated from the *node seed* via a fixed
  `derivation scheme <Derivation of contract ID discriminator_>`_
  that supports validation (see below).
* ``suffix`` is a sequence of 0 to 94 bytes used to enforce global
  uniqueness of the contract ID in a distributed ledger.

This scheme does not use relative contract IDs: they are treated like
global contract IDs.


Derivation of contract ID discriminator
---------------------------------------

The discriminator for the created contract ID V1 is given as follows::

  discriminator = HMAC(nodeSeed, submissionTime ∥ nₛ ∥ stakeholders) 

where

* ``nodeSeed`` is the seed of the node where the contract is created;
* ``submissionTime`` is the submission time in micro second encoded as
  a 64 bytes big-endian integer;
* ``nₛ`` is the number of stakeholder's of the contract encoded as a
  32 bits big-endian integer;
* ``stakeholders`` is the concatenation of the stakeholders IDs sorted
  lexicographically. IDs of stakeholder are interpreted as their
  US-ASCII encoding prefixed with there size encoded as a 32 bits
  big-endian integer.

Contract ID Scheme V2
^^^^^^^^^^^^^^^^^^^^^

A *contract identifier* of version V2 (or contract ID V2 for short) is
a sequence of bytes defined as follows ::

  ContractID := versionPrefix ∥ time prefix ∥ shortened seed ∥ suffix

where

* ``∥`` is the concatenation operation;
* ``versionPrefix`` is 1 byte (equal to `0x01`) used to identify the
  contract ID V2 scheme;
* ``time prefix``` is a sequence of 5 bytes that encodes the submission time
  with a resolution of 286981 microseconds;
* ``shortened seed`` consists of the first 7 bytes of the node seed;
* ``suffix`` is a sequence of 0 to 33 bytes used to enforce global
  uniqueness of the contract ID in a distributed ledger.

Time prefix
-----------

The time prefix is the 40-bit big-endian encoding of the value

  submission time in microseconds since 0001-01-01T00:00:00.000000Z / 286981

Note that 286981 microseconds is the finest resolution that ensures
that all valid submission times between 0001-01-01T00:00:00.000000Z and
9999-12-31T23:59:59.999999Z can be encoded in 40 bytes.

Relative and absolute suffixes
------------------------------

Relative contract IDs have the first bit of the suffix unset, i.e.,
the most significant bit of the first byte in the suffix is 0.
Absolute contract IDs have the first bit of the suffix set.


Submission
^^^^^^^^^^

The submission performs the following steps:

* Pick a submission seed with high entropy.
* Derive the transaction seed and start the interpretation
* During interpretation derive the node seeds, and the local contract IDs
  accordingly the schemes described above.  If the
  local contract ID is not `fresh <Contract ID Comparability_>`_, abort the
  interpretation. The submitter can restart the interpretation, which will pick
  another submission seed.
* If the transaction succeeds, the output is a *submitted transaction*


The ledger implementation suffixes the local contract IDs in a later step.
This yields the *committed transaction*.


Validation
^^^^^^^^^^

Reinterpretation for a full transaction validation takes the
transaction, the submission seed, and the submission time as
inputs. Transaction seed is derived in the same way as for
submission.

Reinterpretation for a partial transaction validation takes the
partial transaction, the seeds of the partial transaction root nodes,
and the submission time as inputs.

In both cases when a contract ID must be allocated, the local contract ID
is computed and checked for freshness in the same way as for
submission. The resulting transactions are then compared with the
original ones ignoring the suffix of the local contract IDs.



.. Local Variables:
.. eval: (flyspell-mode 1)
.. eval: (set-input-method "TeX")
.. End:

..  LocalWords:  subactions lexicographically endian Executability
..  LocalWords:  Unlinkability
