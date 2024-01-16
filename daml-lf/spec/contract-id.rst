.. Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

V1 Contract ID allocation scheme
================================


Goals
^^^^^

* Allows ordering contract IDs and make Daml semantics depend on this
  order, e.g., for comparison builtin and maps using IDs as keys.
* Eliminate all contract ID translations for central committer ledger
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

**Freshness**: It is computationally infeasible to find contracts C1
and C2 with the same contract ID such that one of the following holds:

* The contracts have different stakeholders.
* The contracts are created by different transactions.
* The contracts are created by different sub-actions of the same
  transaction forest where the seeds are pairwise different.

**Distinctness**: The ledger can enforce that the seeds are pairwise
different.


Contract Identifiers
^^^^^^^^^^^^^^^^^^^^

A *contract identifier* (or contract ID for short) is a sequence of
bytes defined as follows ::

  ContractID := versionPrefix ∥ discriminator ∥ suffix

where

* ``∥`` is the concatenation operation; 
* ``versionPrefix`` is 1 byte (equal to `0x00`) used to version the
  contract ID scheme;
* ``discriminator`` is a sequence of 32 bytes. It is like a random
  UUID, but generated from an initial seed (called *submission seed*)
  via a fixed `derivation scheme <Allocation scheme for
  discriminators_>`_ that supports validation (see below).
* ``suffix`` is a sequence of 0 to 94 bytes used to enforce global
  uniqueness of the contract ID in a distributed ledger.


Local/Global Contract IDs
-------------------------

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

Note that local contract IDs correspond to the ID of output contracts
together with those contracts that have been both created and archived
in the transaction. On the other hand, global contract IDs do not only
reference IDs of some contracts that have been previously persisted on
the ledger, but also any arbitrary contract IDs that the submitter has
referenced in its submission.


Contract ID uniqueness
----------------------

During interpretation local contract IDs are created without suffix.
Ledger implementations are responsible for enforcing global uniqueness
of all the contract IDs referenced by the ledger, including IDs of
transient contract. This can be done by enforcing global uniqueness of
the seeds or by appropriately suffixing the contract IDs.  No other
requirement (except the 94 bytes size limit) is assumed for those
suffixes.

The simplest approach consists to suffix all local contract IDs with a
uniquely global transaction ID. Alternatively central committer ledger
can completely avoid suffixing by enforcing that the pair (submission
seed, submission time) is not used by two different submission, as the
discriminator allocation scheme ensures in this case the uniqueness of
allocated discriminators.


Contract ID Comparability
-------------------------

The so-called *contract ID comparability restriction*, states that the
comparison of a local contract ID with a global contract ID with the
same discriminator is forbidden. Any attempt to compare such IDs
during interpretation, either explicitly (using Generic equality and
order builtins) or implicitly (through ordering of key in generic
maps), must be rejected. Ledger implementations that suffix contract
IDs should furthermore enforce that all global Contract IDs are
suffixed.

This ensures that only the discriminators, not the suffix are needed
to compare the contract IDs of local contract IDs w.r.t. global
contract IDs.


Submission time
---------------

The submission time is used to derive the transaction seed. In
practice, it should be close to the original time at which the
submission was initiated. No particular requirement is made in this
specification. However a particular ledger implementation can require
this time to be within a particular time window around the commit
time. This allows, for instance, the committer of a central committer
ledger to enforce global uniqueness of the pair (submission seed,
submission time).


Allocation scheme for discriminators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  
From an initial seed, called in the following *submission seed*, seeds
for all Create and Exercise nodes in the transaction tree are derived as they are
computed.

We propose the following scheme, inspired by NIST’s HMAC_DRBG
construction. It assumes a hash-based MAC function HMAC, and defines
for each big-endian 32 bits integer ``n`` the derived seed:

  deriveSeed(seed, n) := HMAC(seed, n)


Submission seed
---------------

Any random number with sufficient entropy works as submission seed
``submission_seed`` (256 bit entropy).


Transaction seed construction
-----------------------------

From the submission, a so-called *transaction seed* is derived as follows::

  deriveTransactionSeed(submissionSeed, participantId, submissionTime) :=
     HMAC(submissionSeed, participantId ∥ submissionTime)

where

* ``submissionSeed`` is the submission seed;
* ``participantId`` is US-ASCII encoding of the participant ID
  prefixed with its size encoded as a 32 bits big-endian integer;
* ``submissionTime`` is the submission time in micro second encoded as
  a 64 bytes big-endian integer;


Derivation of seeds for root nodes of the transaction
--------------------------------------------------

For each root node of the transaction, a root seed `rootSeedᵢ` is
computed from the initial seed as follows::

  rootSeedᵢ = deriveSeed(transactionSeed, i)

where

* ``i`` is the 0-based index of the root node as a 64 bytes big-endian
  integer;


Derivation of seeds for the children of exercise nodes
------------------------------------------------------

For an exercise node with seed ``nodeSeed``, the seeds `childSeedᵢ` for the
children are derived as follows::

  childSeedᵢ = deriveSeed(nodeSeed, i)

where

* ``i`` is the 0-based index of the child node as a 64 bytes big-endian
  integer;


Derivation of contract ID discriminator
---------------------------------------

The discriminator for the created contract ID is given as follows::

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


Submission
^^^^^^^^^^

The submission performs the following steps:

* Pick a submission seed with high entropy.
* Derive the transaction seed and start the interpretation
* During interpretation derive the node seeds, and the discriminator
  of local contract ID accordingly the scheme described above.  If the
  discriminator is not `fresh <Discriminator Freshness_>`_, abort the
  interpretation. The submitter can restart the interpretation, which will pick
  another submission seed.
* If the transaction succeeds, the output is a *submitted transaction*


Depending on the ledger implementation, the local contract IDs are
suffixed with a suffix in a later step. This yields the *committed
transaction*.

For ledgers that do not require suffixing, committed and
submitted transactions coincide. Committed transactions are the source
of truth to derive the state of the ledger.


Validation
^^^^^^^^^^

Reinterpretation for a full transaction validation takes the
transaction, the submission seed, and the submission time as
inputs. Transaction seed is derived in the same way as for
submission.

Reinterpretation for a partial transaction validation takes the
partial transaction, the seeds of the partial transaction root nodes,
and the submission time as inputs.

In both cases when a contract ID must be allocated, the discriminator
is computed and check for freshness in the same way as for
submission. The resulting transactions are then compared with the
original ones ignoring the suffix of the local contract IDs.



.. Local Variables:
.. eval: (flyspell-mode 1)
.. eval: (set-input-method "TeX")
.. End:

..  LocalWords:  subactions lexicographically endian Executability
..  LocalWords:  Unlinkability
