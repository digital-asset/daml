.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Copyright © 2020, `Digital Asset (Switzerland) GmbH
<https://www.digitalasset.com/>`_ and/or its affiliates.  All rights
reserved.


Contract ID V1 allocation scheme
================================

Contract IDs are may of two parts.

* A *discriminator* is like a random UUID, but generated from an
  initial seed using a deterministic random bit generator (DRBG) via a
  fixed derivation scheme that supports validation (see below).
* To guard against the submitter choosing low-entropy initial seeds
  for the discriminators, Contract IDs are formed by extending the
  discriminators with a *suffix*.  The suffix depends on the
  particular ledger and ensures that contract IDs are universally
  unique even with a malicious submitter who controls the seed.




  
During interpretation for submission, keep track of all seen discriminators. They can appear in the following places:




* 32 bytes for the discriminator
* upto 96 bytes for the so-called suffix.






Replace relative contract IDs in the transaction tree by discriminators. A discriminator is like a random UUID, but generated from an initial seed using a deterministic random bit generator (DRBG) via a fixed derivation scheme that supports validation (see below).
To guard against the submitter choosing low-entropy initial seeds for the discriminators, absolute contract IDs are formed by extending the discriminators with a transaction ID. The transaction ID depends on the particular ledger and ensures that contract IDs are universally unique even with a malicious submitter who controls the seed.
During interpretation for submission, keep track of all seen discriminators. They can appear in the following places:
The discriminators of created contracts
The IDs of an input contract
In the command
In template arguments of an input contract
If the discriminators of created contracts are not disjoint from the others, abort the interpretation. The submitter (e.g., the command service) can restart the interpretation with another seed.
This ensures that only the discriminators, not the transaction IDs are needed to order the contract IDs of created contract w.r.t. other contract IDs.
If interpretation succeeds, the output is a raw transaction.
Every ledger provides a function that takes a raw transaction, the commands, and metadata (ledger-effective time, submitter, command ID, application ID) and produces a transaction ID.
All discriminator-only contract IDs in the raw transaction are suffixed with the transaction ID. This yields the ready transaction. The finished transaction is then sent to the ledger on the write path, along with the transaction ID and the seed.
Re-interpretation during validation takes the transaction ID and the seed as inputs. When a contract ID must be allocated, the discriminator is computed in the same way, but the transaction ID is immediately added. Accordingly, no check for discriminator clashes is needed.


Goals
^^^^^

*


Transaction Seed

Submission Seed



Goals
^^^^^

* Do not leak information

Requirements
^^^^^^^^^^^^

* Executability: DAML engine can determine the contract IDs before it hands out the transaction to the write service.
Validation: The allocation scheme commutes with transaction projection. That is, the contract IDs for contracts created in the projection of a transaction to a set of parties can be computed solely from the projection and input seeds.
* Unlinkability: It is computationally infeasible to link the contract contents to the contract ID unless the create node is witnessed. The contract contents include the contract instance, the template ID, the stakeholders / signatories / maintainers, and the contract key.
* Freshness: It is computationally infeasible to find contracts C1 and C2 with the same contract ID such that one of the following holds:
* The contracts are instances of different templates
The contracts have different signatories, observers, or maintainers.
The contracts are created by different transactions.
The contracts are created by different subactions of the same transaction forest where the seeds are pairwise different.
Distinctness: The ledger can enforce that the seeds are pairwise different.

Suggested approach: Discriminators instead of indices
Replace relative contract IDs in the transaction tree by discriminators. A discriminator is like a random UUID, but generated from an initial seed using a deterministic random bit generator (DRBG) via a fixed derivation scheme that supports validation (see below).
To guard against the submitter choosing low-entropy initial seeds for the discriminators, absolute contract IDs are formed by extending the discriminators with a transaction ID. The transaction ID depends on the particular ledger and ensures that contract IDs are universally unique even with a malicious submitter who controls the seed.
During interpretation for submission, keep track of all seen discriminators. They can appear in the following places:
The discriminators of created contracts
The IDs of an input contract
In the command
In template arguments of an input contract
If the discriminators of created contracts are not disjoint from the others, abort the interpretation. The submitter (e.g., the command service) can restart the interpretation with another seed.
This ensures that only the discriminators, not the transaction IDs are needed to order the contract IDs of created contract w.r.t. other contract IDs.
If interpretation succeeds, the output is a raw transaction.
Every ledger provides a function that takes a raw transaction, the commands, and metadata (ledger-effective time, submitter, command ID, application ID) and produces a transaction ID.
All discriminator-only contract IDs in the raw transaction are suffixed with the transaction ID. This yields the ready transaction. The finished transaction is then sent to the ledger on the write path, along with the transaction ID and the seed.
Re-interpretation during validation takes the transaction ID and the seed as inputs. When a contract ID must be allocated, the discriminator is computed in the same way, but the transaction ID is immediately added. Accordingly, no check for discriminator clashes is needed.

Allocation scheme for discriminators
From an initial seed, seeds for all nodes in the transaction tree are derived as they are computed. We propose the following scheme, inspired by NIST’s HMAC_DRBG construction. It assumes a hash-based MAC function HMAC, and defines for each natural number n the derived seed
deriveSeed(seed, n) := HMAC(seed, n)
Initial seed construction:
Any random number with sufficient entropy works as initial seed init_seed (256 bit entropy)
Canton currently uses the HMAC with the submitting participant’s private HMAC key on the following pieces
submitting party
command Id
application Id
domain
ledger-effective time
I recommend a SHA256 HMAC of the following pieces
submitting party
command Id
application Id
submitting participant ID
a counter that is incremented if interpretation fails because of discriminator clashes
HMAC key: a random nonce (256 bit randomness) or the private key of the submitting participant
Derivation of seeds for root nodes of the transaction:
For each root node of the transaction, a root seed is computed from the initial seed as follows:
root_seed_i = deriveSeed(init_seed, i)
Derivation of seeds for the children of exercise nodes:
For an exercise node with seed seed, the seeds child_seed_i for the children are derived as follows:
child_seed_i = deriveSeed(seed, i)
Contract ID allocation scheme
Given the seed of a create node, the discriminator for the created contract ID is given as follows:
discriminator =
  hash(DISCRIMINATOR_TAG ++ seed ++ hashed_contract_instance)
where
hash is a collision-resistant hash function (e.g., SHA-256)
DISCRIMINATOR_TAG is a tag that marks this hash as being a discriminator
hashed_contract_instance is the collision-resistant hash of the contract instance, as given by the DAML-LF hash function for values.
Transaction ID generation
How transaction IDs are generated depends on the particular ledgers.
Sandbox / DAML on SQL: A counter for transactions.
Canton: A cryptographic hash of the following pieces:
the ledger effective time,
the domain (determined by the multi-domain dispatcher based on the transaction’s input contracts, the domain topology, and topology hints such as namespaces and workflow IDs)
the command ID, the application ID, the submitter
kvutils: Currently picked by the ledger integration. Sawtooth picks a UUID at submission time, VMware uses a counter.
Corda: The transaction ID is generated by
Corda
A transaction builder is signed by the initial party to return a signed transaction which is distributed to other parties and the notary for further signing as required.
fun signInitialTransaction(builder: TransactionBuilder): SignedTransaction
The signed transaction contains an id which is a Corda SecureHash.  This is 256bit (32 byte) SHA256 hash of the transaction payload.  This transaction may contain a number of output contracts.  These are identified by a Corda StateRef:
StateRef(val txhash: SecureHash, val index: Int)
It is these StateRef objects which are mapped onto AbsoluteContractId (and the other way around).  This is possible because we ensure that each output state we add has a reference of its associated NodeId.
Analysis
Executability
If command interpretation takes init_seed as input, it can derive all the salts and discriminators and detect clashes between discriminators. The raw transaction is transformed into the ready transaction only once before it hits the write service. (In that sense, Canton’s multi-domain dispatcher is not part of the write service.)
Validation
For validation, the transaction ID is known before re-interpretation starts (as it can be distributed along with the ready transaction). So the reinterpretation function can take the transaction Id and the seed as additional parameters.
reinterpret: txId -> seed -> node -> transaction
It can internally then derive the same seeds for the children and outputs the same subtransaction as what the submitter has computed. In particular, it does not contain IDs with discriminators only. Ledgers no longer need to map or store relative contract IDs.
In particular, every validator can check that the seeds have been correctly derived within the projection it sees. Sub-transactions with invalid derivations are considered invalid and must be rejected.
Unlinkability
For unlinkability, we assume that the submitter chooses high-quality randomness for the initial seed. (For if it is dishonest, it could just publish the contents of the contracts directly.)
The seed derivation using the DRBG spreads this randomness to the seeds of all create nodes. These seeds become salts that blind the hash of the contract instance. Therefore, a discriminator can be linked only to a contract instance if the salt is known.
The seeds in one subtree are pseudo-independent from those in a different subtree. Therefore, if a participant learns the seeds of its projection, it does not learn anything about the salts in the subtrees outside of its projection. It therefore cannot link the discriminators in those trees with the contract instances.
The unlinkability guarantee depends on the randomness quality of the initial seed. This is why the initial seed construction includes the nonce or is based on a private key of the submitting participant.
Freshness
We now consider a malicious submitter that can choose the initial seed arbitrarily, rather than randomly. Moreover, we also assume that the ledger ensures that transaction IDs are unique with overwhelming probability under the appropriate trust assumptions. Let C1 and C2 be two different contracts on the ledger. We want to show that the contracts have different IDs with overwhelming probability.
If C1 and C2 are instances of different templates, then those templates will affect the hashed_contract_instance and therefore lead to different discriminators.
If C1 and C2 have different signatories, observers, or stakeholders, then their contract instances must be different, as they are derived from the template parameters. Like in the previous case, hashed_contract_instance will produce a different hash and therefore lead to different discriminators.
If C1 and C2 are created by different transactions, then the transaction IDs are different with overwhelming probability. Since these IDs are suffixes of the contract IDs, the contract IDs are also different.
So let C1 and C2 are created by different subactions of the same transaction on the ledger, with the same template and contract instances. Let A1 and A2 be the root actions of the transaction that contain C1 and C2, respectively. By definition, the seeds in A1 and A2 have been derived from seeds seed1 and seed2 as described above.
If A1 = A2, then the nodes creating C1 and C2 have different seeds by the collision resistance of the seed derivation scheme.
If A1 ≠ A2, then by distinctness below, we can assume seed1 ≠ seed2. Again, by collision resistance, C1 and C2 have different IDs.
Distinctness
Different ledgers enforce distinctness of seeds differently:
Ledgers with a centralized committer (kvutils, DAML-on-SQL):
The committer checks that the seeds at the root of the transaction are unique.
Canton
The mediator checks that the seeds are pairwise different.
Corda
The validating notaries check that the seeds are pairwise different.
