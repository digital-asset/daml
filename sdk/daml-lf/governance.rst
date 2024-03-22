.. Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml-LF Governance process
==========================

These are the principles and rules by which changes to three data
formats are governed by the Daml Language team:

* language,
* values and,
* transaction nodes, and
* transactions.

Overview
--------

* The *Daml-LF language* is the typed core calculus that Daml gets
  compiled to, and that the ledger runs.

* *Daml-LF  serializable  values*
  (Daml-LF values for brevity in the rest of this document)
  is  the  universe of  Daml-LF  values  that  can  be stored  on  the
  ledger.   Currently  nominal  records,  nominal   variants,  lists,
  optionals, maps, and primitive types.

* *Daml-LF transaction nodes* (Daml-LF nodes for brevity in the rest
   of this document) used to represent Daml actions described in the
   Daml ledger model. Currently there are *create*, *exercise*,
   *fetch*, and *lookup-by-key* nodes.
   
* *Daml-F transactions* are sequences of transaction nodes that
  defines an update to the ledger.
  
* The Daml-LF language is versioned using a major and minor component.
  Changes to the major component allow us to drop features, or update
  the serialization format.  Changes to the minor component cannot
  break backward compatibility, and operate on the same major version
  of the serialization format in a backward compatible way. For
  historical reason this scheme starts at `1.6`.

* Daml-LF values, nodes, and transactions are versioned using a common
  one component version scheme, called transaction version scheme.
  For historical reason this scheme starts at `10`.
  
* Daml-LF Language, values, nodes, and transactions are evolved
  together without ever breaking backwards compatibility.
  
* Daml-LF values, nodes, and transactions, and language are involved
  together, meaning the introduction of a new language version implies
  the introduction of a new transaction version, and reciprocally.

* A specification is provides for each major version language, and A
  common specification document are provided for values, nodes,
  transactions.  Moreover, a one-line summary of each version must be
  present in the ``daml-lf-X.proto`` and ``transaction.proto`` files.

* Every change to Daml-LF values, nodes, transactions or languages *must* be
  checked for governance rules conformance and be approved by Rémy Haemmerle
  (@remyhaemmerle-da) or Moisés Ackerman (@akrmn) if  Rémy is on vacation.
  Proposed changes can be filed as tickets under labels "component/daml-lf"
  and "discussion".

Language
--------

Some version bumps might not change the ``.proto`` at all, but just
add / change semantics in the specification. For example, the string
format for decimal numbers in Daml-LF values might be made more
lenient, resulting in a version bump with no ``.proto`` schema changes
(but probably a comment).

Also note that the Daml-LF versioning is independent from the Daml
surface language versioning.

"dev" version
~~~~~~~~~~~~~

Every Daml-LF major version includes a minor version, the *dev*
version, which we use as staging area for the next stable minor
version of that major version.

The "dev" version can be changed freely without compatibility
considerations to prior "dev" versions.  Since "dev" is always
considered to be newer than every stable minor version of that major
version, it must be backward-compatible with all such stable versions.

All newly-implemented minor version features or changes must be staged
in the *dev* version for later release as a stable group of
backward-compatible features.

The Daml-LF dev version is enabled in the sandbox and ledger server,
but will never be emitted by damlc unless explicitly requested via
``--target 1.dev`` or similar.

Associated Transaction version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each language version is associated to an unique transaction version,
called the associated transaction version. Language versions `1.6`,
`1.7` and `1.8` are all associated to transaction version
`10`. Starting for language version `1.11`, each language version
`1.x` is associated with transaction `1.x`.

Values, Nodes, Transactions
---------------------------

Every change to the ``transaction.proto`` specification must yield a
specification capable of consuming all values, nodes, and transactions
produced by any prior version of the specification. New fields can be
added, the human-readable names of existing fields can be changed, and
the semantics of fields can be changed according to the version number
included with the enclosing value, node or transaction.  Moreover,
messages produced via the new proto may be consumed via any prior
proto, but *only* to the extent of completing without error and
providing the version number.  The remaining contents of the message
are neither required to make sense, nor preserve the semantics of
older versions.

This "old data can be consumed" form is the only form of compatibility
supported.  That is because there is no way for a consumer to
anticipate whether new, unreadable data may be essential to consuming
a given value.  Consumers can detect gracefully if they get a value, a
node, or a transaction that they do not support, and *must* reject it
rather than "best-effort" consuming it in such a case, because any
field might be ignored when within values/transactions of later
versions.

For Daml-LF values, node and transaction we have a single document for
all the versions, with annotations on version-specific features (in
the form of "since version XYZ").

These versioning rules are described in details, with examples, in the
transaction specification.

Permanent backward compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*This section is non-normative.*

Unlike the language, values and transactions have only one major
version each; in terms of how language is versioned, they only have
minor versions.  Their treatment is different for a few reasons:

- We expect Daml-LF values to be often stored in a long-term fashion -
  for example a ledger needs to do so.

- It should be possible to revalidate old transactions at any time;
  requiring users to maintain an archive of every Daml-LF engine
  version they ever used in order to be able to revalidate would be
  extremely onerous, and ledger operators should be able to upgrade
  the engine freely.

- While we could have major revisions and have each ledger deployment
  to have a blessed set of major revisions, like we plan do for code,
  we expect the impact of such a decision to be much greater, since
  there is essentially only one consumer of Daml-LF code (the server),
  while Daml-LF values will be stored all over the place, and the
  disruption of breaking changes would be too burdensome.

- It is *much* easier to preserve compatibility of simple first-order
  data, compared to a full-blown language.

Working with LF data
--------------------

The language Team provides libraries to read and write Daml-LF values,
Daml-LF transactions, and Daml-LF packages in a version-aware manner,
to aid the implementation of readers and writers.

With "version-aware" we mean that the libraries encoding and decoding
data structures are aware of what versions they support, and will fail
gracefully if they encounter unknown versions.  Because this only
becomes more complex as the specifications evolve, we strongly
recommend that JVM-hosted applications use our libraries for encoding
and decoding.

The ``daml-lf/transaction`` library for Daml-LF values and
transactions includes:

.. todo include this at some point? - Specification

- ``.proto`` definitions for both, in two different files, with the
  transaction one referencing the value one.
- Enforcement of the specifications' rules, not just the proto structure
- Data structures to work with values and transactions from at least
  Scala and Java, and functions to decode / encode between the
  ``.proto`` and the data structures.

The ``daml-lf/lfpackage`` library for the Daml-LF language includes:

.. todo include this at some point? - Specifications, one per major revision

- Data structures to work with Daml-LF packages. Currently, this is an
  AST that supports a set of Daml-LF versions, spanning across multiple
  major revisions, either by having a larger type or by converting some
  features into other features (e.g. if we add dependent functions the
  library would convert non-dependent functions and foralls into
  dependent functions).

``lfpackage`` is built upon the ``daml-lf/archive`` library, which
includes:

- The language ``.proto`` files, with a top-level sum type to identify
  the major revision.

The ``daml-lf/validation`` library includes:

- Enforcement of the language specifications' rules, not just the proto
  structure, against ``lfpackage``'s AST
- Checking properties of the synthesis of the different specifications,
  e.g. that a particular value conforms to a type

Engine
------

The Language team provides the Daml-LF engine. The Daml-LF engine
will advertise what versions of the Daml-LF language it accepts.
It is guaranteed and engine will
accept all transaction version associated to those language versions.

..  LocalWords:  optionals LF
