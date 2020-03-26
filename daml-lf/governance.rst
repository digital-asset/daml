.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML-LF Governance process
==========================

These are the principles and rules by which changes to three data
formats are governed by the DAML Runtime team:

* values,
* transactions, and
* language.

Overview
--------

* *DAML-LF serializable values* or (DAML-LF values for brevity in the
  rest of this document) is the universe of DAML-LF values that can be
  stored on the ledger. Currently nominal records, nominal variants,
  lists, and primitive types.

* *DAML-LF transactions* are the data structure that defines an update
  to a ledger. Currently comprised of create, exercise, and fetch
  nodes.

* The *DAML-LF language* is the typed core calculus that DAML gets
  compiled to, and that the ledger runs.

* DAML-LF values and transactions are evolved without ever breaking
  backwards compatibility. They are evolved separatedly, with
  transactions building on top of values.

* Specification documents are provided for DAML-LF values, DAML-LF
  transactions, and for the DAML-LF language.

* Every merged change to DAML-LF values, transactions, or language
  *must* come with changes to the appropriate specification
  document. Moreover, a one-line summary of each version must be
  present in the ``.proto`` files for DAML-LF values, transactions, and
  language.

* Every change to DAML-LF values or transactions *must* be checked for
  governance rules conformance and approved by Stephen Compall, or by
  Gerolf Seitz if Stephen is on vacation.  Every change to the DAML-LF
  language *must* be checked for governance rules conformance and
  approved by Rémy Haemmerle and Martin Huschenbett (with Gerolf Seitz
  as Rémy's vacation backup, and Francisco Mota as Martin's vacation
  backup).

  These checks are for governance and versioning only; the substance of
  DAML-LF language changes should be made in consultation with the DAML
  Language team.  Proposed changes can be filed as tickets under
  Milestone "DAML-LF Spec & Validation", labels "component/daml-lf" and
  "discussion".

* Every ledger implementation must declare what DAML-LF value,
  transaction, and language versions it supports. This information will
  be most likely tied to what DAML-LF engine the ledger is running, but
  additional constraints external to the DAML-LF engine can be put in
  place, if necessary. Note that a running ledger can never drop support
  for value, transaction, or language versions, since it must be able to
  replay every transaction ever run on the ledger. However the ledger is
  free to refuse to accept new code running with older DAML-LF versions
  while still maintaining support for existing code internally.

* Client libraries and other tooling mostly care about DAML-LF values
  and DAML-LF serializable types. Deciding how these components handle
  versions is not part of the DAML-LF governance progress. For
  example, a certain version of the DAML-LF Scala codegen might
  support a specified set of DAML-LF versions -- much like ledger
  implementations.

Values
------

Each DAML-LF value and DAML-LF transaction comes with a version number,
indicating how the respective specification should be consulted for
rules regarding that value or transaction.

Every change to the ``.proto`` specification must yield a specification
capable of consuming all values produced by any prior version of the
specification. New fields can be added, the human-readable names of
existing fields can be changed, and the semantics of fields can be
changed according to the version number included with the enclosing
value or transaction.  Moreover, messages produced via the new proto may
be consumed via any prior proto, but *only* to the extent of completing
without error and providing the version number.  The remaining contents
of the message are neither required to make sense, nor preserve the
semantics of older versions.

This "old data can be consumed" form is the only form of compatibility
supported.  That is because there is no way for a consumer to anticipate
whether new, unreadable data may be essential to consuming a given
value.  Consumers can detect gracefully if they get a value or a
transaction that they do not support, and *must* reject it rather than
"best-effort" consuming it in such a case, because any field might be
ignored when within values/transactions of later versions.

These versioning rules are described in more detail, with examples, in
the value specification.

For DAML-LF values we have a single document for all the versions, with
annotations on version-specific features (in the form of "since version
XYZ").

Note how the versioning is not per-node, but as a whole. The version is
decided as the DAML-LF engine generates a value -- at contract creation
time and contract exercise time (for choice arguments). Client libraries
are free to present all values according to latest DAML-LF value they
understand.

Permanent backward compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*This section is non-normative.*

Unlike the language, values and transactions have only one major version
each; in terms of how language is versioned, they only have minor
versions.  Their treatment is different for a few reasons:

- We expect DAML-LF values to be often stored in a long-term fashion -
  for example a ledger needs to do so.

- It should be possible to revalidate old transactions at any time;
  requiring users to maintain an archive of every DAML-LF engine version
  they ever used in order to be able to revalidate would be extremely
  onerous, and ledger operators should be able to upgrade the engine
  freely.

- While we could have major revisions and have each ledger
  deployment to have a blessed set of major revisions, like we plan do
  for code, we expect the impact of such a decision to be much greater,
  since there is essentially only one consumer of DAML-LF code (the
  server), while DAML-LF values will be stored all over the place, and
  the disruption of breaking changes would be too burdensome.

- It is *much* easier to preserve compatibility of simple first-order
  data, compared to a full-blown language.

Transactions
------------

Transactions are specified and versioned according to the same rules as
those for values, so everything above applies equally to transactions as
it does for values.  The only difference is that, as a separate
specification, transactions have their own versions, independent of
value versions.

Transactions incorporate values; however, this does not mean that the
transaction's version depends on any of those values' versions.  Every
occurrence of a value in a transaction is accompanied by a version of
the value specification under which that value should be considered.

As with values, transactions are versioned as a whole, with the
DAML-LF engine picking a version when it creates a new transaction.

Language
--------

The DAML-LF *language* is versioned using a major and minor component.
Increasing the major component allows us to drop features or change
semantics of existing features, and implies creating an entirely new
``.proto`` message for the new major version. Changes to the minor
component cannot break backwards compatibility, and operate on an
existing major version and its ``.proto`` message in a backwards
compatible way; "compatibility" here means exactly what it does for
values and transactions, i.e. "old data can be read"; older serialized
``.proto`` messages from the same major version are still readable and
their semantics are unchanged.

Note that some major or minor version bumps might not change the
``.proto`` at all, but just add / change semantics in the
specification. For example, the string format for decimal numbers in
DAML-LF values might be made more lenient, resulting in a minor version
bump with no ``.proto`` schema changes (but probably a comment).

Also note that the DAML-LF versioning is independent from the DAML
surface language versioning.

We have one specification document per major version, with each document
noting the differences between minor versions within that major version.

"dev" version
~~~~~~~~~~~~~

Every DAML-LF major version includes a minor version, the *dev* version,
which we use as staging area for the next stable minor version of that
major version.

The "dev" DAML-LF major version can be changed freely without
compatibility considerations to prior "dev" versions.  Since "dev" is
always considered to be newer than every stable minor version of that
major version, it must be backward-compatible with all such stable
versions.

All newly-implemented minor version features or changes must be staged
in the *dev* version for later release as a stable group of
backward-compatible features.

The DAML-LF dev version is enabled in the sandbox and ledger server, but
will never be emitted by damlc unless explicitly requested via
``--target 1.dev`` or similar.

Working with LF data
--------------------

The DAML Runtime team provides libraries to read DAML-LF values, DAML-LF
transactions, and DAML-LF packages in a version-aware manner, to aid the
implementation of readers and writers.

With "version-aware" we mean that the libraries encoding and decoding
data structures are aware of what versions they support, and will fail
gracefully if they encounter unknown versions.  Note that the value and
transaction specifications *require* that consumers are version-aware in
this way.  Because this only becomes more complex as the specifications
evolve, we strongly recommend that JVM-hosted applications use our
libraries for encoding and decoding.

The ``daml-lf/transaction`` library for DAML-LF values and transactions
includes:

.. todo include this at some point? - Specification

- ``.proto`` definitions for both, in two different files, with the
  transaction one referencing the value one.
- Enforcement of the specifications' rules, not just the proto structure
- Data structures to work with values and transactions from at least
  Scala and Java, and functions to decode / encode between the
  ``.proto`` and the data structures.

The ``daml-lf/lfpackage`` library for the DAML-LF language includes:

.. todo include this at some point? - Specifications, one per major revision

- Data structures to work with DAML-LF packages. Currently, this is an
  AST that supports a set of DAML-LF versions, spanning across multiple
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

The DAML Runtime team provides the DAML-LF engine. The DAML-LF engine
will advertise what versions of the DAML-LF values and language it
accepts (in commands and packages respectively). Note that the DAML-LF
engine also produces versioned DAML-LF values and transactions. We do
not currently specify the algorithm that decides what version the
DAML-LF engine will use when producing values and transactions.

We do however guarantee that, for the engine:

- Let *er* = DAML-LF engine release,

- let *lv* = DAML-LF language version,

- let *ts* = contract template signature,

- let *vt* = value or transaction;

- we use the deterministic algorithm *vtv* to decide the DAML-LF value /
  transaction version, ``er × lv × ts × vt -> version``;

- If we fix *lv*, *ts*, and *vt*, *vtv* is monotonically decreasing. In
  other words, if the current release of the DAML-LF engine produces
  values of version ``X`` for values regarding a certain contract
  template, future releases will produce DAML-LF values of some version
  ``Y`` such that ``Y ≤ X``.

This allows deployments to use newer DAML-LF language versions while
still working with older clients.

This set of rules applies only to the DAML engine; producers of values
and transactions are not generally required to adhere to these rules,
and may simply use the latest versions of the specifications they
support.

In any case, once a versioned DAML-LF value / transaction is produced by
the DAML-LF engine, the committer must store it together with its
version, which is fixed forever. Note that a ledger implementation is
free to store transactions in some new better format, as long as the
original versioned transaction is stored too so that the whole ledger
can be verified at will.

More on version selection
~~~~~~~~~~~~~~~~~~~~~~~~~

*This section is non-normative.*

We are leaving what version to pick for values and transactions up to
the DAML-LF engine, and not specifying how the DAML-LF engine picks
these versions. This is because we have a few options that we do not
want to commit to right now. Specifically:

- Pin the DAML-LF value version to the DAML-LF language version.  Or in
  other words have a map from DAML-LF language version to what DAML-LF
  value and transaction version to generate. This is problematic because
  it prevents us from generating values compatible with older clients
  with newer versions of the DAML-LF language. For example, say we have
  DAML-LF language 1.0 pinned to DAML-LF value 1. Then we upgrade
  DAML-LF to version 1.1, which adds support for a new numeric type,
  which is available in DAML-LF value 2. With this pinning scheme, *all*
  contract templates defined in DAML-LF 1.1 would use DAML-LF values 2,
  even if they do not use the new numeric type in question. And thus old
  clients will be unable to read all values generated from DAML-LF 1.1,
  even if many contract templates in DAML-LF 1.1 won't make use of the
  new numeric type at all.

- Have the DAML-LF engine to generate values and transactions of the
  lowest version possible. This is the best and "smartest" solution,
  given that values and transactions are forever backwards compatible.
  However, we aren't fully cognizant of what else this might imply.

- Have the users of DAML specify what DAML-LF value version to use for
  each template. This is probably the most "principled" solution, in the
  sense that it's the easiest to support while providing predictable
  results. However it burdens the users with something they are not
  burdened with today, and embeds a highly esoteric choice into the
  writing of each surface language template.

So in the meantime we make the weaker promise in the previous section,
which we might make stronger in the future.
