.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


Daml-LF Transaction Specification
=================================

**version 14, 22 June 2021**

This specification, in concert with the ``transaction.proto``
machine-readable definition, defines a format for *Daml-LF
transactions*, to be used when inspecting ledger activity as a st
ream, or submitting changes to the ledger.

A *ledger* can be viewed as a sequence of these transactions.

Do not read this without ``transaction.proto``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``transaction.proto`` defines the baseline rules for *Daml-LF
transactions*; that file must be consulted in concert with this
document for a full specification of Daml-LF transactions.  Except
where required for clarity, we do not repeat rules defined and
enforced in that file within this document.  When consulting the
section on each message type, you must also refer to the same
definition in ``transaction.proto`` for a full definition of the
requirements for that message.

This document is insufficiently detailed to construct a correct
transaction; you must refer to ``transaction.proto`` as well to have a
full set of rules.

Do not read ``transaction.proto`` without this
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the same fashion, ``transaction.proto`` by itself cannot be consulted
for a full specification of the transaction format, because it is
impossible to define all the requirements for transactions in the
``.proto`` format.  All such rules are included in this document,
instead.

If you are constructing a Daml-LF transaction, it is not sufficient to
merely conform to the structure defined in ``transaction.proto``; you
must also conform to the rules defined in this document.  A transaction
that happens to conform to ``transaction.proto``, yet violates some rule
of this document, is not a valid Daml-LF transaction.

Backward compatibility
^^^^^^^^^^^^^^^^^^^^^^

Daml-LF transaction nodes, and transactions are encoded according a
common versioning scheme, called the *transaction version scheme*.
Each version of this scheme, called a transaction version, is
associated to a language version.

In the following *dev* transaction version will refer to a transaction
version associated to a *dev* language version and *stable*
transaction version will refer to a transaction version associated to
a non-*dev* language version.

Unlike the serialization format of a *dev* transaction version which
can be changed freely without compatibility considerations, the
serialization format of a *stable* transaction version cannot be
changed or associated to a different language version once introduced.

An consumer compliant with this specification does not have to provide
any support for *dev* transaction version. It must however accept all
stable versions.

Every change to ``transaction.proto`` entails a change to this
specification.  `Version history`_ defines a total ordering of all
past stable versions; any stable version *y* unlisted there should be
considered *y>k* for any known version *k*.  At the top of the
specification file, just under the title are written a version number
together with a date. While, the version number described the latest
stable transaction version the file specified, the date indicate when
the file was modified for the last time.  Those changes may included
rewording and typo correction in the specification of stable versions
and arbitrary change in specification of a dev transaction versions.

A a node, or a transaction of version *n* may be interpreted by
consulting any version of this document *m≥n*.  Likewise, any version
*q* of the transaction specification is sufficient to interpret
transactions of any version *r≤q*.  In other words, later versions of
this specification preserve the semantics and parseability of earlier
versions; there is no need to consult or implement older versions in
order to interpret any transaction, and you may always simply consult
the latest appropriate version.

By contrast, a node, or a transaction of version *s* must be rejected
by a consumer that implements this specification of version *t<s*.  So
if you produce a transaction of version *s*, you may assume that its
consumer either implements some version *u≥s*, or will reject the
message containing the transaction.  The ``.proto`` format may make
parsing such transactions possible, but that is irrelevant; semantics
defined in later versions of this specification may be vital for
correctly interpreting transactions defined under those later
versions, and you must not suppose otherwise.

For example, suppose you have a transaction of version 10.  You can
expect it to be interpreted the same by consumers implementing
specification version 11, 12, 5000, and so on.  On the other hand, you
can expect a consumer of version 10 or 11 to reject the message
containing that transaction, because specification version 12 might
define some semantics vital to understanding your transaction.

"since version"
~~~~~~~~~~~~~~~

Every message type and field is accompanied by one or more *(since
version x)* annotations in this document, preceding some description
of semantics.  This defines when that message, field, or semantic rule
was introduced in the transaction specification.  Where there are
multiple overlapping definitions of semantics for the same field of
the form *(since version x)*, the correct interpretation for any
transaction of version *y* is that under the greatest *x* such that
*x≤y*, failing that the second-greatest such *x*, and so on.

For example, suppose you have received transactions of versions 11 and
12, and this document defines overlapping semantics for *since version
10*, *11*, and *13*.  You should interpret both transactions according
to the *since version 11* section, also relying on *since version 10*
where not in conflict with *since version 11*; however, the *since
version 13* section must be ignored entirely.

Changing this specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Future versions of this specification must conform to the `Daml-LF
Governance process`_ and preserve all invariants described above.
Where these are in conflict, the governance process takes priority.

If you introduce version *z*, your changes to this document should
almost certainly all occur under *since version z* annotations; a new
version without any such annotations in either this or the `value`_
specification is probably not a new version at all.  Simply updating
the semantic descriptions is tantamount to retroactively editing the
specification of older versions, and is a violation of governance
except where correction of the description of those older versions is
actually desirable.

Moreover, if those semantics conflict with prior version *y*, such as
by deleting a field, you should note that with an annotation *since
version z, conflicts with version y*.

For example, suppose in version 12, you are defining new semantics for
a field introduced in version 11.  Simply describing those semantics
under the existing *since version 11* section is a governance
violation; you must add a *since version 12* section and describe the
semantics there; this section should be titled *since version 12,
conflicts with version 2* if it does not merely extend, but instead
replaces some part of the *since version 11* description.

However, you may modify the *since version 11* section to explain how
*that version* differs from the newly-added version 4; that is because
this change doesn't modify the specified rules for version 2, it merely
clarifies those rules for the reader, who might otherwise miss an
important subtlety in the comparison.

Additionally, you should update the following `Version history`_.

.. _`Daml-LF Governance process`: ../governance.rst
.. _`value`: value.rst

Version history
^^^^^^^^^^^^^^^

This table lists every version of this specification in ascending order
(oldest first).

Support for transaction versions 1 to 9 was dropped on 2020-11-02.
This breaking change does not impact ledgers created with SDK 1.0.0 or
later.

+--------------------+-----------------+
| Version identifier | Date introduced |
+====================+=================+
|                 10 |      2020-03-25 |
+--------------------+-----------------+
|                 11 |      2021-01-19 |
+--------------------+-----------------+
|                 12 |      2021-01-27 |
+--------------------+-----------------+
|                 13 |      2021-04-06 |
+--------------------+-----------------+
|                 14 |      2021-06-03 |
+--------------------+-----------------+
|                dev |                 |
+--------------------+-----------------+

message Transaction
^^^^^^^^^^^^^^^^^^^

A list of `message Node`_, implicitly forming a forest starting at
``roots``.

(*since version 10*)

As of version 10, these fields are included:

* ``string`` `field version`_
* repeated ``string`` roots
* repeated `message Node`_ nodes

``version`` is required and constrained as described under `field
version`_.  Consumers can expect this field to be present and to have
the semantics defined here without knowing the version of this value
in advance.

``roots`` is constrained as described under `field node_id`_.

field version
~~~~~~~~~~~~~

``version`` and must be a version of this specification.  For example,
for version 11 of this specification, ``version`` must be ``"11"``

Known versions are listed in ascending order in `Version history`_;
any ``version`` not in this list should be considered newer than any
version in same list, and consumers must reject values with such
unknown versions.

message ContractInstance
^^^^^^^^^^^^^^^^^^^^^^^^

An instance of a Daml-LF template, represented by the Daml-LF value used
to construct that instance.

(*since version 10*)

As of version 10, these fields are included:

* `message Identifier`_ template_id
* `message VersionedValue`_ arg_versioned
* ``string`` agreement

``template_id`` and ``arg_versioned`` are required; ``agreement`` is
optional.

``template_id``'s structure is defined by `the value specification`_.

.. _`message Identifier`: value.rst#message-identifier
.. _`message VersionedValue`: value.rst#message-versionedvalue
.. _`message ContractId`: value.rst#message-contractid
.. _`the value specification`: value.rst

message Node
^^^^^^^^^^^^

An action on the ledger.

(*since version 10*)

As of version 10, these fields are included:

* ``string`` `version`
* ``string`` `node_id`

``version``  is optional. If unset it should be interpreted as version 10.
otherwise it should be constraint as described in `field version`_

``node_id`` is required. it is csontraint as described under `field
node_id`_.

Additionally, one of the following node types *must* be included:

* `message NodeCreate`_ create
* `message NodeFetch`_ fetch
* `message NodeExercise`_ exercise
* `message NodeLookupByKey`_ lookup

Field field ``create``, ``fetch``, ``exercise`` and ``lookup`` shall
be consumed according to version 10.
  
(*since version 11*)

As of version 11, this optional field is included:

* ``string`` ``version``

The field ``version`` is optional.

If present it must be a valid version as described under `field
version`_, different from "10", and not newer that the version of the
enclosing Transaction message. Other it is assumed to be version "10".

Field field ``create``, ``fetch``, ``exercise`` and ``lookup`` shall
be consumed according to that version.

field node_id
~~~~~~~~~~~~~

An identifier for this node, unique within the transaction.

(*since version 10*)

There are no particular requirements on its structure or how to generate
them, and node IDs can be reused in different transactions.  An
incrementing natural number is perfectly sufficient on the transaction
producer's part.  However, given this freedom, the consumer must make no
assumptions about IDs' structure or order; they are opaque, unique IDs.

It must conform to the regular expression::

  [A-Za-z0-9._:-]+

Each node ID used as the value of this field must also occur exactly
once, as either

* one of ``roots`` in the containing `message Transaction`_, or
* one of ``children`` in some other `message NodeExercise`_ in the
  transaction.

A node ID that occurs zero, two, or more times in those contexts yields
an invalid transaction.

message KeyWithMaintainers
^^^^^^^^^^^^^^^^^^^^^^^^^^

A contract key paired with its induced maintainers.

(*since version 10*)

As of version 10, these fields are included:

* `message VersionedValue`_ key_versioned
* repeated ``string`` maintainers

``key_versioned`` is required.

``maintainers`` must be non-empty, whose elements are party
identifiers.

(*since version 12*)

As of version 12, this field is included:

* `message Value`_ key_unversioned

``key_unversioned`` is required while ``key_versioned`` is not used
anymore.

message NodeCreate
^^^^^^^^^^^^^^^^^^

The creation of a contract by instantiating a Daml-LF template with the
given argument.

(*since version 10*)

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message ContractInstance`_ contract_instance
* repeated ``string`` stakeholders
* repeated ``string`` signatories
* `message KeyWithMaintainers`_ key_with_maintainers

``contract_id_struct`` is required. Its structure is defined by `the value
specification`_.

``contract_instance`` is required.

Every element of ``signatories`` and ``stakeholders`` is a party
identifier.

.. note:: *This section is non-normative.*
	  
  The stakeholders of a contract are the signatories and the observers of
  said contract.

  The signatories of a contract are specified in the Daml-LF definition of
  the template for said contract. Conceptually, they are the parties that
  agreed for that contract to be created.

``key_with_maintainers`` is optional. 

(*since version 12*)

As of version 12, these fields are included:

* `message Identifier`_ template_id
* `message VersionedValue`_ arg_unversioned
* ``string`` agreement

``template_id`` and ``arg_unversioned`` is required while
``contract_instance`` is not used anymore.

message NodeFetch
^^^^^^^^^^^^^^^^^

Evidence of a Daml-LF ``fetch`` invocation.

(*since version 10*)

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message Identifier`_ template_id
* repeated ``string`` stakeholders
* repeated ``string`` signatories
* repeated ``string`` actors
* `message KeyWithMaintainers`_ key_with_maintainers
* ``string`` value_version

``contract_id_struct`` is required. Its structure is defined by `the
value specification`_.

``template_id`` is required. ``template_id``'s structure is defined by
`the value specification`_

Every element of ``stakeholders``, ``signatories`` and ``actors`` is a
party identifier.

``actors`` is required to be non-empty:

.. note:: *This section is non-normative.*

  Actors are specified explicitly by the user invoking fetching the
  contract -- or in other words, they are _not_ a property of the
  contract itself.

``key_with_maintainers`` is optional.

(*since version 1.14*)

As of version 1.14, this field is required:

``bool`` byKey

message NodeExercise
^^^^^^^^^^^^^^^^^^^^

The exercise of a choice on a contract, selected from the available
choices in the associated Daml-LF template definition.

(*since version 10*)

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message Identifier`_ template_id
* repeated ``string`` actors
* ``string`` choice
* `message VersionedValue`_ arg_versioned
* ``bool`` consuming
* repeated ``string`` children
* repeated ``string`` stakeholders
* repeated ``string`` signatories
* `message VersionedValue`_ result_versioned
* `message KeyWithMaintainers`_ key_with_maintainers

``contract_id_struct`` is required. 

``children`` and ``key_with_maintainers`` may be empty; all other
fields are required, and required to be non-empty.

``template_id``'s structure is defined by `the value specification`_.

``children`` is constrained as described under `field node_id`_.

.. note:: *This section is non-normative.*

  Every node referred to as one of ``children`` is another
  update to the ledger taken as part of this transaction and as a
  consequence of exercising this choice. Nodes in ``children`` appear
  in the order they were created during interpretation.

Every element of ``actors``, ``stakeholders``, ``signatories``, and
``controllers`` is a party identifier.

.. note:: *This section is non-normative.*

  The ``stakeholders`` and ``signatories`` field have the same meaning
  they have for ``NodeCreate``.

  The ``actors`` field contains the parties that exercised the choice.
  The ``controllers`` field contains the parties that _can_ exercise
  the choice. Note that according to the ledger model these two fields
  _must_ be the same. For this reason the ``controllers`` field was
  removed in version 6 -- see *since version 10* below.

  The ``controllers`` field must be empty. Software needing to fill in
  data structures that demand both actors and controllers must use the
  ``actors`` field as the controllers.

(* since version 11*)
  
As version 11, this field is included:

* repeated ``string`` observers

Every element of ``observers`` is a party identifier.

(*since version 12*)

As version 12, these field are included:
 
* `message VersionedValue`_ arg_unversioned
* `message VersionedValue`_ result_unversioned

``arg_unversioned`` and ``result_unversioned`` are required, while
``arg_versioned`` and ``result_versioned`` are not used anymore.

(*since version 14*)

As of version 14, this field is required:

``bool`` byKey

.. TODO: https://github.com/digital-asset/daml/issues/15882
.. -- update for choice authorizers

message NodeLookupByKey
^^^^^^^^^^^^^^^^^^^^^^^

The lookup of a contract by contract key.

(*since version 10*)

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message Identifier`_ template_id
* `message KeyWithMaintainers`_ key_with_maintainers

``template_id`` and ``key_with_maintainers`` are
required. ``contract_id_struct`` is optional:

.. note:: *This section is non-normative.*

  if a contract with the specified key is not found it will
  not be present.

``template_id``'s structure is defined by `the value specification`_

.. _`the value specification`: value.rst


message NodeRollBack
^^^^^^^^^^^^^^^^^^^^

The rollback of a sub-transaction.

(*since version 14*)

As of version 14, these fields are included:

* repeated ``string`` children
