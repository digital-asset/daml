.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML-LF Transaction Specification
=================================

**version 10, 25 March 2020**

This specification, in concert with the ``transaction.proto``
machine-readable definition, defines a format for _transactions_, to be
used when inspecting ledger activity as a stream, or submitting changes
to the ledger.

A _ledger_ can be viewed as a sequence of these transactions.

Do not read this without ``transaction.proto``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``transaction.proto`` defines the baseline rules for transactions; that
file must be consulted in concert with this document for a full
specification of DAML-LF transactions.  Except where required for
clarity, we do not repeat rules defined and enforced in that file within
this document.  When consulting the section on each message type, you
must also refer to the same definition in ``transaction.proto`` for a
full definition of the requirements for that message.

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

If you are constructing a DAML-LF transaction, it is not sufficient to
merely conform to the structure defined in ``transaction.proto``; you
must also conform to the rules defined in this document.  A transaction
that happens to conform to ``transaction.proto``, yet violates some rule
of this document, is not a valid DAML-LF transaction.

Backward compatibility
^^^^^^^^^^^^^^^^^^^^^^

DAML-LF transactions are accompanied by a version identifier; every
change to ``transaction.proto`` entails a change to this specification,
and every change to this specification introduces a unique new version.
`Version history`_ defines a total ordering of all past versions; any
version *y* unlisted there should be considered *y>k* for any known
version *k*.

A transaction of version *n* may be interpreted by consulting any
version of this document *m≥n*.  Likewise, any version *q* of the
transaction specification is sufficient to interpret transactions of any
version *r≤q*.  In other words, later versions of this specification
preserve the semantics and parseability of earlier versions; there is no
need to consult or implement older versions in order to interpret any
transaction, and you may always simply consult the latest appropriate
version.

By contrast, a transaction of version *s* must be rejected by a consumer
that implements this specification of version *t<s*.  So if you produce
a transaction of version *s*, you may assume that its consumer either
implements some version *u≥s*, or will reject the message containing the
transaction.  The ``.proto`` format may make parsing such transactions
possible, but that is irrelevant; semantics defined in later versions of
this specification may be vital for correctly interpreting transactions
defined under those later versions, and you must not suppose otherwise.

For example, suppose you have a transaction of version 3.  You can
expect it to be interpreted the same by consumers implementing
specification version 3, 5, 5000, and so on.  On the other hand, you can
expect a consumer of version 1 or 2 to reject the message containing
that transaction, because specification version 3 might define some
semantics vital to understanding your transaction.

"since version"
~~~~~~~~~~~~~~~

Every message type and field is accompanied by one or more *since
version x* annotations in this document, preceding some description of
semantics.  This defines when that message, field, or semantic rule was
introduced in the transaction specification.  Where there are multiple
overlapping definitions of semantics for the same field of the form
*since version x*, the correct interpretation for any transaction of
version *y* is that under the greatest *x* such that *x≤y*, failing that
the second-greatest such *x*, and so on.

For example, suppose you have received transactions of versions 4 and 5,
and this document defines overlapping semantics for *since version 2*,
*4*, and *6*.  You should interpret both transactions according to the
*since version 4* section, also relying on *since version 2* where not
in conflict with *since version 4*; however, the *since version 6*
section must be ignored entirely.

Changing this specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Future versions of this specification must conform to the `DAML-LF
Governance process`_ and preserve all invariants described above.  Where
these are in conflict, the governance process takes priority.

If you introduce version *z*, your changes to this document should
almost certainly all occur under *since version z* annotations; a new
version without any such annotations in either this or the `value`_
specification is probably not a new version at all.  Simply updating the
semantic descriptions is tantamount to retroactively editing the
specification of older versions, and is a violation of governance except
where correction of the description of those older versions is actually
desirable.

Moreover, if those semantics conflict with prior version *y*, such as by
deleting a field, you should note that with an annotation *since version
z, conflicts with version y*.

For example, suppose in version 4, you are defining new semantics for a
field introduced in version 2.  Simply describing those semantics under
the existing *since version 2* section is a governance violation; you
must add a *since version 4* section and describe the semantics there;
this section should be titled *since version 4, conflicts with version
2* if it does not merely extend, but instead replaces some part of the
*since version 2* description.

However, you may modify the *since version 2* section to explain how
*that version* differs from the newly-added version 4; that is because
this change doesn't modify the specified rules for version 2, it merely
clarifies those rules for the reader, who might otherwise miss an
important subtlety in the comparison.

Additionally, you should update the following `Version history`_.

.. _`DAML-LF Governance process`: ../governance.rst
.. _`value`: value.rst

Version history
^^^^^^^^^^^^^^^

This table lists every version of this specification in ascending order
(oldest first).

The *"dev" version* is used as a staging area for the next stable transaction
version. The "dev" version can be changed freely without considering
compatibility with prior "dev" versions.

Support for transaction versions 1 to 9 was dropped on 2020-11-02.
This breaking change does not impact ledgers created with SDK 1.0.0 or
later.

+--------------------+-----------------+
| Version identifier | Date introduced |
+====================+=================+
|                 10 |      2020-03-25 |
+--------------------+-----------------+
|                dev |      2020-03-25 |
+--------------------+-----------------+

message Transaction
^^^^^^^^^^^^^^^^^^^

*since version 10*

A list of `message Node`_, implicitly forming a forest starting at
``roots``.

As of version 10, these fields are included:

* ``string`` version
* ``string`` roots
* repeated `message Node`_ nodes

``version`` is required, and must be a version of this specification.
For example, for version 10 of this specification, ``version`` must be
``"10"``.  Consumers can expect this field to be present and to
have the semantics defined here without knowing the version of this
value in advance.

Known versions are listed in ascending order in `Version history`_; any
``version`` not in this list should be considered newer than any version
in same list, and consumers must reject values with such unknown
versions.

``roots`` is constrained as described under `field node_id`_.

The node of the tree appears in pre-order traversal in ``nodes``

message ContractInstance
^^^^^^^^^^^^^^^^^^^^^^^^

*since version 10*

An instance of a DAML-LF template, represented by the DAML-LF value used
to construct that instance.

As of version 10, these fields are included:

* `message Identifier`_ template_id
* `message VersionedValue`_ value
* ``string`` agreement

``template_id`` and ``value`` are required; ``agreement`` is optional.

``value`` must conform to the type of the DAML-LF associated data type
indicated by ``template_id``.

``template_id``'s structure is defined by `the value specification`_;
the version of that specification to use when consuming it is the
``version`` field of ``value``.

.. _`message Identifier`: value.rst#message-identifier
.. _`message VersionedValue`: value.rst#message-versionedvalue
.. _`the value specification`: value.rst

message Node
^^^^^^^^^^^^

*since version 10*

An action on the ledger.

As of version 10, this required field is included:

* ``string`` `field node_id`_

Additionally, one of the following node types *must* be included:

* `message NodeCreate`_ create
* `message NodeFetch`_ fetch
* `message NodeExercise`_ exercise
* `message NodeLookupByKey`_ lookup

field node_id
~~~~~~~~~~~~~

*since version 10*

An identifier for this node, unique within the transaction.

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

*since version 10*

A contract key paired with its induced maintainers.

In this version, these fields are included:

* `message VersionedValue`_ key
* repeated ``string`` maintainers

``key`` is required.

``maintainers`` must be non-empty.

The key may not contain contract IDs.


message NodeCreate
^^^^^^^^^^^^^^^^^^

*since version 10*

The creation of a contract by instantiating a DAML-LF template with the
given argument.

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message ContractInstance`_ contract_instance
* repeated ``string`` stakeholders
* repeated ``string`` signatories
* `message KeyWithMaintainers`_ key_with_maintainers

``contract_id_struct`` is required. Its structure is defined by `the value
specification`_, version 3.

``contract_instance`` is required.

Every element of ``stakeholders`` is a party identifier.
``signatories`` must be a non-empty subset of ``stakeholders``.

.. note:: *This section is non-normative.*

  The stakeholders of a contract are the signatories and the observers of
  said contract.

  The signatories of a contract are specified in the DAML-LF definition of
  the template for said contract. Conceptually, they are the parties that
  agreed for that contract to be created.

``key_with_maintainers`` is optional. If present:

* Its ``maintainers`` must be a subset of the ``signatories``;
* The ``template_id` in the ``contract_instance`` must refer to a template with
  a key definition;
* Its ``key`` must conform to the key definition for the ``template_id``
  in the ``contract_instance``.

The maintainers of a contract key are specified in the DAML-LF definition of
the template for the contract.

*since version 4*

``contract_id`` must not be set, and this new field is required:

.. _`message ContractId`: value.rst#message-contractid

message NodeFetch
^^^^^^^^^^^^^^^^^

*since version 10*

Evidence of a DAML-LF ``fetch`` invocation.

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message Identifier`_ template_id
* repeated ``string`` stakeholders
* repeated ``string`` signatories
* repeated ``string`` actors
* `message KeyWithMaintainers`_ key_with_maintainers
* ``string`` value_version

``contract_id_struct`` is required. Its structure is defined by `the value
specification`_, version 3.

``template_id`` is required.

``template_id``'s structure is defined by `the value specification`_,
version 1.

Every element of ``stakeholders``, ``signatories`` and ``actors`` is a party
identifier.

``actors`` is required to be non-empty:

.. note:: *This section is non-normative.*

  Actors are specified explicitly by the user invoking fetching the
  contract -- or in other words, they are _not_ a property of the
  contract itself.

``key_with_maintainers`` is optional. It is present if and only if the
``template_id`` field refers to a template with a DAML-LF key
definition.  When present, the field's sub-fields ``key`` and
``maintainers`` must conform to the key definition for the
``template_id``.

`value_version` is optional; if defined it should be either "1", "2", "3", "4", "5", or "6".

As of version dev

the field `value_version` must not be set.

message NodeExercise
^^^^^^^^^^^^^^^^^^^^

*since version 10*

The exercise of a choice on a contract, selected from the available
choices in the associated DAML-LF template definition.

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message Identifier`_ template_id
* repeated ``string`` actors
* ``string`` choice
* `message VersionedValue`_ chosen_value
* ``bool`` consuming
* repeated ``string`` children
* repeated ``string`` stakeholders
* repeated ``string`` signatories
* `message VersionedValue`_ return_value
* `message KeyWithMaintainers`_ key_with_maintainers

``contract_id_struct`` is required. Its structure is defined by `the value
specification`_, version 3.

``children`` may be empty; all other fields are required, and required
to be non-empty.

``template_id``'s structure is defined by `the value specification`_;
the version of that specification to use when consuming it is the
``version`` field of ``chosen_value``.

``choice`` must be the name of a choice defined in the DAML-LF template
definition referred to by ``template_id``.

``chosen_value`` must conform to the DAML-LF argument type of the
``choice``.

``children`` is constrained as described under `field node_id`_.  Every
node referred to as one of ``children`` is another update to the ledger
taken as part of this transaction and as a consequence of exercising
this choice. Nodes in ``children`` appear in the order they were
created during interpretation.

Every element of ``actors``, ``stakeholders``, ``signatories``, and
``controllers`` is a party identifier.

.. note:: *This section is non-normative.*

  The ``stakeholders`` and ``signatories`` field have the same meaning
  they have for ``NodeCreate``.

* `return_value` is required

``key_with_maintainers`` is optional. It is present if and only if the
``template_id`` field refers to a template with a DAML-LF key
definition.  When present, the field's sub-fields ``key`` and
``maintainers`` must conform to the key definition for the
``template_id``.

As of version dev

A new field is included:

* repeated ``observer`` signatories

Every element of ``observer`` is a party identifier.

message NodeLookupByKey
^^^^^^^^^^^^^^^^^^^^^^^

*since version 10*

The lookup of a contract by contract key.

As of version 10, these fields are included:

* `message ContractId`_ contract_id_struct
* `message Identifier`_ template_id
* `message KeyWithMaintainers`_ key_with_maintainers

``contract_id_struct`` is optional. Its structure is defined by `the value
specification`_, version 3.

``template_id`` and ``key_with_maintainers`` are required. ``contract_id`` is optional: if a
contract with the specified key is not found it will not be present.

``template_id`` must refer to a template with a key definition.
Its structure is defined by `the value specification`_;
the version of that specification to use when consuming it is the
``version`` field of ``key``.

The ``key`` in ``key_with_maintainers`` must conform to the key definition in ``template_id``.

``template_id``'s structure is defined by `the value specification`_;
the version of that specification to use when consuming it is the
``version`` field of the ``key`` field in ``key_with_maintainers``.

.. _`the value specification`: value.rst
