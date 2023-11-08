.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml-LF Value Specification
===========================

**version 15, 22 June 2021**

The Daml-LF language includes ways to define *data types*,
specifications of structure, and includes rules by which a restricted
subset of those data types are considered *serializable*.

This specification, in concert with the ``value.proto`` machine-readable
definition, defines a format by which *values* of *serializable Daml-LF
types* may be represented.  Only such *serializable values* may be
stored on a ledger.

Values are typically consumed in tandem with related Daml-LF type
information, so many fields that may be inferred with this information
are optional.  For example, `message RecordField`_ names are defined as
part of their respective LF datatypes, so there is no need to repeat
that information in the record value.

Do not read this without ``value.proto``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``value.proto`` defines the baseline rules for values; that file must be
consulted in concert with this document for a full specification of
Daml-LF values.  Except where required for clarity, we do not repeat
rules defined and enforced in that file within this document.  When
consulting the section on each message type, you must also refer to the
same definition in ``value.proto`` for a full definition of the
requirements for that message.

This document is insufficiently detailed to construct a correct value;
you must refer to ``value.proto`` as well to have a full set of rules.

Do not read ``value.proto`` without this
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the same fashion, ``value.proto`` by itself cannot be consulted for a
full specification of the value format, because it is impossible to
define all the requirements for values in the ``.proto`` format.  All
such rules are included in this document, instead.

If you are constructing a Daml-LF value, it is not sufficient to merely
conform to the structure defined in ``value.proto``; you must also
conform to the rules defined in this document.  A value that happens to
conform to ``value.proto``, yet violates some rule of this document, is
not a valid Daml-LF value.

Backward compatibility
^^^^^^^^^^^^^^^^^^^^^^

Daml-LF values are accompanied by a version identifier; every change to
``value.proto`` entails a change to this specification, and every change
to this specification introduces a unique new version.  `Version
history`_ defines a total ordering of all past versions; any version *y*
not listed there should be considered *y>k* for any known version *k*.

A value of version *n* may be interpreted by consulting any version of
this document *m≥n*.  Likewise, any version *q* of the value
specification is sufficient to interpret values of any version *r≤q*.
In other words, later versions of this specification preserve the
semantics and parseability of earlier versions; there is no need to
consult or implement older versions in order to interpret any value, and
you may always simply consult the latest appropriate version.

By contrast, a value of version *s* must be rejected by a
consumer that implements this specification of version *t<s*.  So if you
produce a value of version *s*, you may assume that its consumer either
implements some version *u≥s*, or will reject the message containing the
value.  The ``.proto`` format may make parsing such values possible, but
that is irrelevant; semantics defined in later versions of this
specification may be vital for correctly interpreting values defined
under those later versions, and you must not suppose otherwise.

For example, suppose you have a value of version 3.  You can expect it
to be interpreted the same by consumers implementing specification
version 3, 5, 5000, and so on.  On the other hand, you can expect a
consumer of version 1 or 2 to reject the message containing that value,
because specification version 3 might define some semantics vital to
understanding your value.

"since version"
~~~~~~~~~~~~~~~

Every message type and field is accompanied by one or more *since
version x* annotations in this document, preceding some description of
semantics.  This defines when that message, field, or semantic rule was
introduced in the value specification.  Where there are multiple
overlapping definitions of semantics for the same field of the form
*since version x*, the correct interpretation for any value of version
*y* is that under the greatest *x* such that *x≤y*, failing that the
second-greatest such *x*, and so on.

For example, suppose you have received values of versions 4 and 5, and
this document defines overlapping semantics for *since version 2*, *4*,
and *6*.  You should interpret both values according to the *since
version 4* section, also relying on *since version 2* where not in
conflict with *since version 4*; however, the *since version 6* section
must be ignored entirely.

Changing this specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Future versions of this specification must conform to the `Daml-LF
Governance process`_ and preserve all invariants described above.  Where
these are in conflict, the governance process takes priority.

If you introduce version *z*, your changes to this document should
almost certainly all occur under *since version z* annotations; a new
version without any such annotations in either this or the
`transaction`_ specification is probably not a new version at all.
Simply updating the semantic descriptions is tantamount to retroactively
editing the specification of older versions, and is a violation of
governance except where correction of the description of those older
versions is actually desirable.

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

.. _`Daml-LF Governance process`: ../governance.rst
.. _`transaction`: transaction.rst

Version history
^^^^^^^^^^^^^^^

This table lists every version of this specification in ascending order
(oldest first).

Support for value versions 1 to 5 was dropped on 2020-12-10.
This breaking change does not impact ledgers created with SDK 1.0.0 or
later.

Support for transaction 13 or older was dropped on 2023-10-13
This breaking change does not impact ledgers created with Canton 2.0.0 or
later.

+--------------------+-----------------+
| Version identifier | Date introduced |
+====================+=================+
|                 14 |      2021-22-06 |
+--------------------+-----------------+
|                 15 |      2022-07-29 |
+--------------------+-----------------+
|                dev |                 |
+--------------------+-----------------+

message VersionedValue
^^^^^^^^^^^^^^^^^^^^^^

The outermost entry point, a `message Value`_ with version annotation.

In this version, these fields are included:

* ``string`` version
* `message Value`_ value

``version`` is required, and must be a version of this specification.
For backward compatibility reasons:
- the version `10` is encoded as the string "6";
- string "10" is reserved and will be never used to encoded any future version;
- versions 11 or latter will be encoded as string, for instance
  version 11 of this specification, ``version`` must be ``"11"``.
  
Consumers can expect this field to be present and to have the
semantics defined here without knowing the version of this value in
advance.

Known versions are listed in ascending order in `Version history`_; any
``version`` not in this list should be considered newer than any version
in same list, and consumers must reject values with such unknown
versions.

``value`` is required.

``VersionedValue`` does not participate in the general recursion of
`message Value`_ itself, because every whole ``Value`` must be
interpreted only according to a single version of this specification.

message Value
^^^^^^^^^^^^^

An actual Daml-LF *serializable value*.

(*since version 14*)

As of version 14, may be any one of these:

* `message Record`_ record
* `message Variant`_ variant
* `message ContractId`_ contract_id_struct
* `message List`_ list
* ``sint64`` int64
* ``string`` `field numeric`_
* ``string`` text
* ``sfixed64`` `field timestamp`_
* ``string`` `field party`_
* ``bool`` bool
* ``Empty`` `field unit`_
* ``int32`` `field date`_
* `message Optional`_ optional
* `message Map`_ map
* `message Enum`_ enum
* `message Numeric`_ numeric
* `message GenMap`_ gen_map

``Value`` is recursive by virtue of occurrences in some of the above
cases, e.g. ``list`` contains any number of ``Value``. The maximum
depth of a nested ``Value``, including the outermost, is 100; any more
yields an invalid value.

field contract_id
~~~~~~~~~~~~~~~~~

(*since version 14*)

Its text must be a valid contract ID.

field numeric
~~~~~~~~~~~~~

(*since version 14*)

Expresses a signed number that can be represented in base-10 without
loss of precision with at most 38 digits and with a scale between 0
and 37 (bounds inclusive). In other words, in base-10, a number with
at most 38 digits from which at most 37 appears on the right hand side
of the decimal point.  A leading `-` sign may be optionally included
to indicate negative number. In regular expression terms::

  -?([1-9][0-9]*|0)\.[0-9]*

with the additional constraint that the string must contain at most 38
digits.

Any value that does not conform, either by being outside the range or
having too many decimal digits or for any other reason, must be
rejected as an invalid message; consumers must not round, overflow, or
otherwise try to compensate for "bad" input when reading decimal
fields.  As such, value producers should take care to properly format
these decimals.


field timestamp
~~~~~~~~~~~~~~~

(*since version 14*)

The number of microseconds since 1970-01-01T00:00:00Z, with that epoch
being 0.  The allowed range is 0001-01-01T00:00:00Z to
9999-12-31T23:59:59.999999Z, inclusive; while ``sfixed64`` supports
numbers outside that range, such timestamps are not allowed and must be
rejected with error by conforming consumers.

field party
~~~~~~~~~~~

(*since version 14*)

A party identifier; unlike arbitrary text, this will be interpreted
with respect to the ledger under consideration by whatever command
contains this value. Party identifiers are restricted to be a
non-empty string of printable US-ASCII characters (characters ranging
from '\32' to '\127').

field unit
~~~~~~~~~~

(*since version 14*)

While ``Empty`` contains no information, conforming consumers are
permitted to expect this member of `message Value`_ to be chosen
correctly in appropriate contexts.  So if the ``Value``'s Daml-LF type
is ``Unit``, a consumer *may* reject the message if the ``Value`` is not
the ``unit`` member of the sum, so value producers must take care to
select this member and not another value as a placeholder (e.g. 0,
false, empty text) in such cases.

field date
~~~~~~~~~~

(*since version 14*)

The number of days since 1970-01-01, with that epoch being 0.  The
allowed range is 0001-01-01 to 9999-12-31, inclusive; while ``int32``
supports numbers outside that range, such dates are not allowed and must
be rejected with error by conforming consumers.

message Record
^^^^^^^^^^^^^^

(*since version 14*)

The core primitive for combining `message Value`_ of different type into
a single value.

As of version 10, these fields are included:

* `message Identifier`_ record_id
* repeated `message RecordField`_ fields

``record_id`` must be the unused

message RecordField
^^^^^^^^^^^^^^^^^^^

(*since version 14*)

One of `field fields`_.

As of version 14, these fields are included:

* ``string`` label
* `message Value`_ value

``label`` may be unused and Value is required.

message Identifier
^^^^^^^^^^^^^^^^^^

(*since version 14*)

A reference to a Daml-LF record or variant type.

As of version 10, these fields are included, all required to be
non-empty:

* ``string`` package_id
* repeated ``string`` module_name
* repeated ``string`` name


``package_id`` is a Daml-LF package ID, indicating the LF package in
which the type is defined. package ID are restricted to be a
non-empty string of printable US-ASCII characters (characters ranging
from '\32' to '\127').

``module_name`` lists the components of the name of the module within
that package.

``name`` lists the components of the name of the type declaration within
that module.

Each component of ``module_name`` and ``name`` must be non empty. Moreover,
we restrict each component as follows:

* The first character must be ``$``, ``_``, or an ASCII letter;
* Every other character must be ``$``, ``_``, an ASCII letter, or an
  ASCII digit.

message Variant
^^^^^^^^^^^^^^^

(*since version 10*)

The core primitive for injecting `message Value`_ of different type into
a single type at runtime.

As of version 14, these fields are included:

* `message Identifier`_ variant_id
* ``string`` `field constructor`_
* `message Value`_ value

Both ``Constructor`` and ``value`` are required while ``variant_id`` must be unused

All the fields are required.

.. note: *this section is non-normative*

   ``value`` must conform to the LF type selected by the `field
   constructor`_.


field variant_id
~~~~~~~~~~~~~~~~

(*since version 14*)

The fully-qualified `message Identifier`_ of the Daml-LF variant type.
It may be omitted.

field constructor
~~~~~~~~~~~~~~~~~

The name of the variant alternative selected for this variant value.
Required.

For example, given the LF variant::

  data E = L Text | R Text

A `message Variant`_ conforming to ``E`` may have in this field ``"L"``
or ``"R"``; any other ``constructor`` yields an invalid Value.

message ContractId
^^^^^^^^^^^^^^^^^^

(*since version 14*)

A reference to a contract, either absolute or relative.

As of version 14, this field is included:

* ``string`` contract_id

``contract_id`` must conform to the regular expression::

  [A-Za-z0-9._:-]+

message List
^^^^^^^^^^^^

(*since version 14*)

A homogenous list of values.

As of version 10, these fields are included:

* repeated `message Value`_ elements

.. note: *this section is non-normative*

    Every member of ``elements`` must conform to the same type.

message Optional
^^^^^^^^^^^^^^^^

(*since version 14*)

An optional value (equivalent to Scala's ``Option`` or Haskell's
``Maybe``).

In this version, these fields are included:

* `message Value`_ value

The ``value`` field is optional, embodying the semantics of the
``Optional`` type.

message Map.Entry
^^^^^^^^^^^^^^^^^

(*since version 14*)

A map entry (key-value pair) used to build `message Map`_.

As of version 14, these fields are included:

* string key

* `message Value`_ value

Both ``key`` and ``value`` are required.

message Map
^^^^^^^^^^^

(*since version 14*)

A homogeneous map where keys are strings.

In this version, these fields are included:

* repeated `message Map.Entry`_ entries

.. note: *this section is non-normative*

   The ``value`` field of every member of ``entries`` must conform to
   the same type.  If two ore more entries have the same keys, the
   last one overrides the former entry. Entries with different key may
   occur in arbitrary order.

message Enum
^^^^^^^^^^^^

(*since version 14*)

An Enum value, a specialized form of variant without argument.

In this version, these fields are included:

* `message Identifier`_ enum_id
* ``string`` value

Field ``value`` is required while ``variant_id`` must be unused

.. note: *this section is non-normative*

   ``value`` must to be one of the values of the enum type to which
   this ``message Enum`` conforms.

message GenMap.Entry
^^^^^^^^^^^^^^^^^

(*since version 14*)

A map entry (key-value pair) used to build `message GenMap`_.

As of version 11, these fields are included:

* `message Value`_  key

* `message Value`_ value

Both ``key`` and ``value`` are required.

message GenMap
^^^^^^^^^^^

(*since version 14*)

A map where keys and values are homogeneous.

In this version, these fields are included:

* repeated `message GenMap.Entry`_ entries

.. note: *this section is non-normative*

   The ``value`` field of every member of ``entries`` must conform to
   the same type.  The ``key`` field of every member of ``entries``
   must conform to the same type. If two ore more entries have the
   same keys, the last one overrides the former entry.  Entries with
   different key may occur in arbitrary order.

