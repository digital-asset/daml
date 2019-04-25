.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML-LF Value Specification
===========================

**version 4, 2 April 2019**

The DAML-LF language includes ways to define *data types*,
specifications of structure, and includes rules by which a restricted
subset of those data types are considered *serializable*.

This specification, in concert with the ``value.proto`` machine-readable
definition, defines a format by which *values* of *serializable DAML-LF
types* may be represented.  Only such *serializable values* may be
stored on a ledger.

Values are typically consumed in tandem with related DAML-LF type
information, so many fields that may be inferred with this information
are optional.  For example, `message RecordField`_ names are defined as
part of their respective LF datatypes, so there is no need to repeat
that information in the record value.

Do not read this without ``value.proto``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``value.proto`` defines the baseline rules for values; that file must be
consulted in concert with this document for a full specification of
DAML-LF values.  Except where required for clarity, we do not repeat
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

If you are constructing a DAML-LF value, it is not sufficient to merely
conform to the structure defined in ``value.proto``; you must also
conform to the rules defined in this document.  A value that happens to
conform to ``value.proto``, yet violates some rule of this document, is
not a valid DAML-LF value.

Backward compatibility
^^^^^^^^^^^^^^^^^^^^^^

DAML-LF values are accompanied by a version identifier; every change to
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

Future versions of this specification must conform to the `DAML-LF
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

.. _`DAML-LF Governance process`: ../governance.rst
.. _`transaction`: transaction.rst

Version history
^^^^^^^^^^^^^^^

This table lists every version of this specification in ascending order
(oldest first).

+--------------------+-----------------+
| Version identifier | Date introduced |
+====================+=================+
|                  1 |      2018-12-13 |
+--------------------+-----------------+
|                  2 |      2019-01-25 |
+--------------------+-----------------+
|                  3 |      2019-02-14 |
+--------------------+-----------------+
|                  4 |      2019-03-27 |
+--------------------+-----------------+

message VersionedValue
^^^^^^^^^^^^^^^^^^^^^^

*since version 1*

The outermost entry point, a `message Value`_ with version annotation.

In this version, these fields are included:

* ``string`` version
* `message Value`_ value

``version`` is required, and must be a version of this specification.
For example, for version 1 of this specification, ``version`` must be
``"1"``.  Consumers can expect this field to be present and to
have the semantics defined here without knowing the version of this
value in advance.

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

*since version 1*
 
An actual DAML-LF *serializable value*.

As of version 1, may be any one of these:

* `message Record`_ record
* `message Variant`_ variant
* ``string`` `field contract_id`_
* `message List`_ list
* ``sint64`` int64
* ``string`` `field decimal`_
* ``string`` text
* ``sfixed64`` `field timestamp`_
* ``string`` `field party`_
* ``bool`` bool
* ``Empty`` `field unit`_
* ``int32`` `field date`_

``Value`` is recursive by virtue of occurrences in some of the above
cases, e.g. ``list`` contains any number of ``Value``. The maximum depth
of nested ``Value``, including the outermost, is 100; any more yields an
invalid value.

*since version 2*

As of version 2, may be any one of the above, or this:

* `message Optional`_ optional

*since version 3*

As of version 3, may be any one of the above *except for* `field
contract_id`_, which is no longer allowed.  Instead, the value may be
this:

* `message ContractId`_ contract_id_struct

*since version 4*

As of version 4, may be any one of the above, or this:

* `message Map`_ map


field contract_id
~~~~~~~~~~~~~~~~~

*since version 1*

Its text must be a valid contract ID.

field decimal
~~~~~~~~~~~~~

*since version 1*

Expresses a number in [–(10³⁸–1)÷10¹⁰, (10³⁸–1)÷10¹⁰] with at most 10
digits of decimal precision.  In other words, in base-10, a number with
28 digits before the decimal point and up to 10 after the decimal point.
A leading sign, + or -, may be optionally included.  In regular
expression terms::

  [+-]?[0-9]{1,28}(\.[0-9]{1,10})?

Any value that does not conform, either by being outside the range or
having too many decimal digits or for any other reason, must be rejected
as an invalid message; consumers must not round, overflow, or otherwise
try to compensate for "bad" input when reading decimal fields.  As such,
value producers should take care to properly format these decimals.

It may seem strange that the value specification uses ``string`` here
rather than a protobuf-supported numeric type; however, none of
protobuf's numeric types have the proper precision for this field.

field timestamp
~~~~~~~~~~~~~~~

*since version 1*

The number of microseconds since 1970-01-01T00:00:00Z, with that epoch
being 0.  The allowed range is 0001-01-01T00:00:00Z to
9999-12-31T23:59:59.999999Z, inclusive; while ``sfixed64`` supports
numbers outside that range, such timestamps are not allowed and must be
rejected with error by conforming consumers.

field party
~~~~~~~~~~~

*since version 1*

A party identifier; unlike arbitrary text, this will be interpreted
with respect to the ledger under consideration by whatever command
contains this value. Party identifiers are restricted to be a
non-empty string of printable US-ASCII characters (characters ranging
from '\32' to '\127').

field unit
~~~~~~~~~~

*since version 1*

While ``Empty`` contains no information, conforming consumers are
permitted to expect this member of `message Value`_ to be chosen
correctly in appropriate contexts.  So if the ``Value``'s DAML-LF type
is ``Unit``, a consumer *may* reject the message if the ``Value`` is not
the ``unit`` member of the sum, so value producers must take care to
select this member and not another value as a placeholder (e.g. 0,
false, empty text) in such cases.

field date
~~~~~~~~~~

*since version 1*

The number of days since 1970-01-01, with that epoch being 0.  The
allowed range is 0001-01-01 to 9999-12-31, inclusive; while ``int32``
supports numbers outside that range, such dates are not allowed and must
be rejected with error by conforming consumers.

message Record
^^^^^^^^^^^^^^

*since version 1*

The core primitive for combining `message Value`_ of different type into
a single value.

As of version 1, these fields are included:

* `message Identifier`_ `field record_id`_
* repeated `message RecordField`_ fields

field record_id
~~~~~~~~~~~~~~~

*since version 1*

The fully-qualified `message Identifier`_ of the DAML-LF record type.
It may be omitted.

field fields
~~~~~~~~~~~~

*since version 1*

Zero or more `message RecordField`_ values.

The number and types of values in the fields must match the DAML-LF
record type associated with the `message Record`_, whether that record
type is inferred from context or explicitly supplied as a `field
record_id`_.

Additionally, the *order* of fields must match the order in which they
are declared in DAML-LF for that record type.  Neither producers nor
consumers are permitted to use ``label`` to reorder the fields.

So, for example, it is unsafe to use a ``Map``, ``HashMap``, or some
such as a trivial intermediate representation of fields, because
enumerating it will likely output fields in the wrong order; if such a
structure is used, you must use the LF record type information to output
the fields in the correct order.

message RecordField
^^^^^^^^^^^^^^^^^^^

*since version 1*

One of `field fields`_.

As of version 1, these fields are included:

* ``string`` label
* `message Value`_ value

``label`` may be an empty string.  However, if ``label`` is non-empty,
it must match the name of the field in this position in the DAML-LF
record type under consideration.  For example, if the second field of an
LF record type is named ``bar``, then label of the second element of
`field fields`_ may be ``"bar"``, or an empty string in circumstances
mentioned above.  Any other label produces an invalid LF value.

The ``value`` field must conform to the type of this field of the
containing record, as declared by the LF record type.  It must be
supplied in all cases.

message Identifier
^^^^^^^^^^^^^^^^^^

*since version 1*

A reference to a DAML-LF record or variant type.

As of version 1, these fields are included, all required to be
non-empty:

* ``string`` package_id
* repeated ``string`` module_name
* repeated ``string`` name

``package_id`` is a DAML-LF package ID, indicating the LF package in
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

*since version 1*

The core primitive for injecting `message Value`_ of different type into
a single type at runtime.

As of version 1, these fields are included:

* `message Identifier`_ `field variant_id`_
* ``string`` `field constructor`_
* `message Value`_ value

Only ``Variant`` may be used to encode a Value that conforms to an LF
variant type.  Alternative encodings are not permitted.

``value`` is required, and must conform to the LF type selected by the
`field constructor`_.

field variant_id
~~~~~~~~~~~~~~~~

*since version 1*

The fully-qualified `message Identifier`_ of the DAML-LF variant type.
It may be omitted.

field constructor
~~~~~~~~~~~~~~~~~

*since version 1*

The name of the variant alternative selected for this variant value.
Required.

For example, given the LF variant::

  data E = L Text | R Text

A `message Variant`_ conforming to ``E`` may have in this field ``"L"``
or ``"R"``; any other ``constructor`` yields an invalid Value.

message ContractId
^^^^^^^^^^^^^^^^^^

*since version 3*

A reference to a contract, either absolute or relative.

As of version 3, these fields are included:

* ``string`` contract_id
* ``bool`` relative

``contract_id`` must conform to the regular expression::

  [A-Za-z0-9._:-]+

If ``relative`` is unset or false, this is an absolute contract ID;
otherwise, this is a relative contract ID.

message List
^^^^^^^^^^^^

*since version 1*

A homogenous list of values.

As of version 1, these fields are included:

* repeated `message Value`_ elements

Every member of ``elements`` must conform to the same type.

message Optional
^^^^^^^^^^^^^^^^

*since version 2*

An optional value (equivalent to Scala's ``Option`` or Haskell's
``Maybe``).

Only ``Optional`` may be used to encode a Value that conforms to an LF
option type.  Alternative encodings are not permitted.

In this version, these fields are included:

* `message Value`_ value

The ``value`` field is optional, embodying the semantics of the
``Optional`` type.


message Map.Entry
^^^^^^^^^^^^^^^^^

*since version 4*

A map entry (key-value pair) used to build `message Map`_.

As of version 4, these fields are included:

* string key

* `message Value`_ value

Both ``key`` and ``value`` are required.

message Map
^^^^^^^^^^^

*since version 4*

A homogeneous map of values.

In this version, these fields are include:

* repeated `message Map.Entry`_ entries

The ``value`` field of every member of ``entries`` must conform to the
same type. Furthermore,the ``key`` fields of the entries must be distinct.
Entries may occur in arbitrary order.
