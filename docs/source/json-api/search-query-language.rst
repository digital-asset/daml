.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Query Language
##############

The body of ``POST /v1/query`` looks like so:

.. code-block:: text

    {
        "templateIds": [...template IDs...],
        "query": {...query elements...}
    }

The elements of that query are defined here.

Fallback Rule
*************

Unless otherwise required by one of the other rules below or to follow,
values are interpreted according to :doc:`lf-value-specification`, and
compared for equality.

All types are supported by this simple equality comparison except:

- lists
- textmaps
- genmaps

Simple Equality
***************

Match records having at least all the (potentially nested) keys
expressed in the query. The result record may contain additional
properties.

Example: ``{ person: { name: "Bob" }, city: "London" }``

- Match: ``{ person: { name: "Bob", dob: "1956-06-21" }, city: "London",
  createdAt: "2019-04-30T12:34:12Z" }``
- No match: ``{ person: { name: "Bob" }, city: "Zurich" }``
- Typecheck failure: ``{ person: { name: ["Bob", "Sue"] }, city:
  "London" }``

A JSON object, when considered with a record type, is always interpreted
as a field equality query. Its type context is thus mutually exclusive
with comparison queries.

Comparison Query
****************

Match values on comparison operators for int64, numeric, text, date, and
time values. Instead of a value, a key can be an object with one or more
operators: ``{ <op>: value }`` where ``<op>`` can be:

- ``"%lt"`` for less than
- ``"%gt"`` for greater than
- ``"%lte"`` for less than or equal to
- ``"%gte"`` for greater than or equal to

``"%lt"`` and ``"%lte"`` may not be used at the same time, and likewise
with ``"%gt"`` and ``"%gte"``, but all other combinations are allowed.

Example:  ``{ "person" { "dob": { "%lt": "2000-01-01", "%gte": "1980-01-01" } } }``

- Match: ``{ person: { dob: "1986-06-21" } }``
- No match: ``{ person: { dob: "1976-06-21" } }``
- No match: ``{ person: { dob: "2006-06-21" } }``

These operators cannot occur in objects interpreted in a record context,
nor may other keys than these four operators occur where they are legal,
so there is no ambiguity with field equality.

Appendix: Type-aware Queries
****************************

**This section is non-normative.**

This is not a *JSON* query language, it is a *Daml-LF* query
language. So, while we could theoretically treat queries (where not
otherwise interpreted by the "may contain additional properties" rule
above) without concern for what LF type (i.e. template) we're
considering, we *will not* do so.

Consider the subquery ``{"foo": "bar"}``. This query conforms to types,
among an unbounded number of others::

  record A ↦ { foo : Text }
  record B ↦ { foo : Optional Text }
  variant C ↦ foo : Party | bar : Unit

  // NB: LF does not require any particular case for VariantCon or Field;
  // these are perfectly legal types in Daml-LF packages

In the cases of ``A`` and ``B``, ``"foo"`` is part of the query
language, and only ``"bar"`` is treated as an LF value; in the case of
``C``, the whole query is treated as an LF value. The wide variety of
ambiguous interpretations about what elements are interpreted, and what
elements treated as literal, and *how* those elements are interpreted or
compared, would preclude many techniques for efficient query compilation
and LF value representation that we might otherwise consider.

Additionally, it would be extremely easy to overlook unintended meanings
of queries when writing them, and impossible in many cases to suppress
those unintended meanings within the query language. For example, there
is no way that the above query could be written to match ``A`` but never
``C``.

For these reasons, as with LF value input via JSON, queries written in
JSON are also always interpreted with respect to some specified LF types
(e.g. template IDs). For example:

.. code-block:: json

    {
        "templateIds": ["Foo:A", "Foo:B", "Foo:C"],
        "query": {"foo": "bar"}
    }

will treat ``"foo"`` as a field equality query for A and B, and
(supposing templates' associated data types were permitted to be
variants, which they are not, but for the sake of argument) as a whole
value equality query for C.

The above "Typecheck failure" happens because there is no LF type to
which both ``"Bob"`` and ``["Bob", "Sue"]`` conform; this would be
caught when interpreting the query, before considering any contracts.

Appendix: Known Issues
**********************

When Using Oracle, Queries Fail if a Token Is Too Large
=======================================================

This limitation is exclusive to users of the HTTP JSON API using Daml Enterprise support for Oracle. Due to a known limitation in Oracle, the full-test JSON search index on the contract payloads rejects query tokens larger than 256 bytes. This limitations shouldn't impact most workloads, but if this needs to be worked around, the HTTP JSON API server can be started passing the additional ``disableContractPayloadIndexing=true`` (after wiping an existing query store database, if necessary).

`Issue on GitHub <https://github.com/digital-asset/daml/issues/10780>`__

