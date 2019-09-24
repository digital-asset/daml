.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

/contracts/search query language
================================

The body of ``POST /contracts/search`` looks like so::

  {"%templates": [...template IDs...],
   ...other query elements...}

The elements of that query are defined here.

Fallback rule
-------------

Unless otherwise required by one of the other rules below or to follow,
values are interpreted according to the `LF values' JSON format
<../lf-value-json/specification.rst>`_, and compared for equality.

Simple equality
---------------

Match records having at least all the (potentially nested) keys
expressed in the query. The result record may contain additional
properties.

Example: ``{ person: { name: "Bob" }, city: "London" }``

- Match: ``{ person: { name: "Bob", dob: "1956-06-21" }, city: "London",
  createdAt: "2019-04-30T12:34:12Z" }``
- No match: ``{ person: { name: "Bob" }, city: "Zurich" }``
- Typecheck failure: ``{ person: { name: ["Bob", "Sue"] }, city:
  "London" }``

Example: ``{ favorites: ["vanilla", "chocolate"] }``

- Match: ``{ favorites: ["vanilla", "chocolate"] }``
- No match: ``{ favorites: ["chocolate", "vanilla"] }``
- No match: ``{ favorites: ["vanilla", "strawberry"] }``
- No match: ``{ favorites: ["vanilla", "chocolate", "strawberry"] }``

A JSON object, when considered with a record type, is always interpreted
as a field equality query. Its type context is thus mutually exclusive
with `the forthcoming comparison queries
<https://github.com/digital-asset/daml/issues/2780>`_.

Appendix: Type-aware queries
----------------------------

**This section is non-normative.**

This is not a *JSON* query language, it is a *DAML-LF* query
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
  // these are perfectly legal types in DAML-LF packages

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
(e.g. template IDs). As #2777 implies, for example::

  {"%templates": [{"moduleName": "Foo", "entityName": "A"},
                  {"moduleName": "Foo", "entityName": "B"},
                  {"moduleName": "Foo", "entityName": "C"}],
   "foo": "bar"}

will treat ``"foo"`` as a field equality query for A and B, and
(supposing templates' associated data types were permitted to be
variants, which they are not, but for the sake of argument) as a whole
value equality query for C.

The above "Typecheck failure" happens because there is no LF type to
which both ``"Bob"`` and ``["Bob", "Sue"]`` conform; this would be
caught when interpreting the query, before considering any contracts.
