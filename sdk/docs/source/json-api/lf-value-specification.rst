.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml-LF JSON Encoding
#####################

We describe how to decode and encode Daml-LF values as JSON. For each
Daml-LF type we explain what JSON inputs we accept (decoding), and what
JSON output we produce (encoding).

Codec Library
*************

At the library level, the output format is parameterized by two flags::

    encodeDecimalAsString: boolean
    encodeInt64AsString: boolean

The suggested defaults for both of these flags is false. If the intended
recipient is written in JavaScript, however, note that the JavaScript data
model will decode these as numbers, discarding data in some cases;
encode-as-String avoids this, as mentioned with respect to ``JSON.parse``
below. **For that reason, the HTTP JSON API Service uses ``true`` for both flags.**

Type-directed Parsing
*********************

Note that throughout the document the decoding is type-directed. In
other words, the same JSON value can correspond to many Daml-LF values,
and a single Daml-LF value can correspond to multiple JSON encodings. This
means it is crucial to know the expected type of a JSON-encoded LF value to
make sense of it.

For that reason, you should parse the data into appropriate data types
(including parsing numbers into appropriate representations) before doing any
meaningful manipulations (e.g. comparison for equality).

ContractId
**********

Contract ids are expressed as their string representation::

    "123"
    "XYZ"
    "foo:bar#baz"

Decimal
*******

Input
=====

Decimals can be expressed as JSON numbers or as JSON strings. JSON
strings are accepted using the same format that JSON accepts, and
treated them as the equivalent JSON number::

    -?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?

Note that JSON numbers would be enough to represent all
Decimals. However, we also accept strings because in many languages
(most notably JavaScript) use IEEE Doubles to express JSON numbers, and
IEEE Doubles cannot express Daml-LF Decimals correctly. Therefore, we
also accept strings so that JavaScript users can use them to specify
Decimals that do not fit in IEEE Doubles.

Numbers must be within the bounds of Decimal, [–(10³⁸–1)÷10¹⁰,
(10³⁸–1)÷10¹⁰]. Numbers outside those bounds will be rejected. Numbers
inside the bounds will always be accepted, using banker's rounding to
fit them within the precision supported by Decimal.

A few valid examples::

    42 --> 42
    42.0 --> 42
    "42" --> 42
    9999999999999999999999999999.9999999999 -->
        9999999999999999999999999999.9999999999
    -42 --> -42
    "-42" --> -42
    0 --> 0
    -0 --> 0
    0.30000000000000004 --> 0.3
    2e3 --> 2000

A few invalid examples::

    "  42  "
    "blah"
    99999999999999999999999999990
    +42

Output
======

If encodeDecimalAsString is set, decimals are encoded as strings, using
the format ``-?[0-9]{1,28}(\.[0-9]{1,10})?``. If encodeDecimalAsString
is not set, they are encoded as JSON numbers, also using the format
``-?[0-9]{1,28}(\.[0-9]{1,10})?``.

Note that the flag encodeDecimalAsString is useful because it lets
JavaScript consumers consume Decimals safely with the standard
JSON.parse.

Int64
*****

Input
=====

Int64, much like Decimal, can be represented as JSON numbers and as
strings, with the string representation being ``[+-]?[0-9]+``. The
numbers must fall within [-9223372036854775808,
9223372036854775807]. Moreover, if represented as JSON numbers, they
must have no fractional part.

A few valid examples::

    42
    "+42"
    -42
    0
    -0
    9223372036854775807
    "9223372036854775807"
    -9223372036854775808
    "-9223372036854775808"

A few invalid examples::

    42.3
    +42
    9223372036854775808
    -9223372036854775809
    "garbage"
    "   42 "

Output
======

If encodeInt64AsString is set, Int64s are encoded as strings, using the
format ``-?[0-9]+``. If encodeInt64AsString is not set, they are encoded as
JSON numbers, also using the format ``-?[0-9]+``.

Note that the flag encodeInt64AsString is useful because it lets
JavaScript consumers consume Int64s safely with the standard
``JSON.parse``.

Timestamp
*********

Input
=====

Timestamps are represented as ISO 8601 strings, rendered using the
format ``yyyy-mm-ddThh:mm:ss.ssssssZ``::

    1990-11-09T04:30:23.123456Z
    9999-12-31T23:59:59.999999Z

Parsing is a little bit more flexible and uses the format
``yyyy-mm-ddThh:mm:ss(\.s+)?Z``, i.e. it's OK to omit the microsecond part
partially or entirely, or have more than 6 decimals. Sub-second data beyond
microseconds will be dropped. The UTC timezone designator must be included. The
rationale behind the inclusion of the timezone designator is minimizing the
risk that users pass in local times. Valid examples::

    1990-11-09T04:30:23.1234569Z
    1990-11-09T04:30:23Z
    1990-11-09T04:30:23.123Z
    0001-01-01T00:00:00Z
    9999-12-31T23:59:59.999999Z

The timestamp must be between the bounds specified by Daml-LF and ISO
8601, [0001-01-01T00:00:00Z, 9999-12-31T23:59:59.999999Z].

JavaScript

::

    > new Date().toISOString()
    '2019-06-18T08:59:34.191Z'

Python

::

    >>> datetime.datetime.utcnow().isoformat() + 'Z'
    '2019-06-18T08:59:08.392764Z'

Java

::

    import java.time.Instant;
    class Main {
        public static void main(String[] args) {
            Instant instant = Instant.now();
            // prints 2019-06-18T09:02:16.652Z
            System.out.println(instant.toString());
        }
    }

Output
======

Timestamps are encoded as ISO 8601 strings, rendered using the format
``yyyy-mm-ddThh:mm:ss[.ssssss]Z``.

The sub-second part will be formatted as follows:

- If no sub-second part is present in the timestamp (i.e. the timestamp
  represents whole seconds), the sub-second part will be omitted
  entirely;
- If the sub-second part does not go beyond milliseconds, the sub-second
  part will be up to milliseconds, padding with trailing 0s if
  necessary;
- Otherwise, the sub-second part will be up to microseconds, padding
  with trailing 0s if necessary.

In other words, the encoded timestamp will either have no sub-second
part, a sub-second part of length 3, or a sub-second part of length 6.

Party
*****

Represented using their string representation, without any additional
quotes::

    "Alice"
    "Bob"

Unit
****

Represented as empty object ``{}``. Note that in JavaScript ``{} !==
{}``; however, ``null`` would be ambiguous; for the type ``Optional
Unit``, ``null`` decodes to ``None``, but ``{}`` decodes to ``Some ()``.

Additionally, we think that this is the least confusing encoding for
Unit since unit is conceptually an empty record.  We do not want to
imply that Unit is used similarly to null in JavaScript or None in
Python.

Date
****

Represented as an ISO 8601 date rendered using the format
``yyyy-mm-dd``::

    2019-06-18
    9999-12-31
    0001-01-01

The dates must be between the bounds specified by Daml-LF and ISO 8601,
[0001-01-01, 9999-12-31].

Text
****

Represented as strings.

Bool
****

Represented as booleans.

Record
******

Input
=====

Records can be represented in two ways. As objects::

    { f₁: v₁, ..., fₙ: vₙ }

And as arrays::

    [ v₁, ..., vₙ ]

Note that Daml-LF record fields are ordered. So if we have

::

    record Foo = {f1: Int64, f2: Bool}

when representing the record as an array the user must specify the
fields in order::

    [42, true]

The motivation for the array format for records is to allow specifying
tuple types closer to what it looks like in Daml. Note that a Daml
tuple, i.e. (42, True), will be compiled to a Daml-LF record ``Tuple2 {
_1 = 42, _2 = True }``.

Output
======

Records are always encoded as objects.

List
****

Lists are represented as

::

    [v₁, ..., vₙ]

TextMap
*******

TextMaps are represented as objects:

::

    { k₁: v₁, ..., kₙ: vₙ }

GenMap
******

GenMaps are represented as lists of pairs::

    [ [k₁, v₁], [kₙ, vₙ] ]

Order does not matter.  However, any duplicate keys will cause the map
to be treated as invalid.

Optional
********

Input
=====

Optionals are encoded using ``null`` if the value is None, and with the
value itself if it's Some. However, this alone does not let us encode
nested optionals unambiguously. Therefore, nested Optionals are encoded
using an empty list for None, and a list with one element for Some. Note
that after the top-level Optional, all the nested ones must be
represented using the list notation.

A few examples, using the form

::

    JSON  -->  Daml-LF  :  Expected Daml-LF type

to make clear what the target Daml-LF type is::

    null    -->  None                  : Optional Int64
    null    -->  None                  : Optional (Optional Int64)
    42      -->  Some 42               : Optional Int64
    []      -->  Some None             : Optional (Optional Int64)
    [42]    -->  Some (Some 42)        : Optional (Optional Int64)
    [[]]    -->  Some (Some None)      : Optional (Optional (Optional Int64))
    [[42]]  -->  Some (Some (Some 42)) : Optional (Optional (Optional Int64))
    ...

Finally, if Optional values appear in records, they can be omitted to
represent None. Given Daml-LF types

::

    record Depth1 = { foo: Optional Int64 }
    record Depth2 = { foo: Optional (Optional Int64) }

We have

::

    { }              -->  Depth1 { foo: None }            :  Depth1
    { }              -->  Depth2 { foo: None }            :  Depth2
    { foo: 42 }      -->  Depth1 { foo: Some 42 }         :  Depth1
    { foo: [42] }    -->  Depth2 { foo: Some (Some 42) }  :  Depth2
    { foo: null }    -->  Depth1 { foo: None }            :  Depth1
    { foo: null }    -->  Depth2 { foo: None }            :  Depth2
    { foo: [] }      -->  Depth2 { foo: Some None }       :  Depth2

Note that the shortcut for records and Optional fields does not apply to
Map (which are also represented as objects), since Map relies on absence
of key to determine what keys are present in the Map to begin with.  Nor
does it apply to the ``[f₁, ..., fₙ]`` record form; ``Depth1 None`` in
the array notation must be written as ``[null]``.

Type variables may appear in the Daml-LF language, but are always
resolved before deciding on a JSON encoding.  So, for example, even
though ``Oa`` doesn't appear to contain a nested ``Optional``, it may
contain a nested ``Optional`` by virtue of substituting the type
variable ``a``::

    record Oa a = { foo: Optional a }

    { foo: 42 }     -->  Oa { foo: Some 42 }        : Oa Int
    { }             -->  Oa { foo: None }           : Oa Int
    { foo: [] }     -->  Oa { foo: Some None }      : Oa (Optional Int)
    { foo: [42] }   -->  Oa { foo: Some (Some 42) } : Oa (Optional Int)

In other words, the correct JSON encoding for any LF value is the one
you get when you have eliminated all type variables.

Output
======

Encoded as described above, never applying the shortcut for None record
fields; e.g. ``{ foo: None }`` will always encode as ``{ foo: null }``.

Variant
*******

Variants are expressed as

::

    { tag: constructor, value: argument }

For example, if we have

::

    variant Foo = Bar Int64 | Baz Unit | Quux (Optional Int64)

These are all valid JSON encodings for values of type Foo::

    {"tag": "Bar", "value": 42}
    {"tag": "Baz", "value": {}}
    {"tag": "Quux", "value": null}
    {"tag": "Quux", "value": 42}

Note that Daml data types with named fields are compiled by factoring
out the record. So for example if we have

::

    data Foo = Bar {f1: Int64, f2: Bool} | Baz

We'll get in Daml-LF

::

    record Foo.Bar = {f1: Int64, f2: Bool}
    variant Foo = Bar Foo.Bar | Baz Unit

and then, from JSON

::

    {"tag": "Bar", "value": {"f1": 42, "f2": true}}
    {"tag": "Baz", "value": {}}

This can be encoded and used in TypeScript, including exhaustiveness
checking; see `a type refinement example`_.

.. _a type refinement example: https://www.typescriptlang.org/play/#code/C4TwDgpgBAYg9nKBeAsAKCpqBvKwCGA5gFxQBEAQvgE5kA0UAbvgDYCuEpuAZgIykA7NgFsARhGoNuAJlKiELCPgFQAvmvSYAPjjxFSlfAC96TVhy5q1AbnTpubAQGNgASzgrgEAM7AAFIyk8HAAlDiaUN4A7q7ATgAWUAEAdASEYdgRmE743tCGtMRZWE4e3nCKySxwhCnM7BDJfAyMyfUcTdIhthhYmNQQwGzUAj19OXnkVCZFveNlFY3Vta3tEN3F-YPDo8UAJhDc+GwswLN92WXAUAD6ghCMEshMYxpoqkA

Enum
****

Enums are represented as strings. So if we have

::

    enum Foo = Bar | Baz

There are exactly two valid JSON values for Foo, "Bar" and "Baz".
