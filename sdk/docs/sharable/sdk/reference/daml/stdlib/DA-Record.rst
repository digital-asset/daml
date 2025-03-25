.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-record-78710:

DA.Record
=========

Exports the record machinery necessary to allow one to annotate
code that is polymorphic in the underlying record type\.

Typeclasses
-----------

.. _class-da-internal-record-getfield-53979:

**class** `GetField <class-da-internal-record-getfield-53979_>`_ x r a **where**

  ``GetField x r a`` provides the getter part of ``HasField``

  .. _function-da-internal-record-getfield-6942:

  `getField <function-da-internal-record-getfield-6942_>`_
    \: r \-\> a

.. _class-da-internal-record-setfield-4311:

**class** `SetField <class-da-internal-record-setfield-4311_>`_ x r a **where**

  ``SetField x r a`` provides the setter part of ``HasField``

  .. _function-da-internal-record-setfield-14978:

  `setField <function-da-internal-record-setfield-14978_>`_
    \: a \-\> r \-\> r

Data Types
----------

.. _type-da-internal-record-hasfield-59910:

**type** `HasField <type-da-internal-record-hasfield-59910_>`_ x r a
  \= (`GetField <class-da-internal-record-getfield-53979_>`_ x r a, `SetField <class-da-internal-record-setfield-4311_>`_ x r a)

  ``HasField`` is a class synonym for ``GetField`` and ``SetField``, which
  respectively give you getter and setter functions for each record field
  automatically\.

  **In the vast majority of use\-cases, plain Record syntax should be
  preferred**\:

  .. code-block:: daml

    daml> let a = MyRecord 1 "hello"
    daml> a.foo
    1
    daml> a.bar
    "hello"
    daml> a { bar = "bye" }
    MyRecord {foo = 1, bar = "bye"}
    daml> a with foo = 3
    MyRecord {foo = 3, bar = "hello"}
    daml>


  For more on Record syntax, see https\://docs\.daml\.com/daml/intro/3\_Data\.html\#record\.

  ``GetField x r a`` and ``SetField x r a`` are typeclasses taking three parameters\. The first
  parameter ``x`` is the field name, the second parameter ``r`` is the record type,
  and the last parameter ``a`` is the type of the field in this record\. For
  example, if we define a type\:

  .. code-block:: daml

    data MyRecord = MyRecord with
        foo : Int
        bar : Text


  Then we get, for free, the following GetField and SetField instances\:

  .. code-block:: daml

    GetField "foo" MyRecord Int
    SetField "foo" MyRecord Int
    GetField "bar" MyRecord Text
    SetField "bar" MyRecord Text


  If we want to get a value, we can use the ``getField`` method of class ``GetField``\:

  .. code-block:: daml

    getFoo : MyRecord -> Int
    getFoo r = getField @"foo" r

    getBar : MyRecord -> Text
    getBar r = getField @"bar" r


  Note that this uses the “type application” syntax ( ``f @t`` ) to specify the
  field name\.

  Likewise, if we want to set the value in the field, we can use the ``setField`` method of class ``SetField``\:

  .. code-block:: daml

    setFoo : Int -> MyRecord -> MyRecord
    setFoo a r = setField @"foo" a r

    setBar : Text -> MyRecord -> MyRecord
    setBar a r = setField @"bar" a r
