.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Data types
==========

In :doc:`contracts`, you learnt about contract templates, which specify the types of contracts that can be created on the ledger, and what data those contracts hold in their arguments.

In :doc:`daml-scripts`, you learnt about the script view in Daml Studio, which displays the current ledger state. It shows one table per template, with one row per contract of that type and one column per field in the arguments.

This actually provides a useful way of thinking about templates: like tables in databases. Templates specify a data schema for the ledger:

- each template corresponds to a table
- each field in the ``with`` block of a template corresponds to a column in that table
- each contract of that type corresponds to a table row

In this section, you'll learn how to create rich data schemas for your ledger. Specifically you'll learn about:

- Daml's built-in and native data types
- Record types
- Derivation of standard properties
- Variants
- Data manipulation
- Contract manipulation

After this section, you should be able to use a Daml ledger as a simple database where individual parties can write, read, and delete complex data.

.. hint::

  Remember that you can load all the code for this section into a folder called ``intro3`` by running ``daml new intro3 --template daml-intro-data``

.. _native-types:

Native types
------------

You have already encountered a few native Daml types: ``Party`` in :doc:`contracts`, and ``Text`` and ``ContractId`` in :doc:`daml-scripts`. Here are those native types and more:

- ``Party``
  Stores the identity of an entity that is able to act on the ledger, in the sense that they can sign contracts and submit transactions. In general, ``Party`` is opaque.
- ``Text``
  Stores a unicode character string like ``"Alice"``.
- ``ContractId a``
  Stores a reference to a contract of type ``a``.
- ``Int``
  Stores signed 64-bit integers. For example, ``-123``.
- ``Decimal``
  Stores fixed-point number with 28 digits before and 10 digits after the decimal point. For example, ``0.0000000001`` or ``-9999999999999999999999999999.9999999999``.
- ``Bool``
  Stores ``True`` or ``False``.
- ``Date``
  Stores a date.
- ``Time``
  Stores absolute UTC time.
- ``RelTime``
  Stores a difference in time.

The below script instantiates each one of these types, manipulates it where appropriate, and tests the result:

.. literalinclude:: daml/daml-intro-data/daml/Native.daml
  :language: daml
  :start-after: -- NATIVE_TEST_BEGIN
  :end-before: -- NATIVE_TEST_END

Despite its simplicity, there are quite a few things to note in this script:

- The ``import`` statements at the top import two packages from the Daml standard library, which contain all the date and time related functions we use here as well as the functions used in Daml Scripts. More on packages, imports and the standard library later.
- Most of the variables are declared inside a ``let`` block.

  That's because the ``script do`` block expects script actions like ``submit`` or ``allocateParty``. An integer like ``123`` is not an action, it's a pure expression, something we can evaluate without any ledger. You can think of the ``let`` as turning variable declaration into an action.
- Most variables do not have annotations to say what type they are.

  That's because Daml is very good at *inferring* types. The compiler knows that ``123`` is an ``Int``, so if you declare ``my_int = 123``, it can infer that ``my_int`` is also an ``Int``. This means you don't have to write the type annotation ``my_int : Int = 123``.

  However, if the type is ambiguous so that the compiler can't infer
  it, you do have to add a type annotation. This is the case for
  ``0.001`` which could be any ``Numeric n``. Here we
  specify ``0.001 : Decimal`` which is a synonym for ``Numeric 10``. You can always choose to add type annotations to aid readability.
- The ``assert`` function is an action that takes a boolean value and succeeds with ``True`` and fails with ``False``.

  Try putting ``assert False`` somewhere in a script and see what happens to the script result.

With templates and these native types, it's already possible to write a schema akin to a table in a relational database. Below, ``Token`` is extended into a simple ``CashBalance``, administered by a party in the role of an accountant:

.. literalinclude:: daml/daml-intro-data/daml/Native.daml
  :language: daml
  :start-after: -- CASH_BALANCE_BEGIN
  :end-before: -- CASH_BALANCE_END

Assemble types
--------------

There's quite a lot of information on the ``CashBalance`` above and it would be nice to be able to give that data more structure. Fortunately, Daml's type system has a number of ways to assemble these native types into much more expressive structures.

Tuples
~~~~~~

A common task is to group values in a generic way. Take, for example, a key-value pair with a ``Text`` key and an ``Int`` value. In Daml, you could use a two-tuple of type ``(Text, Int)`` to do so. If you wanted to express a coordinate in three dimensions, you could group three ``Decimal`` values using a three-tuple ``(Decimal, Decimal, Decimal)``:

.. literalinclude:: daml/daml-intro-data/daml/Tuple.daml
  :language: daml
  :start-after: -- TUPLE_TEST_BEGIN
  :end-before: -- TUPLE_TEST_END

You can access the data in the tuples using:

- functions ``fst``, ``snd``, ``fst3``, ``snd3``, ``thd3``
- a dot-syntax with field names ``_1``, ``_2``, ``_3``, etc.

Daml supports tuples with up to 20 elements, but accessor functions like ``fst`` are only included for 2- and 3-tuples.

Lists
~~~~~

Lists in Daml take a single type parameter defining the type of thing in the list. So you can have a list of integers ``[Int]`` or a list of strings ``[Text]``, but not a list mixing integers and strings.

That's because Daml is statically and strongly typed. When you get an element out of a list, the compiler needs to know what type that element has.

The below script instantiates a few lists of integers and demonstrates the most important list functions.

.. literalinclude:: daml/daml-intro-data/daml/List.daml
  :language: daml
  :start-after: -- LIST_TEST_BEGIN
  :end-before: -- LIST_TEST_END

Note the type annotation on ``empty : [Int] = []``. It's necessary because ``[]`` is ambiguous. It could be a list of integers or of strings, but the compiler needs to know which it is.

Records
~~~~~~~

You can think of records as named tuples with named fields. Declare them using the ``data`` keyword: ``data T = C with``, where ``T`` is the type name and ``C`` is the data constructor. In practice, it's a good idea to always use the same name for type and data constructor:

.. literalinclude:: daml/daml-intro-data/daml/Record.daml
  :language: daml
  :start-after: -- RECORD_TEST_BEGIN
  :end-before: -- RECORD_TEST_END

You'll notice that the syntax to declare records is very similar to the syntax used to declare templates. That's no accident because a template is really just a special record. When you write ``template Token with``, one of the things that happens in the background is that this becomes a ``data Token = Token with``.

In the ``assert`` statements above, we always compared values of in-built types. If you wrote ``assert (my_record == my_record)`` in the script, you may be surprised to get an error message ``No instance for (Eq MyRecord) arising from a use of ‘==’``. Equality in Daml is always value equality and we haven't written a function to check value equality for ``MyRecord`` values. But don't worry, you don't have to implement this rather obvious function yourself. The compiler is smart enough to do it for you, if you use ``deriving (Eq)``:

.. literalinclude:: daml/daml-intro-data/daml/Record.daml
  :language: daml
  :start-after: -- EQ_TEST_BEGIN
  :end-before: -- EQ_TEST_END

``Eq`` is what is called a *typeclass*. You can think of a typeclass as being like an interface in other languages: it is the mechanism by which you can define a set of functions (for example, ``==`` and ``/=`` in the case of ``Eq``) to work on multiple types, with a specific implementation for each type they can apply to.

There are some other typeclasses that the compiler can derive automatically. Most prominently, ``Show`` to get access to the function ``show`` (equivalent to ``toString`` in many languages) and ``Ord``, which gives access to comparison operators ``<``, ``>``, ``<=``, ``>=``.

It's a good idea to always derive ``Eq`` and ``Show`` using ``deriving (Eq, Show)``. The record types created using ``template T with`` do this automatically, and the native types have appropriate typeclass instances. For example, ``Int`` derives ``Eq``, ``Show``, and ``Ord``. As another example, ``ContractId a`` derives ``Eq`` and ``Show``.

Records can give the data on ``CashBalance`` a bit more structure:

.. literalinclude:: daml/daml-intro-data/daml/Record.daml
  :language: daml
  :start-after: -- CASH_BALANCE_BEGIN
  :end-before: -- CASH_BALANCE_END

If you look at the resulting script view, you'll see that this still gives rise to one table. The records are expanded out into columns using dot notation.

Variants and pattern matching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Suppose now that you also wanted to keep track of cash in hand. Cash in hand doesn't have a bank, but you can't just leave ``bank`` empty. Daml doesn't have an equivalent to ``null``. Variants can express that cash can either be in hand or at a bank:

.. literalinclude:: daml/daml-intro-data/daml/Variants.daml
  :language: daml
  :start-after: -- CASH_BALANCE_BEGIN
  :end-before: -- CASH_BALANCE_END

The way to read the declaration of ``Location`` is "*A Location either has value* ``InHand`` *OR has a value* ``InAccount a`` *where* ``a`` *is of type Account*". This is quite an explicit way to say that there may or may not be an ``Account`` associated with a ``CashBalance`` and gives both cases suggestive names.

Another option is to use the built-in ``Optional`` type. The ``None`` value of type ``Optional a`` is the closest Daml has to a ``null`` value:

.. literalinclude:: daml/daml-intro-data/daml/Variants.daml
  :language: daml
  :start-after: -- OPTIONAL_BEGIN
  :end-before: -- OPTIONAL_END

Variant types where none of the data constructors take a parameter are called enums:

.. literalinclude:: daml/daml-intro-data/daml/Variants.daml
  :language: daml
  :start-after: -- ENUM_BEGIN
  :end-before: -- ENUM_END

To access the data in variants, you need to distinguish the different possible cases. For example, you can no longer access the account number of a ``Location`` directly, because if it is ``InHand``, there may be no account number.

To do this, you can use *pattern matching* and either throw errors or return compatible types for all cases:

.. literalinclude:: daml/daml-intro-data/daml/Variants.daml
  :language: daml
  :start-after: -- VARIANT_ACCESS_BEGIN
  :end-before: -- VARIANT_ACCESS_END

Manipulate data
---------------

You've got all the ingredients to build rich types expressing the data you want to be able to write to the ledger, and you have seen how to create new values and read fields from values. But how do you manipulate values once created?

All data in Daml is immutable, meaning once a value is created, it will never change. Rather than changing values, you create new values based on old ones with some changes applied:

.. literalinclude:: daml/daml-intro-data/daml/Record.daml
  :language: daml
  :start-after: -- MANIPULATION_BEGIN
  :end-before: -- MANIPULATION_END

``changed_record`` and ``better_changed_record`` are each a copy of ``eq_record`` with the field ``my_int`` changed. ``better_changed_record`` shows the recommended way to change fields on a record. The syntax is almost the same as for a new record, but the record name is replaced with the old value: ``eq_record with`` instead of ``EqRecord with``. The ``with`` block no longer needs to give values to all fields of ``EqRecord``. Any missing fields are taken from ``eq_record``.

Throughout the script, ``eq_record`` never changes. The expression ``"Zero" :: eq_record.my_list`` doesn't change the list in-place, but creates a new list, which is ``eq_record.my_list`` with an extra element in the beginning.

.. _manipulate-contracts:

Manipulate contracts
--------------------

Daml's type system allows you to store structured data within Daml contracts.
Like records and other data structures, contracts are immutable.
They can only be created and archived.
If you need to change a contract, you must archive the original contract and create a new one with the updated data.

.. literalinclude:: daml/daml-intro-data/daml/ContractManipulation.daml
  :language: daml
  :start-after: -- CONTRACT_MANIPULATION_BEGIN
  :end-before: -- CONTRACT_MANIPULATION_END

The script above uses ``archiveCmd`` and ``createCmd`` to modify a contract of type ``Account`` in the same transaction.
This process creates a new contract, and a new, distinct contract ID, as shown by ``newAccountCid =/= accountCid``

When you modify a contract, by archiving it and creating a new one, you generate a new contract ID.
That makes contract IDs unstable, and it can cause stale references.

.. literalinclude:: daml/daml-intro-data/daml/ContractManipulation.daml
  :language: daml
  :start-after: -- STALE_CONTRACT_ID_BEGIN
  :end-before: -- STALE_CONTRACT_ID_END

In the script above, the ``ContractId`` in ``balance.account`` still refers to the archived contract.
Consequently, querying the ``balance.account`` fails and returns ``None``.

Next up
-------

You can now define data schemas for the ledger, read, write, and delete data from the ledger.

In :doc:`choices` you'll learn how to define data transformations and give other parties the right to manipulate data in restricted ways.
