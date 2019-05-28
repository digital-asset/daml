Data Types
==========

Contract templates specify the types of contracts that can be created on the ledger, and what data they hold in their arguments (see :doc:`1_Token`). The scenario view in the IDE displays the current ledger state by showing one table per template, with one row per contract of that type and one column per field in the arguments. This analogy is a useful one. Templates specify a data schema for the ledger, with each template corresponding to a table, each field in the ``with`` block of a template corresponding to a column in that table, and each contract instance of that type corresponding to a table row.

In this section, you'll learn how to create rich data schemas for your ledger. Specifically you'll learn about:

- DAML's built-in and *native* data types
- Record types
- Derivation of standard properties
- Variants
- Manipulating immutable data
- Contract keys

After this section, you should be able to use a DAML ledger as a simple database where individual parties can write, read and delete complex data.

Native Types
------------

You have already encountered the type ``Party`` in :doc:`1_Token` and ``Text`` and ``ContractId`` in :doc:`2_Scenario`.

- ``Party``
  Stores the identity of an entity that is able to act on the ledger.
- ``Text``
  Stores a unicode character string like ``"Alice"``.
- ``ContractId a``
  Stores a reference to a contract of type ``a``.
- ``Int``
  Stores unsigned 64-bit integers. For example, ``-123``.
- ``Decimal``
  Stores fixed-point number with precision 38 and scale 10. For example, ``0.0000000001`` or ``-9999999999999999999999999999.9999999999``.
- ``Bool``
  Stores ``True`` or ``False``.
- ``Date``
  Stores a date.
- ``Time``
  Stores absolute UTC time.
- ``RelTime``
  Stores a difference in time.

The below scenario instantiates each one of these types manipulates it where appropriate and tests the result.

.. literalinclude:: daml/3_Data/Native.daml
  :language: daml
  :start-after: -- NATIVE_TEST_BEGIN
  :end-before: -- NATIVE_TEST_END

Despite its simplicity, there are quite a few things to note in this scenario:

1. the ``import`` statements at the top import two packages from the DAML Standard Library, which contain all the date and time related functions we use here. More on packages, imports and the standard library later.
2. Most of the variables are declared inside a ``let`` block. That's because the ``scenario do`` block expects scenario actions like ``submit`` or ``getParty``. An integer like ``123`` is not an action, it's a pure expression we can evaluate without any ledger. The ``let`` can be thought of as turning variable declaration into an action.
3. None of the variables have type annotations. That's because DAML is very good at *inferring* types. The compiler knows that ``123`` is an ``Int`` so if you declare ``my_int = 123``, it can infer that ``my_int`` is also an ``Int`` so you don't have to write ``my_int : Int = 123``. You can always add type annotations to aid readability, and sometimes, in case of ambiguity, have to.
4. The ``assert`` function is an action that takes a boolean value and succeeds with ``True`` and fails with ``False``. Try putting ``assert False`` somewhere in a scenario and see what happens to the scenario result.

With tempates and these native types, it's already possible to write a schema akin to table in relational database. Below, ``Token`` is extended into a simple ``CashBalance``, administered by a party in the role of an accountant.

.. literalinclude:: daml/3_Data/Native.daml
  :language: daml
  :start-after: -- CASH_BALANCE_BEGIN
  :end-before: -- CASH_BALANCE_END

Assembling types
----------------

There's quite a lot of information on the ``CashBalance`` above and it would be nice to be able to give that data more structure. Fortunately, DAML's type system has a number of ways to assemble these native types into much more expressive structures.

Tuples
~~~~~~

A common task is to group values in a generic way. Take, for example a key-value pair with a ``Text`` key and an ``Int`` value. In DAML, you could use a two-tuple of type ``(Text, Int)`` to do so. If you wanted to express a coordinate in three dimensions, you could group three ``Decimal`` values using a three-tuple ``(Decimal, Decimal, Decimal)``.


.. literalinclude:: daml/3_Data/Tuple.daml
  :language: daml
  :start-after: -- TUPLE_TEST_BEGIN
  :end-before: -- TUPLE_TEST_END

Note how the data can be accessed either through functions ``fst``, ``snd``, ``fst3``, ``snd3``, ``thd3``, or via a dot-syntax with field names ``_1``, ``_2``, ``_3``, etc.

Lists
~~~~~

Lists in DAML take a single type parameter defining the type of thing in the list. So you can have a list of integers ``List Int`` or a list of strings ``List Text``, but not a list mixing integers and strings. That's because DAML is statically and strongly typed. When you get an element out of a list, the compiler needs to know what type that element has. The below scenario instantiates a few lists of integers and demonstrates the most important list functions.

.. literalinclude:: daml/3_Data/List.daml
  :language: daml
  :start-after: -- LIST_TEST_BEGIN
  :end-before: -- LIST_TEST_END

Note the type annotation on ``empty : List Int = []``. It's necessary because ``[]`` is ambiguous. It could be a list of integers or of strings, but the compiler needs to know which it is.

Records
~~~~~~~

Records can be thought of as named tuples with named fields. They are declared using the ``data`` keyword: ``data T = C with``, where ``T`` is the type name and ``C`` is the type constructor. In practice, it's a good idea to always use the same name for type and type constructor.

.. literalinclude:: daml/3_Data/Record.daml
  :language: daml
  :start-after: -- RECORD_TEST_BEGIN
  :end-before: -- RECORD_TEST_END

You'll notice that the syntax to declare records is very similar to the syntax used to declare templates. That's no accident because a template is really just a special record. When you write ``template Token with``, one of the things that happens in the background is that this becomes a ``data Token = Token with``.

In the ``assert`` statements above, we always compared values of in-built types. If you wrote ``assert (my_record == my_record)`` in the scenario, you may be surprised to get an error message ``No instance for (Eq MyRecord) arising from a use of ‘==’``. Equality in DAML is always value equality and we haven't written a function to check value equality for ``MyRecord`` values. But don't worry, you don't have to implement this rather obvious function yourself. The compiler is smart enough to do it for you.

.. literalinclude:: daml/3_Data/Record.daml
  :language: daml
  :start-after: -- EQ_TEST_BEGIN
  :end-before: -- EQ_TEST_END

``Eq`` is a so-called type-class, which can be thought of like an interface in other languages. There are some other type-classes that can be derived automatically. Most prominently ``Show`` to get access to the function ``show``, equivalent to ``toString`` in many languages, and ``Ord``, which gives access to comparison operators ``<``, ``>``, ``<=``, ``>=``.

It's a good idea to always derive ``Eq`` and ``Show`` using ``deriving (Eq, Show)``. The record types created using ``template T with`` do this automatically.

Records can give the data on ``CashBalance`` a bit more structure:

.. literalinclude:: daml/3_Data/Record.daml
  :language: daml
  :start-after: -- CASH_BALANCE_BEGIN
  :end-before: -- CASH_BALANCE_END

If you look at the resulting scenario view, you'll see that this still gives rise to one table. The records are expanded out into columns using dot notation.

Variants
~~~~~~~~

Suppose now that you also wanted to keep track of cash in hand. Cash in hand doesn't have a bank, but you can't just leave ``bank`` empty. DAML doesn't have an equivalent to ``null``. Variants can express that cash can either be in hand or at a bank.

.. literalinclude:: daml/3_Data/Variants.daml
  :language: daml
  :start-after: -- CASH_BALANCE_BEGIN
  :end-before: -- CASH_BALANCE_END

The way to read the declaration of ``Location`` is "*A Location either has value* ``InHand`` *OR has a value* ``InAccount a`` *where* ``a`` *is of type Account*". This is quite an explicit way to say that there may or may not be an ``Account`` associated with a ``CashBalance`` and gives both cases suggestive names. The generic way would be to use the built-in ``Optional`` type. The ``None`` value of type ``Optional a`` is the closest DAML has to a ``null`` value.

.. literalinclude:: daml/3_Data/Variants.daml
  :language: daml
  :start-after: -- OPTIONAL_BEGIN
  :end-before: -- OPTIONAL_END

Variant types where none of the type constructors take a parameter are called enums:

.. literalinclude:: daml/3_Data/Variants.daml
  :language: daml
  :start-after: -- ENUM_BEGIN
  :end-before: -- ENUM_END

Accessing data in variants requires distinguishing the different possible cases. It is no longer possible to access the account number of a ``Location`` directly, for example, as there may be no account number in case of ``InHand``. Instead, use *pattern matching* and either throw errors or return compatible types for all cases.

.. literalinclude:: daml/3_Data/Variants.daml
  :language: daml
  :start-after: -- VARIANT_ACCESS_BEGIN
  :end-before: -- VARIANT_ACCESS_END

Manipulating Data
-----------------

You've got all the ingredients to build rich types expressing the data you want to be able to write to the ledger, and you have seen how to create new values and read fields from values. But how do you manipulate values once created?

All data in DAML is immutable, meaning once a value is created, it will never change. Rather than changing values, one creates new values based on old ones with some changes applied. Whilst contracts and records may feel a bit like objects in object oriented langauges, they behave quite different in this respect.

.. literalinclude:: daml/3_Data/Record.daml
  :language: daml
  :start-after: -- MANIPULATION_BEGIN
  :end-before: -- MANIPULATION_END

``changed_record`` and ``better_changed_record`` are each a copy of ``eq_record`` with the field ``my_int`` changed. ``better_changed_record`` shows the recommended way to change fields on a record. The syntax is almost the same as for a new record, but the record name is replaced with the old value: ``eq_record with`` instead of ``EqRecord with``. The ``with`` block no longer needs to give values to all fields of ``EqRecord``. Any missing fields are taken from ``eq_record``.

Throughout the scenario, ``eq_record`` never changes. The expression ``"Zero" :: eq_record.my_list`` doesn't change the list in-place, but creates a new list, which is ``eq_record.my_list`` with an extra element in the beginning.

Contract keys
-------------

DAML's type system let's you store richly structured data on DAML templates, but just like most database schemas have more than one table, DAML contract models often have multiple templates that reference each other. For example, you may not want to store your bank and account information on each individual cash balance, but instead store those separately.

You have already met the type ``ContractId a``, which references a contract of type ``a``. The below shows a contract model where ``Account`` is split out into a separate template and referenced by ``ContractId`, but it also highlights a big problem with that kind of reference: Just like data, contracts are immutable. They can only be created and archived. That makes contract ids very unstable, and can cause stale references.

.. literalinclude:: daml/3_Data/IDRef.daml
  :language: daml
  :start-after: -- ID_REF_TEST_BEGIN
  :end-before: -- ID_REF_TEST_END

Note the use of the ``fetch`` function in the above, which retrieves the arguments of an active contract from its contract id. Also take note that for the first time, the party submitting a transaction is doing more than one thing as part of that transaction. To create ``new_account``, the accountant fetches a the arguments of the old account, archives the old account and creates a new account, all in one transaction. More on building transactions in :doc:`7_Composing`.

DAML allows the definition of stable keys using the ``key`` and ``maintainer`` keywords. ``key`` defines the primary key of a template, with a uniqueness constraint on that key and the ability to look up contracts by key.

.. literalinclude:: daml/3_Data/Keys.daml
  :language: daml
  :start-after: -- KEY_TEST_BEGIN
  :end-before: -- KEY_TEST_END

Since DAML is a distributed system, there is no global entity that can guarantee uniqueness, which is why each ``key`` expression must come with a ``maintainer`` expression. ``maintainer`` takes one or several parties, all of which have to be signatories of the contract and be part of the key. That way the index can be partitioned amongst sets of maintainers, and each set of maintainers can independently ensure the uniqueness constraint on their piece of the index.

Note how the ``fetch`` in the final ``submit`` block has become a ``fetchByKey @Account``. ``fetchByKey @Account`` takes a value of type `AccountKey` and returns a tuple ``(ContractId Account, Account)`` if the lookup was successful or fails the transaction otherwise. Since a single type could be used as the key for multiple templates, the compiler needs to be told what type of contract is being fetched using the ``@Account`` notation.


Next Up
-------

You are now able to define data schemas for the ledger, read, write and delete data from the ledger and use keys to reference and look up data in a stable fashion. In :doc:`4_Transformations` you'll learn how to define data transformations and give other parties the right to manipulate data in restricted ways.
