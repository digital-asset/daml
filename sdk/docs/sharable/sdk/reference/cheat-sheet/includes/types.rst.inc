Types
-----

.. list-table::
   :widths: 30 70

   * - Type annotation
     - ``myVar : TypeName``
   * - Builtin types
     - ``Int``, ``Decimal``, ``Numeric n``, ``Text``, ``Bool``, ``Party``, ``Date``, ``Time``, ``RelTime``
   * - Type synonym
     - ``type MyInt = Int``
   * - Lists
     - ``type ListOfInts = [Int]``
   * - Tuples
     - ``type MyTuple = (Int, Text)``
   * - Polymorphic types
     - ``type MyType a b = [(a, b)]``
