.. _module-constrainttuples-79635:

ConstraintTuples
----------------

Data Types
^^^^^^^^^^

.. _type-constrainttuples-eq2-29566:

**type** `Eq2 <type-constrainttuples-eq2-29566_>`_ a b
  \= (`Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ a, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ b)

.. _type-constrainttuples-eq3-18799:

**type** `Eq3 <type-constrainttuples-eq3-18799_>`_ a b c
  \= (`Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ a, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ b, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ c)

.. _type-constrainttuples-eq4-25412:

**type** `Eq4 <type-constrainttuples-eq4-25412_>`_ a b c d
  \= (`Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ a, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ b, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ c, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ d)

Functions
^^^^^^^^^

.. _function-constrainttuples-eq2-16370:

`eq2 <function-constrainttuples-eq2-16370_>`_
  \: `Eq2 <type-constrainttuples-eq2-29566_>`_ a b \=\> a \-\> a \-\> b \-\> b \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

.. _function-constrainttuples-eq2tick-63686:

`eq2' <function-constrainttuples-eq2tick-63686_>`_
  \: (`Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ a, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ b) \=\> a \-\> a \-\> b \-\> b \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

.. _function-constrainttuples-eq3-5603:

`eq3 <function-constrainttuples-eq3-5603_>`_
  \: `Eq3 <type-constrainttuples-eq3-18799_>`_ a b c \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

.. _function-constrainttuples-eq3tick-36081:

`eq3' <function-constrainttuples-eq3tick-36081_>`_
  \: (`Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ a, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ b, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ c) \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

.. _function-constrainttuples-eq4-77456:

`eq4 <function-constrainttuples-eq4-77456_>`_
  \: `Eq4 <type-constrainttuples-eq4-25412_>`_ a b c d \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> d \-\> d \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

.. _function-constrainttuples-eq4tick-59016:

`eq4' <function-constrainttuples-eq4tick-59016_>`_
  \: (`Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ a, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ b, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ c, `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ d) \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> d \-\> d \-\> `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_
