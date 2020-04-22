.. _module-constrainttuples-44760:

Module ConstraintTuples
-----------------------

Data Types
^^^^^^^^^^

.. _type-constrainttuples-eq2-31733:

**type** `Eq2 <type-constrainttuples-eq2-31733_>`_ a b
  \= (`Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ a, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ b)

.. _type-constrainttuples-eq3-75180:

**type** `Eq3 <type-constrainttuples-eq3-75180_>`_ a b c
  \= (`Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ a, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ b, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ c)

.. _type-constrainttuples-eq4-2935:

**type** `Eq4 <type-constrainttuples-eq4-2935_>`_ a b c d
  \= (`Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ a, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ b, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ c, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ d)

Functions
^^^^^^^^^

.. _function-constrainttuples-eq2-12289:

`eq2 <function-constrainttuples-eq2-12289_>`_
  \: `Eq2 <type-constrainttuples-eq2-31733_>`_ a b \=\> a \-\> a \-\> b \-\> b \-\> `Bool <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654>`_

.. _function-constrainttuples-eq2tick-62955:

`eq2' <function-constrainttuples-eq2tick-62955_>`_
  \: (`Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ a, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ b) \=\> a \-\> a \-\> b \-\> b \-\> `Bool <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654>`_

.. _function-constrainttuples-eq3-55736:

`eq3 <function-constrainttuples-eq3-55736_>`_
  \: `Eq3 <type-constrainttuples-eq3-75180_>`_ a b c \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> `Bool <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654>`_

.. _function-constrainttuples-eq3tick-75648:

`eq3' <function-constrainttuples-eq3tick-75648_>`_
  \: (`Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ a, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ b, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ c) \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> `Bool <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654>`_

.. _function-constrainttuples-eq4-56779:

`eq4 <function-constrainttuples-eq4-56779_>`_
  \: `Eq4 <type-constrainttuples-eq4-2935_>`_ a b c d \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> d \-\> d \-\> `Bool <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654>`_

.. _function-constrainttuples-eq4tick-50089:

`eq4' <function-constrainttuples-eq4tick-50089_>`_
  \: (`Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ a, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ b, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ c, `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ d) \=\> a \-\> a \-\> b \-\> b \-\> c \-\> c \-\> d \-\> d \-\> `Bool <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654>`_
