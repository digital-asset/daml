.. _module-deriving-95364:

Module Deriving
---------------

Data Types
^^^^^^^^^^

.. _type-deriving-formula-84903:

**data** `Formula <type-deriving-formula-84903_>`_ t

  .. _constr-deriving-tautology-41024:
  
  `Tautology <constr-deriving-tautology-41024_>`_
  
  
  .. _constr-deriving-contradiction-93645:
  
  `Contradiction <constr-deriving-contradiction-93645_>`_
  
  
  .. _constr-deriving-proposition-99264:
  
  `Proposition <constr-deriving-proposition-99264_>`_ t
  
  
  .. _constr-deriving-negation-52326:
  
  `Negation <constr-deriving-negation-52326_>`_ (`Formula <type-deriving-formula-84903_>`_ t)
  
  
  .. _constr-deriving-conjunction-36676:
  
  `Conjunction <constr-deriving-conjunction-36676_>`_ \[`Formula <type-deriving-formula-84903_>`_ t\]
  
  
  .. _constr-deriving-disjunction-94592:
  
  `Disjunction <constr-deriving-disjunction-94592_>`_ \[`Formula <type-deriving-formula-84903_>`_ t\]
  
  
  **instance** `Functor <https://docs.daml.com/daml/stdlib/index.html#class-ghc-base-functor-73448>`_ `Formula <type-deriving-formula-84903_>`_
  
  **instance** `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ t \=\> `Eq <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216>`_ (`Formula <type-deriving-formula-84903_>`_ t)
  
  **instance** `Ord <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-ord-70960>`_ t \=\> `Ord <https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-ord-70960>`_ (`Formula <type-deriving-formula-84903_>`_ t)
  
  **instance** `Show <https://docs.daml.com/daml/stdlib/index.html#class-ghc-show-show-56447>`_ t \=\> `Show <https://docs.daml.com/daml/stdlib/index.html#class-ghc-show-show-56447>`_ (`Formula <type-deriving-formula-84903_>`_ t)
