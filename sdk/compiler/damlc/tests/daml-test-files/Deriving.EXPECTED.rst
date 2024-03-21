.. _module-deriving-95739:

Module Deriving
---------------

Data Types
^^^^^^^^^^

.. _type-deriving-formula-60264:

**data** `Formula <type-deriving-formula-60264_>`_ t

  .. _constr-deriving-tautology-1247:
  
  `Tautology <constr-deriving-tautology-1247_>`_
  
  
  .. _constr-deriving-contradiction-64078:
  
  `Contradiction <constr-deriving-contradiction-64078_>`_
  
  
  .. _constr-deriving-proposition-76435:
  
  `Proposition <constr-deriving-proposition-76435_>`_ t
  
  
  .. _constr-deriving-negation-39767:
  
  `Negation <constr-deriving-negation-39767_>`_ (`Formula <type-deriving-formula-60264_>`_ t)
  
  
  .. _constr-deriving-conjunction-55851:
  
  `Conjunction <constr-deriving-conjunction-55851_>`_ \[`Formula <type-deriving-formula-60264_>`_ t\]
  
  
  .. _constr-deriving-disjunction-19371:
  
  `Disjunction <constr-deriving-disjunction-19371_>`_ \[`Formula <type-deriving-formula-60264_>`_ t\]
  
  
  **instance** `Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ `Formula <type-deriving-formula-60264_>`_
  
  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ t \=\> `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ (`Formula <type-deriving-formula-60264_>`_ t)
  
  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ t \=\> `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ (`Formula <type-deriving-formula-60264_>`_ t)
  
  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ t \=\> `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ (`Formula <type-deriving-formula-60264_>`_ t)
