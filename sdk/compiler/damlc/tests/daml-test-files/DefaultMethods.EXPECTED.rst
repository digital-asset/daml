.. _module-defaultmethods-34992:

Module DefaultMethods
---------------------

Typeclasses
^^^^^^^^^^^

.. _class-defaultmethods-d-39130:

**class** `D <class-defaultmethods-d-39130_>`_ a **where**

  .. _function-defaultmethods-x-53637:
  
  `x <function-defaultmethods-x-53637_>`_
    \: a
  
  .. _function-defaultmethods-y-51560:
  
  `y <function-defaultmethods-y-51560_>`_
    \: a

.. _class-defaultmethods-foldablex-43965:

**class** `FoldableX <class-defaultmethods-foldablex-43965_>`_ t **where**

  .. _function-defaultmethods-foldrx-50503:
  
  `foldrX <function-defaultmethods-foldrx-50503_>`_
    \: (a \-\> b \-\> b) \-\> b \-\> t a \-\> b

.. _class-defaultmethods-traversablex-84604:

**class** (`Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ t, `FoldableX <class-defaultmethods-foldablex-43965_>`_ t) \=\> `TraversableX <class-defaultmethods-traversablex-84604_>`_ t **where**

  .. _function-defaultmethods-traversex-89947:
  
  `traverseX <function-defaultmethods-traversex-89947_>`_
    \: `Applicative <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ m \=\> (a \-\> m b) \-\> t a \-\> m (t b)
  
  .. _function-defaultmethods-sequencex-92456:
  
  `sequenceX <function-defaultmethods-sequencex-92456_>`_
    \: `Applicative <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ m \=\> t (m a) \-\> m (t a)

.. _class-defaultmethods-id-10050:

**class** `Id <class-defaultmethods-id-10050_>`_ a **where**

  .. _function-defaultmethods-id-52623:
  
  `id <function-defaultmethods-id-52623_>`_
    \: a \-\> a
  
  **instance** `Id <class-defaultmethods-id-10050_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

.. _class-defaultmethods-myshow-63060:

**class** `MyShow <class-defaultmethods-myshow-63060_>`_ t **where**

  Default implementation with a separate type signature for the default method\.
  
  .. _function-defaultmethods-myshow-32065:
  
  `myShow <function-defaultmethods-myshow-32065_>`_
    \: t \-\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
    
    Doc for method\.
  
  **default** myShow
  
    \: `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ t \=\> t \-\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
    
    Doc for default\.
