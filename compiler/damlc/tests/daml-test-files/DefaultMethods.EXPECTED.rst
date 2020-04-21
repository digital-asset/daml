.. _module-defaultmethods-97307:

Module DefaultMethods
---------------------

Typeclasses
^^^^^^^^^^^

.. _class-defaultmethods-d-4635:

**class** `D <class-defaultmethods-d-4635_>`_ a **where**

  .. _function-defaultmethods-x-92038:
  
  `x <function-defaultmethods-x-92038_>`_
    \: a
  
  .. _function-defaultmethods-y-38115:
  
  `y <function-defaultmethods-y-38115_>`_
    \: a

.. _class-defaultmethods-foldablex-48748:

**class** `FoldableX <class-defaultmethods-foldablex-48748_>`_ t **where**

  .. _function-defaultmethods-foldrx-33654:
  
  `foldrX <function-defaultmethods-foldrx-33654_>`_
    \: (a \-\> b \-\> b) \-\> b \-\> t a \-\> b

.. _class-defaultmethods-traversablex-59027:

**class** (`Functor <https://docs.daml.com/daml/stdlib/index.html#class-ghc-base-functor-73448>`_ t, `FoldableX <class-defaultmethods-foldablex-48748_>`_ t) \=\> `TraversableX <class-defaultmethods-traversablex-59027_>`_ t **where**

  .. _function-defaultmethods-traversex-21140:
  
  `traverseX <function-defaultmethods-traversex-21140_>`_
    \: Applicative m \=\> (a \-\> m b) \-\> t a \-\> m (t b)
  
  .. _function-defaultmethods-sequencex-86855:
  
  `sequenceX <function-defaultmethods-sequencex-86855_>`_
    \: Applicative m \=\> t (m a) \-\> m (t a)

.. _class-defaultmethods-id-77721:

**class** `Id <class-defaultmethods-id-77721_>`_ a **where**

  .. _function-defaultmethods-id-57162:
  
  `id <function-defaultmethods-id-57162_>`_
    \: a \-\> a
  
  **instance** `Id <class-defaultmethods-id-77721_>`_ `Int <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-int-68728>`_

.. _class-defaultmethods-myshow-63359:

**class** `MyShow <class-defaultmethods-myshow-63359_>`_ t **where**

  Default implementation with a separate type signature for the default method\.
  
  .. _function-defaultmethods-myshow-41356:
  
  `myShow <function-defaultmethods-myshow-41356_>`_
    \: t \-\> `Text <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-text-57703>`_
    
    Doc for method\.
  
  **default** myShow
  
    \: `Show <https://docs.daml.com/daml/stdlib/index.html#class-ghc-show-show-56447>`_ t \=\> t \-\> `Text <https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-text-57703>`_
    
    Doc for default\.
