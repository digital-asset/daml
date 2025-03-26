.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-bifunctor-44306:

DA.Bifunctor
============

Typeclasses
-----------

.. _class-da-bifunctor-bifunctor-68312:

**class** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ p **where**

  A bifunctor is a type constructor that takes
  two type arguments and is a functor in *both* arguments\. That
  is, unlike with ``Functor``, a type constructor such as ``Either``
  does not need to be partially applied for a ``Bifunctor``
  instance, and the methods in this class permit mapping
  functions over the Left value or the ``Right`` value,
  or both at the same time\.

  It is a bifunctor where both the first and second
  arguments are covariant\.

  You can define a ``Bifunctor`` by either defining bimap or by
  defining both first and second\.

  If you supply bimap, you should ensure that\:

  .. code-block:: daml
    :force:

    `bimap identity identity` ≡ `identity`


  If you supply first and second, ensure\:

  .. code-block:: daml
    :force:

    first identity ≡ identity
    second identity ≡ identity



  If you supply both, you should also ensure\:

  .. code-block:: daml
    :force:

    bimap f g ≡ first f . second g


  By parametricity, these will ensure that\:

  .. code-block:: daml
    :force:

    
    bimap  (f . g) (h . i) ≡ bimap f h . bimap g i
    first  (f . g) ≡ first  f . first  g
    second (f . g) ≡ second f . second g


  .. _function-da-bifunctor-bimap-49864:

  `bimap <function-da-bifunctor-bimap-49864_>`_
    \: (a \-\> b) \-\> (c \-\> d) \-\> p a c \-\> p b d

    Map over both arguments at the same time\.

    .. code-block:: daml
      :force:

      bimap f g ≡ first f . second g


    Examples\:

    .. code-block:: daml

      >>> bimap not (+1) (True, 3)
      (False,4)

      >>> bimap not (+1) (Left True)
      Left False

      >>> bimap not (+1) (Right 3)
      Right 4

  .. _function-da-bifunctor-first-18253:

  `first <function-da-bifunctor-first-18253_>`_
    \: (a \-\> b) \-\> p a c \-\> p b c

    Map covariantly over the first argument\.

    .. code-block:: daml
      :force:

      first f ≡ bimap f identity


    Examples\:

    .. code-block:: daml

      >>> first not (True, 3)
      (False,3)

      >>> first not (Left True : Either Bool Int)
      Left False

  .. _function-da-bifunctor-second-99394:

  `second <function-da-bifunctor-second-99394_>`_
    \: (b \-\> c) \-\> p a b \-\> p a c

    Map covariantly over the second argument\.

    .. code-block:: daml
      :force:

      second ≡ bimap identity


    Examples\:

    .. code-block:: daml

      >>> second (+1) (True, 3)
      (True,4)

      >>> second (+1) (Right 3 : Either Bool Int)
      Right 4

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ :ref:`Either <type-da-types-either-56020>`

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ ()

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ x1

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ (x1, x2)

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ (x1, x2, x3)

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ (x1, x2, x3, x4)

  **instance** `Bifunctor <class-da-bifunctor-bifunctor-68312_>`_ (x1, x2, x3, x4, x5)
