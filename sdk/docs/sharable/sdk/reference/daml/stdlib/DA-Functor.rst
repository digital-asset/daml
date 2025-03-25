.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-functor-63823:

DA.Functor
==========

The ``Functor`` class is used for types that can be mapped over\.

Functions
---------

.. _function-da-functor-dollargt-48161:

`($>) <function-da-functor-dollargt-48161_>`_
  \: :ref:`Functor <class-ghc-base-functor-31205>` f \=\> f a \-\> b \-\> f b

  Replace all locations in the input (on the left) with the given
  value (on the right)\.

.. _function-da-functor-ltampgt-91298:

`(<&>) <function-da-functor-ltampgt-91298_>`_
  \: :ref:`Functor <class-ghc-base-functor-31205>` f \=\> f a \-\> (a \-\> b) \-\> f b

  Map a function over a functor\. Given a value ``as`` and a function
  ``f``, ``as <&> f`` is ``f <$> as``\. That is, ``<&>`` is like ``<$>`` but the
  arguments are in reverse order\.

.. _function-da-functor-ltdollardollargt-89503:

`(<$$>) <function-da-functor-ltdollardollargt-89503_>`_
  \: (:ref:`Functor <class-ghc-base-functor-31205>` f, :ref:`Functor <class-ghc-base-functor-31205>` g) \=\> (a \-\> b) \-\> g (f a) \-\> g (f b)

  Nested ``<$>``\.

.. _function-da-functor-void-91123:

`void <function-da-functor-void-91123_>`_
  \: :ref:`Functor <class-ghc-base-functor-31205>` f \=\> f a \-\> f ()

  Replace all the locations in the input with ``()``\.
