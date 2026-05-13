.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-action-state-50232:

DA.Action.State
===============

DA\.Action\.State

Data Types
----------

.. _type-da-action-state-type-state-76783:

**data** `State <type-da-action-state-type-state-76783_>`_ s a

  A value of type ``State s a`` represents a computation that has access to a state variable
  of type ``s`` and produces a value of type ``a``\.

  > > > runState (modify (+1)) 0
  > > > ((), 1)


  > > > evalState (modify (+1)) 0
  > > > ()


  > > > execState (modify (+1)) 0
  > > > 1


  > > > runState (do x \<- get; modify (+1); pure x) 0
  > > > (0, 1)


  > > > runState (put 1) 0
  > > > ((), 1)


  > > > runState (modify (+1)) 0
  > > > ((), 1)


  Note that values of type ``State s a`` are not serializable\.

  .. _constr-da-action-state-type-state-26:

  `State <constr-da-action-state-type-state-26_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - runState
         - s \-\> (a, s)
         -

  **instance** :ref:`ActionState <class-da-action-state-class-actionstate-80467>` s (`State <type-da-action-state-type-state-76783_>`_ s)

  **instance** :ref:`Action <class-da-internal-prelude-action-68790>` (`State <type-da-action-state-type-state-76783_>`_ s)

  **instance** :ref:`Applicative <class-da-internal-prelude-applicative-9257>` (`State <type-da-action-state-type-state-76783_>`_ s)

  **instance** :ref:`Functor <class-ghc-base-functor-31205>` (`State <type-da-action-state-type-state-76783_>`_ s)

Functions
---------

.. _function-da-action-state-evalstate-95640:

`evalState <function-da-action-state-evalstate-95640_>`_
  \: `State <type-da-action-state-type-state-76783_>`_ s a \-\> s \-\> a

  Special case of ``runState`` that does not return the final state\.

.. _function-da-action-state-execstate-48251:

`execState <function-da-action-state-execstate-48251_>`_
  \: `State <type-da-action-state-type-state-76783_>`_ s a \-\> s \-\> s

  Special case of ``runState`` that does only retun the final state\.
