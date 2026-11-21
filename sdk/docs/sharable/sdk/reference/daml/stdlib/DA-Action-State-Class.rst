.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-action-state-class-12696:

DA.Action.State.Class
=====================

DA\.Action\.State\.Class

Typeclasses
-----------

.. _class-da-action-state-class-actionstate-80467:

**class** `ActionState <class-da-action-state-class-actionstate-80467_>`_ s m **where**

  Action ``m`` has a state variable of type ``s``\.

  Rules\:

  * ``get *> ma  =  ma``
  * ``ma <* get  =  ma``
  * ``put a >>= get   =  put a $> a``
  * ``put a *> put b  =  put b``
  * ``(,) <$> get <*> get  =  get <&> \a -> (a, a)``

  Informally, these rules mean it behaves like an ordinary assignable variable\:
  it doesn't magically change value by looking at it, if you put a value there
  that's always the value you'll get if you read it, assigning a value but
  never reading that value has no effect, and so on\.

  .. _function-da-action-state-class-get-54107:

  `get <function-da-action-state-class-get-54107_>`_
    \: m s

    Fetch the current value of the state variable\.

  .. _function-da-action-state-class-put-15832:

  `put <function-da-action-state-class-put-15832_>`_
    \: s \-\> m ()

    Set the value of the state variable\.

  .. _function-da-action-state-class-modify-80630:

  `modify <function-da-action-state-class-modify-80630_>`_
    \: (s \-\> s) \-\> m ()

    Modify the state variable with the given function\.

  **default** modify

    \: :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> (s \-\> s) \-\> m ()

  **instance** `ActionState <class-da-action-state-class-actionstate-80467_>`_ s (:ref:`State <type-da-action-state-type-state-76783>` s)
