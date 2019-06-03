.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Restricting the Contract Model
==============================

Oftentimes, contract models will have constraints on the data stored or the allowed data transformations. In this section, you will learn about the two main mechanisms provided in DAML:

1. The ``ensure`` keyword.
2. The ``assert``, ``abort`` and ``error`` keywords.

To make sense of the latter, you'll also learn more about the ``Update`` type and ``do`` blocks, which will be good preparation for :doc:`7_Composing`, where you will use ``do`` blocks to compose choices into complex transactions.

Lastly, you will learn about time on the ledger and in scenarios.

Template Pre-Conditions
-----------------------

The first kind of restriction one commonly puts on the contract model are called *template pre-conditions*. These are simply restrictions on the data that can be stored on a contract from that template. Suppose, for examples, that the ``SimpleIou`` contract from :ref:`simple_iou` should only be able to store positive amounts. This can be enforced using the ``ensure`` keyword.

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTED_IOU_BEGIN
  :end-before: -- RESTRICTED_IOU_END

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- RESTRICTIONS_TEST_BEGIN
  :end-before: -- RESTRICTIONS_TEST_END

The ``ensure`` keyword takes a single expression of type ``Bool``. If you wanted to add more restrictions, you can use logical operators ``&&``, ``||`` and ``not`` to build up expressions. The above shows the additional restriction that currencies are three capital letters.

Assertions and Errors
---------------------

A second common kind of restriction is one on data transformations. For example, the simple Iou in :ref:`simple_iou` allowed the no-op where the ``owner`` transfers to themselves. This can be revented using an ``assert`` statement, which you have already encountered in the context of scenarios. ``assert`` does not return an informative error so often it's better to use the function ``assertMsg``, which takes a custom error message:

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_CHOICE_BEGIN
  :end-before: -- TRANSFER_CHOICE_END

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- TRANSFER_TEST_BEGIN
  :end-before: -- TRANSFER_TEST_END

In a similar vein, one can write a ``Redeem`` choice, which allows the ``owner`` to redeem an ``Iou`` during business hours on week days. The choice doesn't do anything other than archiving the ``SimpleIou``. The assumption here is that actual cash changes hands off-ledger.

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_CHOICE_BEGIN
  :end-before: -- REDEEM_CHOICE_END

.. literalinclude:: daml/Intro_5_Restrictions.daml
  :language: daml
  :start-after: -- REDEEM_TEST_BEGIN
  :end-before: -- REDEEM_TEST_END

There are two
