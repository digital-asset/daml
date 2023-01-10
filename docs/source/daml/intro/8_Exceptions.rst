.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Exception Handling
==================

The default behavior in Daml is to abort the transaction on any error
and roll back all changes that have happened until then. However, this
is not always appropriate. In some cases, it makes sense to recover
from an error and continue the transaction instead of aborting it.

One option for doing that is to represent errors explicitly via
``Either`` or ``Option`` as shown in :doc:`3_Data`. This approach has
the advantage that it is very explicit about which operations are
allowed to fail without aborting the entire transaction. However, it
also has two major downsides. First, it can
be invasive for operations where aborting the transaction is often the
desired behavior, e.g., changing division to return ``Either`` or an
``Option`` to handle division by zero would be a very invasive change
and many callsites might not want to handle the error case explicitly.
Second, and more importantly, this approach does not allow rolling
back ledger actions that have happened before the point where failure is
detected; if a contract
got created before we hit the error, there is no way to undo that
except for aborting the entire transaction (which is what we were
trying to avoid in the first place).

By contrast, exceptions provide a way to handle certain types of
errors in such a way that, on the one hand, most of the code that is
allowed to fail can be written just like normal code, and, on the
other hand, the programmer can clearly delimit which part of the
current transaction should be rolled back on failure.
All of that still happens within the same
transaction and is thereby atomic contrary to handling the error
outside of Daml.

.. hint::

  Remember that you can load all the code for this section into a folder called ``intro8`` by running ``daml new intro8 --template daml-intro-8``

Our example for the use of exceptions will be a simple shop
template. Users can order items by calling a choice and transfer money
(in the form of an Iou issued by their bank) from their account to the
owner in return.

First, we need to setup a template to represent the account of a user:

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- ACCOUNT_BEGIN
  :end-before: -- ACCOUNT_END

Note that the template has an ``ensure`` clause that ensures that the
amount is always positive so ``Transfer`` cannot transfer more money
than is available.

The shop is represented as a template signed by the owner. It has a
field to represent the bank accepted by the owner, a list of
observers that can order items, and a fixed price for the items that can be
ordered:

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- SHOP_BEGIN
  :end-before: -- SHOP_END

.. note:: In a real setting the price of each item for sale might be
  defined in a separate contract.

The ordering process is then represented by a non-consuming choice on
this template which calls ``Transfer`` and creates an ``Order``
contract in return:

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- ORDER_BEGIN
  :end-before: -- ORDER_END

However, the shop owner has realized that often orders fail because
the account of their users is not topped up. They have a small trusted
userbase they know well so they decide that if the account is not
topped up, the shoppers can instead issue an Iou to the owner and pay
later. While it would be possible to check the conditions under which
``Transfer`` will fail in ``OrderItem`` this can be quite fragile: In
this example, the condition is relatively simple but in larger
projects replicating the conditions outside the choice and keeping the
two in sync can be challenging.

Exceptions allow us to handle this differently. Rather than
replicating the checks in ``Transfer``, we can instead catch the
exception thrown on failure. To do so we need to use a try-catch
block. The ``try`` block defines the scope within which we want to
catch exceptions while the ``catch`` clauses define which exceptions
we want to catch and how we want to handle them. In this case, we want
to catch the exception thrown by a failed ``ensure`` clause. This
exception is defined in ``daml-stdlib`` as
``PreconditionFailed``. Putting it together our order process for
trusted users looks as follows:

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- ORDER_TRUSTED_BEGIN
  :end-before: -- ORDER_TRUSTED_END

Let's walk through this code. First, as mentioned, the shop owner is
the trusting kind, so he wants to start by creating the ``Order``
no matter what. Next, he tries to charge the customer for the order. We
could, at this point, check their balance against the cost of the
order, but that would amount to duplicating the logic already present
in ``Account``. This logic is pretty simple in this case, but
duplicating invariants is a bad habit to get into. So, instead, we
just *try* to charge the account. If that succeeds, we just merrily
ignore the entire ``catch`` clause; if that fails, however, we do not
want to destroy the Order contract we had already created. Instead, we
want to *catch* the error thrown by the ``ensure`` clause of
``Account`` (in this case, it is of type ``PreconditionFailed``) and
try something else: create an ``Iou``  contract to register the debt
and move on.

Note that if the ``Iou`` creation still failed (unlikely with our
definition of ``Iou`` here, but could happen in more complex
scenarios), because that one is not wrapped in a ``try`` block, we
would revert to the default Daml behaviour and the ``Order`` creation
*would* be rolled back.

In addition to catching built-in exceptions like
``PreconditionFailed``, you can also define your own exception types
which can be caught and thrown. As an example, let’s consider a
variant of the ``Transfer`` choice that only allows for transfers up
to a given limit. If the amount is higher than the limit, we throw an
exception called ``TransferLimitExceeded``.

We first have to define the exception and define a way to represent it
as a string. In this case, our exception should store the amount that
someone tried to transfer as well as the limit.

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- CUSTOM_EXCEPTION_BEGIN
  :end-before: -- CUSTOM_EXCEPTION_END

To throw our own exception, you can use ``throw`` in ``Update`` and
``Script`` or ``throwPure`` in other contexts.

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- TRANSFER_LIMITED_BEGIN
  :end-before: -- TRANSFER_LIMITED_END

Finally, we can adapt our choice to catch this exception as well:

.. literalinclude:: daml/daml-intro-8/daml/Intro/Exceptions.daml
  :language: daml
  :start-after: -- ORDER_TRUSTED_LIMITED_BEGIN
  :end-before: -- ORDER_TRUSTED_LIMITED_END

For more information on exceptions, take a look at the
:ref:`language reference <exceptions>`.


Next Up
-------

We have now seen how to develop safe models and how we can handle
errors in those models in a robust and simple way. But the journey
doesn't stop there. In :doc:`9_Dependencies` you will learn how to
extend an already running application to enhance it with new
features. In that context you'll learn a bit more about the
architecture of Daml, about dependencies, and about identifiers.
