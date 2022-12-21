.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml Interfaces
===============

After defining a few templates in Daml, you've probably found yourself repeating
some behaviors between them. For instance, many templates have a notion of
ownership where a party is designated as the "owner" of the contract, and this
party has the power to transfer ownership of the contract to a different party
(subject to that party agreeing to the transfer!). Daml Interfaces provide a
way to abstract those behaviors into a Daml type.

.. hint::

  Remember that you can load all the code for this section into a folder called
  ``intro13`` by running ``daml new intro13 --template daml-intro-13``


Context
-------

First, let's define some templates.

.. literalinclude:: daml/daml-intro-13/daml/Cash.daml
  :language: daml
  :start-after: -- TEMPLATE_CASH_DEF_BEGIN
  :end-before: -- TEMPLATE_CASH_DEF_END

.. literalinclude:: daml/daml-intro-13/daml/Cash.daml
  :language: daml
  :start-after: -- TEMPLATE_CASH_TF_DEF_BEGIN
  :end-before: -- TEMPLATE_CASH_TF_DEF_END

These declarations from ``intro13/daml/Cash.daml`` define ``Cash`` as a simple
template with an issuer and an owner, a currency and an amount. A ``Cash``
contract grants its ``owner`` the choice ``ProposeCashTransfer``, which allows
the owner to propose another party, the ``newOwner``, to take over ownership of
the asset.

This is mediated by the ``CashTransferProposal`` template, which grants two
choices to the new owner: ``AcceptCashTransferProposal`` and
``RejectCashTransferProposal``, each of which archives the
``CashTransferProposal`` and creates a new ``Cash`` contract; in the former case
the ``owner`` of the new ``Cash`` will be ``newOwner``, in the latter, it will
be the existing ``owner``. Finally, the existing ``owner`` also has the choice
``WithdrawCashTransferProposal``, which archives the proposal and creates a new
``Cash`` contract with identical contents to the original one.

Overall, the effect is that a ``Cash`` contract can be transferred to another
party, if they agree, in two steps.

The declarations from ``intro13/daml/NFT.daml`` declare the templates ``NFT``
and ``NFTTransferProposal`` following the same pattern, with names changed where
appropriate, with the main difference being that an ``NFT`` has a ``url : Text``
field whereas ``Cash`` has ``currency : Text`` and ``amount : Decimal``.


Interface definition
--------------------

To abstract this behavior, we introduce two interfaces, ``IAsset`` and
``IAssetTransferProposal``.

.. hint::
  It is not mandatory to prefix interface names with the letter ``I``, but it
  can be convenient to tell at a glance whether or not a type is an interface.

.. literalinclude:: daml/daml-intro-13/daml/IAsset.daml
  :language: daml
  :start-after: -- INTERFACE_IASSET_DEF_BEGIN
  :end-before: -- INTERFACE_IASSET_DEF_END

.. literalinclude:: daml/daml-intro-13/daml/IAsset.daml
  :language: daml
  :start-after: -- INTERFACE_IASSET_TF_DEF_BEGIN
  :end-before: -- INTERFACE_IASSET_TF_DEF_END

There are a few things happening here:

1. For each interface, we have defined a ``viewtype``. This is mandatory for all
   interfaces. All viewtypes must be serializable records, since <insert
   justification here>. This declaration means that the special ``view`` method,
   when applied to a value of this interface, will return the specified type, in
   this case ``VAsset``. This is the definition of ``VAsset``:

   .. literalinclude:: daml/daml-intro-13/daml/IAsset.daml
     :language: daml
     :start-after: -- DATA_VASSET_DEF_BEGIN
     :end-before: -- DATA_VASSET_DEF_END


2. We have defined the methods ``setOwner`` and ``toTransferProposal`` as part
   of the ``IAsset`` interface, and method ``asset`` as part of the
   ``IAssetTransferProposal`` interface. Later, when we provide instances of
   these interfaces, we'll see that it is mandatory to implement each of these
   methods.

3. We have defined the choice ``ProposeIAssetTransfer`` as part of the
   ``IAsset`` interface, and the choices ``AcceptIAssetTransferProposal``,
   ``RejectIAssetTransferProposal`` and ``WithdrawIAssetTransferProposal`` as
   part of the ``IAssetTransferProposal`` interface. These correspond one-to-one
   with the choices of ``Cash`` / ``CashTransferProposal`` and ``NFT`` /
   ``NFTTransferProposal``.

   You should notice that the choice controller and the choice body are defined
   in terms of the methods that we bundled with the interfaces, including the
   special ``view`` method. For example, the controller of
   ``choice ProposeIAssetTransfer`` is ``(view this).owner``, that is, it's the
   ``owner`` field of the ``view`` for the implicit current contract ``this``,
   in other words, the owner of the current contract. The body of this choice is
   ``create (toTransferProposal this newOwner)``, so it creates a new contract
   whose contents are the result of applying the ``toTransferProposal`` method
   to the current contract and the ``newOwner`` field of the choice argument.

.. hint::
  For a detailed explanation of the syntax used here, check out :ref:`daml-ref-interfaces`


Interface instances
-------------------

On its own, an interface isn't very useful, since all contracts on the ledger
must belong to some template type. In order to make the link between an
interface and a template, we define an interface instance on either the
template or the interface. In this example, we add
``interface instance IAsset for Cash`` and
``interface instance IAssetTransferProposal for CashTransferProposal``:

.. literalinclude:: daml/daml-intro-13/daml/Cash.daml
  :language: daml
  :start-after: -- INTERFACE_INSTANCE_IASSET_FOR_CASH_BEGIN
  :end-before: -- INTERFACE_INSTANCE_IASSET_FOR_CASH_END

.. literalinclude:: daml/daml-intro-13/daml/Cash.daml
  :language: daml
  :start-after: -- INTERFACE_INSTANCE_IASSET_TF_FOR_CASH_TF_BEGIN
  :end-before: -- INTERFACE_INSTANCE_IASSET_TF_FOR_CASH_TF_END

The corresponding interface instances for ``NFT`` and ``NFTTransferProposal``
are very similar so we omit them here.

Inside the interface instances, we must implement every method defined for the
corresponding interface, including the special ``view`` method.  Within each
method implementation the variable ``this`` is in scope, corresponding to the
implict current contract, which will have the type of the template (in this case
``Cash`` / ``CashTransferProposal``), as well as each of the fields of the
template type. For example, the ``view`` definition in ``interface instance
IAsset for Cash`` mentions ``issuer`` and ``owner``, which refer to the issuer
and owner of the current ``Cash`` contract, as well as ``this``, which refers to
the entire ``Cash`` contract payload.

The implementations given for each method must match the types given in the
interface definition. In particular, we see that the ``view`` definition
discussed above returns a ``VAsset``, corresponding to ``IAsset``'s
``viewtype``. Similarly, ``setOwner`` returns an ``IAsset``, and
``toTransferProposal`` returns an ``IAssetTransferProposal``. In these last two,
we have used the function ``toInterface`` to convert values from a template type
into an interface type. In ``setOwner``, ``toInterface`` is applied to a
``Cash`` value (``this with owner = newOwner``), producing an ``IAsset`` value;
in ``toTransferProposal``, it's applied to a ``CashTransferProposal`` value
(``CashTransferProposal with {...}``), producing an ``IAssetTransferProposal``
value.

.. * toInterfaceContractId
.. * queryInterface
.. * queryInterfaceContractId
.. * polymorphic tests
