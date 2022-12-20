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
   justification here>.

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

.. * toInterface
.. * toInterfaceContractId
.. * queryInterface
.. * queryInterfaceContractId
.. * polymorphic tests
