.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

First, define some templates:

.. literalinclude:: daml/daml-intro-13/daml/Cash.daml
  :language: daml
  :start-after: -- TEMPLATE_CASH_DEF_BEGIN
  :end-before: -- TEMPLATE_CASH_DEF_END

.. literalinclude:: daml/daml-intro-13/daml/Cash.daml
  :language: daml
  :start-after: -- TEMPLATE_CASH_TF_DEF_BEGIN
  :end-before: -- TEMPLATE_CASH_TF_DEF_END

These declarations from ``intro13/daml/Cash.daml`` define ``Cash`` as a simple
template with an issuer, an owner, a currency, and an amount. A ``Cash``
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

To abstract this behavior, you will next introduce two interfaces: ``IAsset`` and
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

1. For each interface, you have defined a ``viewtype``. This is mandatory for all
   interfaces. All viewtypes must be serializable records. This declaration
   means that the special ``view`` method, when applied to a value of this
   interface, will return the specified type (in this case ``VAsset``). This is
   the definition of ``VAsset``:

   .. literalinclude:: daml/daml-intro-13/daml/IAsset.daml
     :language: daml
     :start-after: -- DATA_VASSET_DEF_BEGIN
     :end-before: -- DATA_VASSET_DEF_END

   .. hint::
     See :ref:`daml-ref-serializable-types` for more information on
     serializability requirements.

2. You have defined the methods ``setOwner`` and ``toTransferProposal`` as part
   of the ``IAsset`` interface, and method ``asset`` as part of the
   ``IAssetTransferProposal`` interface. Later, when you provide instances of
   these interfaces, you will see that it is mandatory to implement each of these
   methods.

3. You have defined the choice ``ProposeIAssetTransfer`` as part of the
   ``IAsset`` interface, and the choices ``AcceptIAssetTransferProposal``,
   ``RejectIAssetTransferProposal`` and ``WithdrawIAssetTransferProposal`` as
   part of the ``IAssetTransferProposal`` interface. These correspond one-to-one
   with the choices of ``Cash`` / ``CashTransferProposal`` and ``NFT`` /
   ``NFTTransferProposal``.

   Notice that the choice controller and the choice body are defined
   in terms of the methods that you bundled with the interfaces, including the
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
interface and a template, you must define an interface instance inside the body of
either the template or the interface. In this example, add:
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

Inside the interface instances, you must implement every method defined for the
corresponding interface, including the special ``view`` method.  Within each
method implementation the variable ``this`` is in scope, corresponding to the
implict current contract, which will have the type of the template (in this case
``Cash`` / ``CashTransferProposal``), as well as each of the fields of the
template type. For example, the ``view`` definition in ``interface instance
IAsset for Cash`` mentions ``issuer`` and ``owner``, which refer to the issuer
and owner of the current ``Cash`` contract, as well as ``this``, which refers to
the entire ``Cash`` contract payload.

The implementations given for each method must match the types given in the
interface definition. Notice that the ``view`` definition
discussed above returns a ``VAsset``, corresponding to ``IAsset``'s
``viewtype``. Similarly, ``setOwner`` returns an ``IAsset``, and
``toTransferProposal`` returns an ``IAssetTransferProposal``. In these last two,
the function ``toInterface`` converts values from a template type
into an interface type. In ``setOwner``, ``toInterface`` is applied to a
``Cash`` value (``this with owner = newOwner``), producing an ``IAsset`` value;
in ``toTransferProposal``, it is applied to a ``CashTransferProposal`` value
(``CashTransferProposal with {...}``), producing an ``IAssetTransferProposal``
value.


Using an interface
------------------

Now that we have some interfaces and templates with instances for them, we can
reduce duplication in the code for different templates by instead going through
the common interface.

For instance, both ``Cash`` and ``NFT`` are ``Asset``\s, which means that
contracts of either template have an owner who can propose to transfer the
contract to a third party. Thus, we use Daml Script (see
:ref:`testing-using-script`) to test that the same contract can be created by
``Alice`` and successively transfered to ``Bob`` and then ``Charlie``, who then
proposes to transfer to ``Dominic``, who rejects the proposal, and finally to
``Emily`` before withdrawing the proposal, so in the end the contract remains in
``Charlie``'s ownership. This procedure is tested on the ``Cash`` and ``NFT``
templates by the Daml Script tests ``cashTest`` and ``nftTest``, respectively,
both defined in ``intro13/daml/Main.daml``.

But that's a lot of duplication! ``cashTest`` and ``nftTest`` only differ in
the line that creates the original asset and in the names of the choices used.
With our new interfaces ``IAsset`` and ``IAssetTransferProposal``, we can write
the body of this test a single time, which we name ``mkAssetTest``,

.. literalinclude:: daml/daml-intro-13/daml/Main.daml
  :language: daml
  :start-after: -- MK_ASSET_TEST_BEGIN
  :lines: 2

The idea is that it isn't the test itself, but rather a recipe for making the
test given some inputs - in this case, ``assetTxt`` (a label used for
debugging), ``Parties {..}`` (a structure containing the ``Party`` values for
``Alice`` and friends) and finally ``mkAsset`` (a function that returns a
contract value of type ``t`` when given two ``Party`` arguments - the constraint
``Implements t IAsset`` means that ``t`` must be some template with an interface
instance for ``IAsset``).

Before looking at the body of ``mkAssetTest``, notice how we use it to define
the new tests ``cashAssetTest`` and ``nftAssetTest``; these are almost identical
except for the label and function given in each case to ``mkAssetTest``. In
effect, we have abstracted those away, so we don't need to include those details
in the body of ``mkAssetTest``:

.. literalinclude:: daml/daml-intro-13/daml/Main.daml
  :language: daml
  :start-after: -- CASH_ASSET_TEST_BEGIN
  :end-before: -- CASH_ASSET_TEST_END

.. literalinclude:: daml/daml-intro-13/daml/Main.daml
  :language: daml
  :start-after: -- MK_CASH_BEGIN
  :end-before: -- MK_CASH_END

.. literalinclude:: daml/daml-intro-13/daml/Main.daml
  :language: daml
  :start-after: -- NFT_ASSET_TEST_BEGIN
  :end-before: -- NFT_ASSET_TEST_END

.. literalinclude:: daml/daml-intro-13/daml/Main.daml
  :language: daml
  :start-after: -- MK_NFT_BEGIN
  :end-before: -- MK_NFT_END

In turn, ``mkAssetTest`` isn't very different from other Daml ``Script``\s you
might have written before: it uses ``do`` notation as usual, including
``submit`` blocks constructed from ``Command``\s that define the ordered
transactions that take place in the test. The main difference is that when
querying values of interface types we cannot use the functions ``query`` and
``queryContractId``; instead, we must use ``queryInterface``  (for obtaining the
set of visible active contracts of a given interface type) and
``queryInterfaceContractId`` (for obtaining a single contract given its
``ContractId``). Importantly, these functions return the *view* of the contract
corresponding to the used interface, rather than the contract record itself.
This is because the ledger might contain contracts of template types that we
don't know about but that do implement our interface, so the view is the only
sensible thing it can see.

Also, note that right after creating the asset with ``createCmd``, we convert
the resulting ``ContractId t`` into a ``ContractId IAsset`` using
``toInterfaceContractId``, which allows us to exercise ``IAsset`` choices on it.

.. literalinclude:: daml/daml-intro-13/daml/Main.daml
  :language: daml
  :start-after: -- MK_ASSET_TEST_BEGIN
  :end-before: -- MK_ASSET_TEST_END
