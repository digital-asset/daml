.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _upgrade-overview:

Upgrading and extending DAML applications
#########################################

**Note:** Cross-SDK upgrades are only supported if you compile to
DAML-LF 1.8 by specifying ``--target=1.8`` in the ``build-options``
field in your ``daml.yaml``.

In applications backed by a centralized database controlled by a
single operator, it is possible to upgrade an application in a single
step that migrates all existing data to a new data model.

However, in a DAML application running on a distributed ledger, the
signatories of a contract have agreed to one specific version of a
template. Changing the definition of a template, e.g., by extending it
with a new choice without agreement from signatories of contracts of
that template would completely break the authorization guarantees provided
by DAML.

Therefore, DAML takes a different approach to upgrades and
extensions. Rather than having a separate concept of data migration
that sidesteps the fundamental guarantees provided by DAML, *upgrades
are expressed as DAML contracts*. This means that the same guarantees
and rules that apply to other DAML contracts also apply to upgrades.

In a DAML application, it therefore makes sense to think of upgrades
as an *extension of an existing application* instead of an operation
that replaces exiting contracts with a newer version of those
contracts. The existing templates stay on the ledger and can still be
used. Contracts of existing templates are not automatically replaced
by newer versions. However, the application is extended with new
templates and if all signatories of a contract agree, a choice can
archive the old version of a contract and create a new contract
instead.

Structuring upgrade contracts
=============================

Upgrade contracts are specific to the templates that are being
upgraded. However, there are common patterns between most of them. We
use the example of a simple *Coin* template as an example here.
We have some prescience that there will be future versions of *Coin*,
and so place the definition of ``Coin`` in a module named ``CoinV1``

.. literalinclude:: example/coinV1/daml/CoinV1.daml
  :language: daml
  :start-after: -- COIN_BEGIN
  :end-before: -- COIN_END

A *Coin* has an issuer and an owner and both are signatories.  Our
goal is to extend this *Coin* template with a field that represents
the number of coins to avoid needing 1000 contracts to represent 1000
coins. (In a real application, you would also want choices for merging
and splitting such a *Coin*. For the sake of simplicity, we omit
those here.) We use a different name for the new template here. This
is not required as templates are identified by the triple
*(PackageId, ModuleName, TemplateName)*

.. literalinclude:: example/coinV2/daml/CoinV2.daml
  :language: daml
  :start-after: -- COIN_AMOUNT_BEGIN
  :end-before: -- COIN_AMOUNT_END

Next, we need to provide a way for
the signatories, issuer and owner, to agree to a contract being
upgraded. It would be possible to structure this such that issuer and
owner have to agree to an upgrade for each individual *Coin* contract
separately. However, since the template definition for all of them is
the same, this is usually not necessary for most
applications. Instead, we collect agreement from the signatories only
once and use that to upgrade all coins. Since there are multiple
signatories involved here, we use a :ref:`Propose-Accept workflow
<intro propose accept>`. First, we define an *UpgradeCoinProposal*
template that will be created by the issuer. This template has an
*Accept* choice that the *owner* can exercise which will then create
an *UpgradeCoinAgreement*.

.. literalinclude:: example/coinV2/daml/UpgradeFromCoinV1.daml
  :language: daml
  :start-after: -- UPGRADE_PROPOSAL_BEGIN
  :end-before: -- UPGRADE_PROPOSAL_END

Now we can define the *UpgradeCoinAgreement* template. This template
has one *nonconsuming* choice that takes the contract ID of a *Coin* contract, archives
this *Coin* contract and creates a *CoinWithAmount* contract with
the same issuer and owner and  the *amount* set to 1.

.. literalinclude:: example/coinV2/daml/UpgradeFromCoinV1.daml
  :language: daml
  :start-after: -- UPGRADE_AGREEMENT_BEGIN
  :end-before: -- UPGRADE_AGREEMENT_END


Building and deploying version-1
================================

Let's see everything in action by first building and deploying version-1 of our coin. After this we'll see how to deploy and upgrade to version-2.

First we'll need a sandbox ledger to which we can deploy.

.. code-block:: none

   $ daml sandbox --port 6865

Now we'll setup the project for the original version of our coin. The project contains the DAML for just the ``Coin`` template, along with a ``CoinProposal`` template which will allow us to issue some coins in the example below.

Here is the project config.

.. literalinclude:: example/coinV1/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END


Now we can build and deploy version-1 of our Coin.

.. code-block:: none

   $ cd example/coinV1
   $ daml build
   $ daml ledger upload-dar --port 6865


Create some version-1 coins
===========================

Let's create some coins!

We'll use the navigator to connect to the ledger, and create two coins issued by Alice, and owned by Bob.

.. code-block:: none

   $ cd example/coinV1
   $ daml navigator server localhost 6865

We point a browser to http://localhost:4000, and follow the steps:

#. Login as Alice:
    #. Select Templates tab.
    #. Create a *CoinProposal* with Alice as issuer and Bob as owner.
    #. Create a 2nd proposal in the same way.
#. Login as Bob:
    #. Exercise the *CoinProposal_Accept* choice on both proposal contracts.


Building and deploying version-2
================================

Now we setup the project for the improved coins containing the *amount* field. This project contains the templates for ``CoinWithAmount``, ``UpgradeCoinProposal`` and ``UpgradeCoinAgreement``.

It's worth stressing here that we must setup a new project for version-2. We cannot just add the new definitions to the original project, rebuild and re-deploy. This is because the cryptographically computed package-id would change, and would not match the package-id of version-1 coin-contracts which are live of the ledger.

Here is the new project config:

.. literalinclude:: example/coinV2/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END

Note how ``coin-1.0.0.dar`` is listed as a ``data-dependency``. This allows the upgrade templates to make reference to the ``Coin`` template from the ``CoinV1`` module, even though that module is not part of this project, but is part of original package already deployed to the ledger.

The DAML for the upgrade contacts imports the modules for both the new and old coin versions.


.. literalinclude:: example/coinV2/daml/UpgradeFromCoinV1.daml
  :language: daml
  :start-after: -- UPGRADE_MODULE_BEGIN
  :end-before: -- UPGRADE_MODULE_END

Now we can build and deploy version-2 of our Coin.

.. code-block:: none

   $ cd example/coinV2
   $ daml build
   $ daml ledger upload-dar --port 6865


Upgrade existing coins from version-1 to version-2
==================================================

We start the navigator again.

.. code-block:: none

   $ cd example/coinV2
   $ daml navigator server localhost 6865

Finally, we point a browser to http://localhost:4000 and can effect the coin upgrades:

#. Login as Alice
    #. Select Templates tab.
    #. Create an `UpgradeCoinProposal` with Alice as issuer and Bob as owner.
#. Login as Bob
    #. Exercise the `Accept` choice of the upgrade proposal, creating an `UpgradeCoinAgreement`.
#. Login again as Alice
    #. Use the `UpgradeCoinAgreement` repeatedly to upgrade any coin for which Alice is issuer and Bob is owner.
