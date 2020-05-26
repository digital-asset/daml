.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _upgrade-overview:

Upgrading and extending DAML applications
#########################################

.. toctree::
   :hidden:

   automation

**Note:** Cross-SDK upgrades require DAML-LF 1.8 or newer.
This is the default starting from SDK 1.0. For older releases add
``build-options: ["--target=1.8"]`` to your ``daml.yaml`` to select
DAML-LF 1.8.

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

.. literalinclude:: example/coin-1.0.0/daml/CoinV1.daml
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

.. literalinclude:: example/coin-2.0.0/daml/CoinV2.daml
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

.. literalinclude:: example/coin-upgrade/daml/UpgradeFromCoinV1.daml
  :language: daml
  :start-after: -- UPGRADE_PROPOSAL_BEGIN
  :end-before: -- UPGRADE_PROPOSAL_END

Now we can define the *UpgradeCoinAgreement* template. This template
has one *nonconsuming* choice that takes the contract ID of a *Coin* contract, archives
this *Coin* contract and creates a *CoinWithAmount* contract with
the same issuer and owner and  the *amount* set to 1.

.. literalinclude:: example/coin-upgrade/daml/UpgradeFromCoinV1.daml
  :language: daml
  :start-after: -- UPGRADE_AGREEMENT_BEGIN
  :end-before: -- UPGRADE_AGREEMENT_END


Building and deploying coin-1.0.0
=================================

Let's see everything in action by first building and deploying ``coin-1.0.0``. After this we'll see how to deploy and upgrade to ``coin-2.0.0`` containing the ``CoinWithAmount`` template.

First we'll need a sandbox ledger to which we can deploy.

.. code-block:: none

   $ daml sandbox --port 6865

Now we'll setup the project for the original version of our coin. The project contains the DAML for just the ``Coin`` template, along with a ``CoinProposal`` template which will allow us to issue some coins in the example below.

Here is the project config.

.. literalinclude:: example/coin-1.0.0/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END


Now we can build and deploy ``coin-1.0.0`` of our Coin.

.. code-block:: none

   $ cd example/coin-1.0.0
   $ daml build
   $ daml ledger upload-dar --port 6865


Create some coin-1.0.0 coins
============================

Let's create some coins!

We'll use the navigator to connect to the ledger, and create two coins issued by Alice, and owned by Bob.

.. code-block:: none

   $ cd example/coin-1.0.0
   $ daml navigator server localhost 6865

We point a browser to http://localhost:4000, and follow the steps:

#. Login as Alice:
    #. Select Templates tab.
    #. Create a *CoinProposal* with Alice as issuer and Bob as owner.
    #. Create a 2nd proposal in the same way.
#. Login as Bob:
    #. Exercise the *CoinProposal_Accept* choice on both proposal contracts.


Building and deploying coin-2.0.0
=================================

Now we setup the project for the improved coins containing the *amount* field. This project contains only the ``CoinWithAmount`` template. The upgrade templates are in a third ``coin-upgrade`` package. While it would be possible to include the upgrade templates in the same package, this means that the package containing the new ``CoinWithAmount`` template depends on the previous version. With the approach taken here of keeping the upgrade templates in a separate package, the ``coin-1.0.0`` package is no longer needed once we have upgraded all coins.

It's worth stressing here that extensions always need to go into separate packages. We cannot just add the new definitions to the original project, rebuild and re-deploy. This is because the cryptographically computed package identifier would change, and would not match the package identifier of the original ``Coin`` contracts from ``coin-1.0.0`` which are live on the ledger.

Here is the new project config:

.. literalinclude:: example/coin-2.0.0/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END

Now we can build and deploy ``coin-2.0.0`` of our Coin.

.. code-block:: none

   $ cd example/coin-2.0.0
   $ daml build
   $ daml ledger upload-dar --port 6865

Building and deploying coin-upgrade
===================================

Having built and deployed ``coin-1.0.0`` and ``coin-2.0.0`` we are now
ready to build the upgrade package ``coin-upgrade``. The project
config references both ``coin-1.0.0`` and ``coin-2.0.0`` via the
``data-dependencies`` field. This allows us to import modules from the
respective packages which allows us to reference templates from
packages that we already uploaded to the ledger.

When following this example, ``path/to/coin-1.0.0.dar`` and
``path/to/coin-2.0.0.dar`` should be replaced by the relative or
absolute path to the DAR file created by building the respective
projects. Commonly the ``coin-1.0.0`` and ``coin-2.0.0`` projects
would be sibling directories in the file systems, so this path would
be: ``../coin-1.0.0/.daml/dist/coin-1.0.0.dar``.

.. literalinclude:: example/coin-upgrade/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END

The DAML for the upgrade contacts imports the modules for both the new and old coin versions.


.. literalinclude:: example/coin-upgrade/daml/UpgradeFromCoinV1.daml
  :language: daml
  :start-after: -- UPGRADE_MODULE_BEGIN
  :end-before: -- UPGRADE_MODULE_END

Now we can build and deploy ``coin-upgrade``. Note that uploading a
DAR also uploads its dependencies so if ``coin-1.0.0`` and
``coin-2.0.0`` had not already been deployed before, they would be
deployed as part of deploying ``coin-upgrade``.

.. code-block:: none

   $ cd example/coin-upgrade
   $ daml build
   $ daml ledger upload-dar --port 6865

Upgrade existing coins from coin-1.0.0 to coin-2.0.0
====================================================

We start the navigator again.

.. code-block:: none

   $ cd example/coin-upgrade
   $ daml navigator server localhost 6865

Finally, we point a browser to http://localhost:4000 and can effect the coin upgrades:

#. Login as Alice
    #. Select Templates tab.
    #. Create an ``UpgradeCoinProposal`` with Alice as issuer and Bob as owner.
#. Login as Bob
    #. Exercise the ``Accept`` choice of the upgrade proposal, creating an ``UpgradeCoinAgreement``.
#. Login again as Alice
    #. Use the ``UpgradeCoinAgreement`` repeatedly to upgrade any coin for which Alice is issuer and Bob is owner.

Further Steps
=============

For the upgrade of our coin model above, we performed all steps
manually via Navigator. However, if Alice had issued millions of
coins, performing all upgrading steps manually becomes infeasible.  It
thus becomes necessary to automate these steps. We will go through a
potential implementation of an automated upgrade in the :ref:`next section <upgrade-automation>`.
