.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Upgrading and extending DAML applications
#########################################

In applications backed by a centralized database controlled by a
single operator, it is possible to upgrade an application in a single
step that migrates all existing data to a new data model.

However, in a DAML application running on a distributed ledger, the
signatories of a contract have agreed to one specific version of a
template. Changing the definition of a template, e.g., by extending it
with a new choice without agreement from signatories of contracts of
that template would completely break the authority guarantees provided
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

.. literalinclude:: daml/Upgrade.daml
  :language: daml
  :start-after: -- COIN_BEGIN
  :end-before: -- COIN_END

A *Coin* has an issuer and an owner and both are signatories.  Our
goal is to extend this *Coin* template with a field that represents
the number of coins to avoid needing 1000 contracts to represent 1000
coins. (In a real application, you would also want choices for merging
and splitting such a *Coin*. For presentational purposes, we omit
those here.) We use a different name for the new template here. This
is not required as templates are identified by the triple
``(PackageId, ModuleName, TemplateName)`` but here we omit package ids
and module names for presentational purposes.

.. literalinclude:: daml/Upgrade.daml
  :language: daml
  :start-after: -- COIN_AMOUNT_BEGIN
  :end-before: -- COIN_AMOUNT_END

Now, that we defined our new template, we need to provide a way for
the signatories issuer and owner to agree to a contract being
upgraded. It would be possible to structure this such that issuer and
owner have to agree to an upgrade for each *Coin* contract
separately. However, since the template definition for those is all
the same, this is usually not necessary for most
applications. Instead, we collect agreement from the signatories only
once and use that to upgrade all coins. Since there are multiple
signatories involved here, we use a :ref:`Propose-Accept workflow
<intro propose accept>`. First, we define an *UpgradeCoinProposal*
template that will be created by the issuer. This template has an
*Accept* choice that the *owner* can exercise which will then create
an *UpgradeCoinAgreement*.

.. literalinclude:: daml/Upgrade.daml
  :language: daml
  :start-after: -- UPGRADE_PROPOSAL_BEGIN
  :end-before: -- UPGRADE_PROPOSAL_END

Now we can define the *UpgradeCoinAgreement* template. This template
has one *nonconsuming* choice that accepts a *Coin* contract, archives
this *Coin* contract and creates a *CoinWithAmount* contract with
*amount* set to 1.

.. literalinclude:: daml/Upgrade.daml
  :language: daml
  :start-after: -- UPGRADE_AGREEMENT_BEGIN
  :end-before: -- UPGRADE_AGREEMENT_END

.. TODO Turn this into a full example project with multiple packages
.. and explain builds and deployment.

.. Building and deploying
.. ======================
