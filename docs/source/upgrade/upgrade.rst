.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _upgrade-overview:

Upgrading Daml Applications
###########################

.. toctree::
   :hidden:

In applications backed by a centralized database controlled by a
single operator, it is possible to upgrade an application in a single
step that migrates all existing data to a new data model.

As a running example, let's imagine a centralized database containing
carbon offset certificates. Its operator created the database schema
with

.. code-block:: SQL

  CREATE TABLE carbon_certs (
    carbon_metric_tons VARINT,
    owner VARCHAR NOT NULL
    issuer VARCHAR NOT NULL
  )

The certificate has a field for the quantity of offset carbon in
metric tons, an owner and an issuer.

In the next iteration of the application, the operator decides to also
store and display the carbon offset method. In the centralized case,
the operator can upgrade the database by executing the single SQL
command

.. code-block:: SQL

  ALTER TABLE carbon_certs ADD carbon_offset_method VARCHAR DEFAULT "unknown"

This adds a new column to the ``carbon_certs`` table and inserts the
value ``unknown`` for all existing entries.

While upgrading this centralized database is simple and convenient,
its data entries lack any kind of signature and hence proof of
authenticity. The data consumers need to trust the operator.

In contrast, Daml templates always have at least one signatory. The
consequence is that the upgrade process for a Daml application needs
to be different.

Daml Upgrade Overview
=====================

In a Daml application running on a distributed ledger, the signatories
of a contract have agreed to one specific version of a template.
Changing the definition of a template, e.g., by extending it with a
new data field or choice without agreement from its signatories would
completely break the authorization guarantees provided by Daml.

Therefore, Daml takes a different approach to upgrades and
extensions. Rather than having a separate concept of data migration
that sidesteps the fundamental guarantees provided by Daml, *upgrades
are expressed as Daml contracts*. This means that the same guarantees
and rules that apply to other Daml contracts also apply to upgrades.

In a Daml application, it thus makes sense to think of upgrades as an
*extension of an existing application* instead of an operation that
replaces existing contracts with a newer version. The existing
templates stay on the ledger and can still be used. Contracts of
existing templates are not automatically replaced by newer versions.
However, the application is extended with new templates. Then if all
signatories of a contract agree, a choice can archive the old version
of a contract and create a new contract instead.

Structure Upgrade Contracts
===========================

Upgrade contracts are specific to the templates that are being
upgraded. But most of them share common patterns. Here is the
implementation of the above ``carbon_certs`` schema in Daml. We have
some prescience that there will be future versions of *CarbonCert*,
and so place the definition of ``CarbonCert`` in a module named
``CarbonV1``

.. literalinclude:: example/carbon-1.0.0/daml/CarbonV1.daml
  :language: daml
  :start-after: -- CARBON_BEGIN
  :end-before: -- CARBON_END

A *CarbonCert* has an issuer and an owner. Both are signatories. Our
goal is to extend this *CarbonCert* template with a field that adds
the method used to offset the carbon.  We use a different name for the
new template here for clarity. This is not required as templates are
identified by the triple *(PackageId, ModuleName, TemplateName)*.

.. literalinclude:: example/carbon-2.0.0/daml/CarbonV2.daml
  :language: daml
  :start-after: -- CARBON_METHOD_BEGIN
  :end-before: -- CARBON_METHOD_END

Next, we need to provide a way for the signatories to agree to a
contract being upgraded. It would be possible to structure this such
that issuer and owner have to agree to an upgrade for each individual
*CarbonCert* contract separately. Since the template definition for
all of them is the same, this is usually not necessary for most
applications. Instead, we collect agreement from the signatories only
once and use that to upgrade all carbon certificates.

Since there are multiple signatories involved here, we use a
:ref:`Propose-Accept workflow <intro propose accept>`.  First, we
define an *UpgradeCarbonCertProposal* template that will be created by
the issuer. This template has an *Accept* choice that the *owner* can
exercise. Upon execution it will then create an
*UpgradeCarbonCertAgreement*.

.. literalinclude:: example/carbon-upgrade/daml/UpgradeFromCarbonCertV1.daml
  :language: daml
  :start-after: -- UPGRADE_PROPOSAL_BEGIN
  :end-before: -- UPGRADE_PROPOSAL_END

Now we can define the *UpgradeCarbonCertAgreement* template. This
template has one *nonconsuming* choice that takes the contract ID of a
*CarbonCert* contract, archives this *CarbonCert* contract and creates a
*CarbonCertWithMethod* contract with the same issuer and owner and  the
*carbon_offset_method* set to ``unknown``.

.. literalinclude:: example/carbon-upgrade/daml/UpgradeFromCarbonCertV1.daml
  :language: daml
  :start-after: -- UPGRADE_AGREEMENT_BEGIN
  :end-before: -- UPGRADE_AGREEMENT_END


Build and Deploy ``carbon-1.0.0``
=================================

Let's see everything in action by first building and deploying
``carbon-1.0.0``. After this we'll see how to deploy and upgrade to
``carbon-2.0.0`` containing the ``CarbonCertWithMethod`` template.

First we'll need a sandbox ledger to which we can deploy.

.. code-block:: none

   $ daml sandbox --port 6865

Now we'll setup the project for the original version of our
certificate. The project contains the Daml for just the ``CarbonCert``
template, along with a ``CarbonCertProposal`` template which will
allow us to issue some coins in the example below.

Here is the project config.

.. literalinclude:: example/carbon-1.0.0/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END


Now we can build and deploy ``carbon-1.0.0``.

.. code-block:: none

   $ cd example/carbon-1.0.0
   $ daml build
   $ daml ledger upload-dar --port 6865


Create carbon-1.0.0 Certificates
================================

Let's create some certificates!

First, we run a setup script to create 3 users ``alice``, ``bob`` and ``charlie``
and corresponding parties. We write out the actual party ids to a JSON
file so we can later use them in Navigator.

.. code-block:: none

   $ cd example/carbon-1.0.0
   $ daml script --dar .dar/dist/carbon-1.0.0.dar --script-name Setup:setup --ledger-host localhost --ledger-port 6865 --output-file parties.json

The resulting ``parties.json`` file will look similar to the following but the actual party ids will
vary.

.. code-block:: json

   {
     "alice": "party-19a21501-ba87-47be-90a6-692dfaefe64a::12203977cedf2d394073b4c58036e047fcc590f7f2d61d82503df431473c4277fe70",
     "bob": "party-7ecb1d67-1d20-4612-be67-b5741c86204d::12203977cedf2d394073b4c58036e047fcc590f7f2d61d82503df431473c4277fe70"
     "charlie": "party-fae6a574-9860-422a-9fd4-7ca2f7295e41::12203977cedf2d394073b4c58036e047fcc590f7f2d61d82503df431473c4277fe70"
   }

We'll use the navigator to connect to the ledger, and create two
certificates issued by Alice, and owned by Bob.

.. code-block:: none

   $ cd example/carbon-1.0.0
   $ daml navigator server localhost 6865

We point a browser to http://localhost:4000, and follow the steps:

#. Login as ``alice``:
    #. Select Templates tab.
    #. Create a *CarbonCertProposal* with Alice as issuer and Bob as owner and an arbitrary value for the ``carbon_metric_tons`` field.
       Note that in place of Alice and Bob, you need to use the party ids from the
       previously created ``parties.json``.
    #. Create a 2nd proposal in the same way.
#. Login as ``bob``:
    #. Exercise the *CarbonCertProposal_Accept* choice on both proposal contracts.


Build and Deploy ``carbon-2.0.0``
=================================

Now we setup the project for the improved certificates containing the *carbon_offset_method* field. This project contains only the ``CarbonCertWithMethod`` template. The upgrade templates are in a third ``carbon-upgrade`` package. While it would be possible to include the upgrade templates in the same package, this means that the package containing the new ``CarbonCertWithMethod`` template depends on the previous version. With the approach taken here of keeping the upgrade templates in a separate package, the ``carbon-1.0.0`` package is no longer needed once we have upgraded all certificates.

It's worth stressing here that extensions always need to go into separate packages. We cannot just add the new definitions to the original project, rebuild and re-deploy. This is because the cryptographically computed package identifier would change. Consequently, it would not match the package identifier of the original ``CarbonCert`` contracts from ``carbon-1.0.0`` which are live on the ledger.

Here is the new project config:

.. literalinclude:: example/carbon-2.0.0/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END

Now we can build and deploy ``carbon-2.0.0``.

.. code-block:: none

   $ cd example/carbon-2.0.0
   $ daml build
   $ daml ledger upload-dar --port 6865

Build and Deploy ``carbon-upgrade``
===================================

Having built and deployed ``carbon-1.0.0`` and ``carbon-2.0.0`` we are
now ready to build the upgrade package ``carbon-upgrade``. The project
config references both ``carbon-1.0.0`` and ``carbon-2.0.0`` via the
``data-dependencies`` field. This allows us to import modules from the
respective packages. With these imported modules we can reference
templates from packages that we already uploaded to the ledger.

When following this example, ``path/to/carbon-1.0.0.dar`` and
``path/to/carbon-2.0.0.dar`` should be replaced by the relative or
absolute path to the DAR file created by building the respective
projects. Commonly the ``carbon-1.0.0`` and ``carbon-2.0.0`` projects
would be sibling directories in the file systems, so this path would
be: ``../carbon-1.0.0/.daml/dist/carbon-1.0.0.dar``.

.. literalinclude:: example/carbon-upgrade/daml.yaml
  :language: yaml
  :start-after: # BEGIN
  :end-before: # END

The Daml for the upgrade contracts imports the modules for both the new
and old certificate versions.


.. literalinclude:: example/carbon-upgrade/daml/UpgradeFromCarbonCertV1.daml
  :language: daml
  :start-after: -- UPGRADE_MODULE_BEGIN
  :end-before: -- UPGRADE_MODULE_END

Now we can build and deploy ``carbon-upgrade``. Note that uploading a
DAR also uploads its dependencies so if ``carbon-1.0.0`` and
``carbon-2.0.0`` had not already been deployed before, they would be
deployed as part of deploying ``carbon-upgrade``.

.. code-block:: none

   $ cd example/carbon-upgrade
   $ daml build
   $ daml ledger upload-dar --port 6865

Upgrade Existing Certificates from carbon-1.0.0 to carbon-2.0.0
=================================================================

We start the navigator again.

.. code-block:: none

   $ cd example/carbon-upgrade
   $ daml navigator server localhost 6865

Finally, we point a browser to http://localhost:4000 and can start the carbon certificates upgrades:

#. Login as ``alice``
    #. Select Templates tab.
    #. Create an ``UpgradeCarbonCertProposal`` with Alice as issuer
       and Bob as owner. As before, in place of Alice and Bob use the
       party ids from ``parties.json``.
#. Login as ``bob``
    #. Exercise the ``Accept`` choice of the upgrade proposal, creating an ``UpgradeCarbonCertAgreement``.
#. Login again as ``alice``
    #. Use the ``UpgradeCarbonCertAgreement`` repeatedly to upgrade any certificate for which Alice is issuer and Bob is owner.

Further Steps
=============

For the upgrade of our carbon certificate model above, we performed all steps
manually via Navigator. However, if Alice had issued millions of
carbon certificates, performing all upgrading steps manually becomes infeasible.  It
thus becomes necessary to automate these steps. We will go through a
potential implementation of an automated upgrade in the :ref:`next section <upgrade-automation>`.
