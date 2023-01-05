.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _upgrade-automation:

Automating the Upgrade Process
##############################

In this section, we are going to automate the upgrade of our carbon certificate
process using :doc:`Daml Script</daml-script/index>` and
:doc:`Daml Triggers </triggers/index>`. Note that automation for upgrades is
specific to an individual application, just like the upgrade models.
Nevertheless, we have found that the pattern shown here
occurs frequently.

Structure the Upgrade
=====================

There are three kinds of actions performed during the upgrade:

#. Alice creates ``UpgradeCarbonCertProposal`` contracts. We assume here,
   that Alice wants to upgrade all ``CarbonCert`` contracts she has
   issued. Since the ``UpgradeCarbonCertProposal`` proposal is specific to
   each owner, Alice has to create one ``UpgradeCarbonCertProposal`` per
   owner. There can be potentially many owners but this step only has
   to be performed once assuming Alice will not issue more ``CarbonCert``
   contracts after this point.
#. Bob and other owners accept the ``UpgradeCarbonCertProposal``. To keep
   this example simple, we assume that there are only carbon certificates issued by
   Alice. Therefore, each owner has to accept at most one proposal.
#. As owners accept upgrade proposals, Alice has to upgrade each
   certificate. This means that she has to execute the upgrade choice once
   for each certificate. Owners will not all accept the upgrade at the same
   time and some might never accept it. Therefore, this should be a
   long-running process that upgrades all carbon certificates of a given owner as
   soon as they accept the upgrade.

Given those constraints, we are going to use the following tools for
the upgrade:

#. A Daml script that will be executed once by Alice and creates an
   ``UpgradeCarbonCertProposal`` contract for each owner.
#. Navigator to accept the ``UpgradeCarbonCertProposal`` as Bob. While we
   could also use a Daml script to accept the proposal, this step will
   often be exposed as part of a web UI so doing it interactively in
   Navigator resembles that workflow more closely.
#. A long-running Daml trigger that upgrades all ``CarbonCert`` contracts
   for which there is a corresponding ``UpgradeCarbonCertAgreement``.

Implementation of the Daml Script
=================================

In our Daml Script, we are first going to query the ACS (Active Contract Set) to find all
``CarbonCert`` contracts issued by us. Next, we are going to extract the
owner of each of those contracts and remove any duplicates coming from
multiple certificates issued to the same owner. Finally, we iterate over the
owners and create an ``UpgradeCarbonCertAgreement`` contract for each owner.

.. literalinclude:: example/carbon-initiate-upgrade/daml/InitiateUpgrade.daml
   :language: daml
   :start-after: -- INITIATE_UPGRADE_BEGIN
   :end-before: -- INITIATE_UPGRADE_END

Implementation of the Daml Trigger
==================================

Our trigger does not need any custom user state and no heartbeat so
the only interesting field in its definition is the rule.

.. literalinclude:: example/carbon-upgrade-trigger/daml/UpgradeTrigger.daml
   :language: daml
   :start-after: -- TRIGGER_BOILERPLATE_BEGIN
   :end-before: -- TRIGGER_BOILERPLATE_END

In our rule, we first filter out all agreements and certificates issued by
us. Next, we iterate over all agreements. For each agreement we filter
the certificates by the owner of the agreement and finally upgrade the certificate
by exercising the ``Upgrade`` choice. We mark the certificate as pending
which temporarily removes it from the ACS and therefore stops the
trigger from trying to upgrade the same certificate multiple times if the
rule is triggered in quick succession.

.. literalinclude:: example/carbon-upgrade-trigger/daml/UpgradeTrigger.daml
   :language: daml
   :start-after: -- TRIGGER_RULE_BEGIN
   :end-before: -- TRIGGER_RULE_END

The trigger is a long-running process and the rule will be executed
whenever the state of the ledger changes. So whenever an owner accepts
an upgrade proposal, the trigger will run the rule and upgrade all
certificates of that owner.

Deploy and Execute the Upgrade
==============================

Now that we defined our Daml script and our trigger, it is time to use
them! If you still have Sandbox running from the previous section,
stop it to clear out all data before continuing.

First, we start sandbox passing in the ``carbon-upgrade`` DAR. Since a
DAR includes all transitive dependencies, this includes ``carbon-1.0.0``
and ``carbon-2.0.0``.

.. code-block:: none

   $ cd example/carbon-upgrade
   $ daml sandbox --dar .daml/dist/carbon-upgrade-1.0.0.dar

To simplify the setup here, we use a Daml script to create 3 parties
Alice, Bob and Charlie and two ``CarbonCert`` contracts issues by
Alice, one owned by Bob and one owned by Charlie. This Daml script
reuses the ``Setup.setup`` Daml script from the previous section to
create the parties & users.

.. literalinclude:: example/carbon-initiate-upgrade/daml/InitiateUpgrade.daml
   :language: daml
   :start-after: -- SETUP_SCRIPT_BEGIN
   :end-before: -- SETUP_SCRIPT_END

Run the script as follows:

.. code-block:: none

   $ cd example/carbon-initiate-upgrade
   $ daml build
   $ daml script --dar=.daml/dist/carbon-initiate-upgrade-1.0.0.dar --script-name=InitiateUpgrade:setup --ledger-host=localhost --ledger-port=6865 --output-file parties.json

As before, ``parties.json`` contains the actual party ids we can use later.

If you now start Navigator from the ``carbon-initiate-upgrade``
directory and log in as ``alice``, you can see the two ``CarbonCert`` contracts.

Next, we run the trigger for Alice. The trigger will keep running throughout the
rest of this example.

.. code-block:: none

   $ cd example/carbon-upgrade-trigger
   $ daml build
   $ daml trigger --dar=.daml/dist/carbon-upgrade-trigger-1.0.0.dar --trigger-name=UpgradeTrigger:upgradeTrigger --ledger-host=localhost --ledger-port=6865 --ledger-user=alice

With the trigger running, we can now run the script to create the
``UpgradeCarbonCertProposal`` contracts (we could also have done that before
starting the trigger). The script takes an argument of type
``Parties`` corresponding to the result of the previous ``setup`` script.
We can pass this in via the ``--input-file`` argument.

.. code-block:: none

   $ cd example/carbon-initiate-upgrade
   $ daml build
   $ daml script --dar=.daml/dist/carbon-initiate-upgrade-1.0.0.dar --script-name=InitiateUpgrade:initiateUpgrade --ledger-host=localhost --ledger-port=6865 --input-file=parties.json

At this point, our trigger is running and the ``UpgradeCarbonCertProposal``
contracts for Bob and Charlie have been created. What is left to do is
to accept the proposals. Our trigger will then automatically pick them
up and upgrade the ``CarbonCert`` contracts.

First, start Navigator and log in as ``bob``. Click on the
``UpgradeCarbonCertProposal`` and accept it. If you now go back to the
contracts tab, you can see that the ``CarbonCert`` contract has been
archived and instead there is a new ``CarbonCertWithMethod`` upgrade. Our
trigger has successfully upgraded the ``CarbonCert``!

Next, log in as ``charlie`` and accept the ``UpgradeCarbonCertProposal``. Just
like for Bob, you can see that the ``CarbonCert`` contract has been archived
and instead there is a new ``CarbonCertWithMethod`` contract.

Since we upgraded all ``CarbonCert`` contracts issued by Alice, we can now stop the
trigger and declare the update successful.
