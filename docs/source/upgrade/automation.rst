.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _upgrade-automation:

Automating the Upgrade Process
##############################

In this section, we are going to automate the upgrade of our coin
process using :doc:`DAML Script</daml-script/index>` and :doc:`DAML
Triggers </triggers/index>`. Note that automation for upgrades is
specific to an individual application, just like the upgrade models.
Nevertheless, we have found that the pattern shown here
occurs frequently.

Structuring the Upgrade
=======================

There are three kinds of actions performed during the upgrade:

#. Alice creates ``UpgradeCoinProposal`` contracts. We assume here,
   that Alice wants to upgrade all ``Coin`` contracts she has
   issued. Since the ``UpgradeCoinProposal`` proposal is specific to
   each owner, Alice has to create one ``UpgradeCoinProposal`` per
   owner. There can be potentially many owners but this step only has
   to be performed once assuming Alice will not issue more ``Coin``
   contracts after this point.
#. Bob and other owners accept the ``UpgradeCoinProposal``. To keep
   this example simple, we assume that there are only coins issued by
   Alice. Therefore, each owner has to accept at most one proposal.
#. As owners accept upgrade proposals, Alice has to upgrade each
   coin. This means that she has to execute the upgrade choice once
   for each coin. Owners will not all accept the upgrade at the same
   time and some might never accept it. Therefore, this should be a
   long-running process that upgrades all coins of a given owner as
   soon as they accept the upgrade.

Given those constraints, we are going to use the following tools for
the upgrade:

#. A DAML script that will be executed once by Alice and creates an
   ``UpgradeCoinProposal`` contract for each owner.
#. Navigator to accept the ``UpgradeCoinProposal`` as Bob. While we
   could also use a DAML script to accept the proposal, this step will
   often be exposed as part of a web UI so doing it interactively in
   Navigator resembles that workflow more closely.
#. A long-running DAML trigger that upgrades all ``Coin`` contracts
   for which there is a corresponding ``UpgradeCoinAgreement``.

Implementation of the DAML Script
=================================

In our DAML Script, we are first going to query the ACS (Active Contract Set) to find all
``Coin`` contracts issued by us. Next, we are going to extract the
owner of each of those contracts and remove any duplicates coming from
multiple coins issued to the same owner. Finally, we iterate over the
owners and create an ``UpgradeCoinAgreement`` contract for each owner.

.. literalinclude:: example/coin-initiate-upgrade/daml/InitiateUpgrade.daml
   :language: daml
   :start-after: -- INITIATE_UPGRADE_BEGIN
   :end-before: -- INITIATE_UPGRADE_END

Implementation of the DAML Trigger
==================================

Our trigger does not need any custom user state and no heartbeat so
the only interesting field in its definition is the rule.

.. literalinclude:: example/coin-upgrade-trigger/daml/UpgradeTrigger.daml
   :language: daml
   :start-after: -- TRIGGER_BOILERPLATE_BEGIN
   :end-before: -- TRIGGER_BOILERPLATE_END

In our rule, we first filter out all agreements and coins issued by
us. Next, we iterate over all agreements. For each agreement we filter
the coins by the owner of the agreement and finally upgrade the coin
by exercising the ``Upgrade`` choice. We mark the coin as pending
which temporarily removes it from the ACS and therefore stops the
trigger from trying to upgrade the same coin multiple times if the
rule is triggered in quick succession.

.. literalinclude:: example/coin-upgrade-trigger/daml/UpgradeTrigger.daml
   :language: daml
   :start-after: -- TRIGGER_RULE_BEGIN
   :end-before: -- TRIGGER_RULE_END

The trigger is a long-running process and the rule will be executed
whenever the state of the ledger changes. So whenever an owner accepts
an upgrade proposal, the trigger will run the rule and upgrade all
coins of that owner.

Deploying and Executing the Upgrade
===================================

Now that we defined our DAML script and our trigger, it is time to use
them! If you still have Sandbox running from the previous section,
stop it to clear out all data before continuing.

First, we start sandbox passing in the ``coin-upgrade`` DAR. Since a
DAR includes all transitive dependencies, this includes ``coin-1.0.0``
and ``coin-2.0.0``.

.. code-block:: none

   $ cd example/coin-upgrade
   $ daml sandbox .daml/dist/coin-upgrade-1.0.0.dar

To simplify the setup here, we use a DAML script to create 3 parties
Alice, Bob and Charlie and two ``Coin`` contracts issues by Alice, one
owned by Bob and one owned by Charlie.

.. literalinclude:: example/coin-initiate-upgrade/daml/InitiateUpgrade.daml
   :language: daml
   :start-after: -- SETUP_SCRIPT_BEGIN
   :end-before: -- SETUP_SCRIPT_END

Run the script as follows:

.. code-block:: none

   $ cd example/coin-initiate-upgrade
   $ daml build
   $ daml script --dar=.daml/dist/coin-initiate-upgrade-1.0.0.dar --script-name=InitiateUpgrade:setup --ledger-host=localhost --ledger-port=6865 --wall-clock-time

If you now start Navigator from the ``coin-initiate-upgrade``
directory and log in as Alice, you can see the two ``Coin`` contracts.

Next, we run the trigger for Alice. The trigger will keep running throughout the
rest of this example.

.. code-block:: none

   $ cd example/coin-upgrade-trigger
   $ daml build
   $ daml trigger --dar=.daml/dist/coin-upgrade-trigger-1.0.0.dar --trigger-name=UpgradeTrigger:upgradeTrigger --ledger-host=localhost --ledger-port=6865 --ledger-party=Alice --wall-clock-time

With the trigger running, we can now run the script to create the
``UpgradeCoinProposal`` contracts (we could also have done that before
starting the trigger). The script takes an argument of type
``Party``. We can pass this in via the ``--input-file`` argument which
we will point to a file ``party.json`` containing ``"Alice"``. This allows us to
change the party without having to change the code of the script.

.. code-block:: none

   $ cd example/coin-initiate-upgrade
   $ daml build
   $ daml script --dar=.daml/dist/coin-initiate-upgrade-1.0.0.dar --script-name=InitiateUpgrade:initiateUpgrade --ledger-host=localhost --ledger-port=6865 --wall-clock-time --input-file=party.json

At this point, our trigger is running and the ``UpgradeCoinProposal``
contracts for Bob and Charlie have been created. What is left to do is
to accept the proposals. Our trigger will then automatically pick them
up and upgrade the ``Coin`` contracts.

First, start Navigator and log in as Bob. Click on the
``UpgradeCoinProposal`` and accept it. If you now go back to the
contracts tab, you can see that the ``Coin`` contract has been
archived and instead there is a new ``CoinWithAmount`` upgrade. Our
trigger has successfully upgraded the ``Coin``!

Next, log in as Charlie and accept the ``UpgradeCoinProposal``. Just
like for Bob, you can see that the ``Coin`` contract has been archived
and instead there is a new ``CoinWithAmount`` contract.

Since we upgraded all ``Coin`` contracts issued by Alice, we can now stop the
trigger and declare the update successful.
