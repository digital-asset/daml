.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Navigator Console
#################

The Navigator Console is a terminal-based front-end for inspecting and modifying a Digital Asset ledger. It's useful for DAML developers, app developers, or business analysts who want to debug or analyse a ledger by exploring it manually.

You can use the Console to:

- inspect available templates
- query active contracts
- exercise commands
- list blocks and transactions

If you prefer to use a graphical user interface for these tasks, use the :doc:`Navigator </tools/navigator/index>` instead.

.. contents:: On this page:
  :local:

Try out the Navigator Console on the Quickstart
===============================================

With the sandbox running the :doc:`quickstart application </getting-started/quickstart>`

#. To start the shell, run ``da run navigator -- console localhost 6865``

   This connects Navigator Console to the sandbox, which is still running.

   You should see a prompt like this:

   .. code-block:: none

        _  __          _           __
       / |/ /__ __  __(_)__ ____ _/ /____  ____
      /    / _ `/ |/ / / _ `/ _ `/ __/ _ \/ __/
     /_/|_/\_,_/|___/_/\_, /\_,_/\__/\___/_/
                      /___/
     Version 1.1.0
     Welcome to the console. Type 'help' to see a list of commands.

#. Since you are connected to the sandbox, you can be any party you like. Switch to Bob by running:

   ``party Bob``

   The prompt should change to ``Bob>``.
#. Issue a *BobsCoin* to yourself. Start by writing the following, then hit Tab to auto-complete and get the full name of the `Iou.Iou` template:

   ``create Iou.Iou <TAB>``

   This full name includes a hash of the DAML package, so don't copy it from the command below - it's better to get it from the auto-complete feature.

   You can then create the contract by running:
   
   ``create Iou.Iou@317057d06d4bc4bb91bf3cfe3292bf3c2467c5e004290e0ba20b993eb1e40931 with {issuer="Bob", owner="Bob", currency="BobsCoin", amount="1.0", observers=[]}``

   You should see the following output:

   .. code-block:: none

     CommandId: 1b8af77a91ad1102
     Status: Success
     TransactionId: 10

#. You can see details of that contract using the TransactionId. First, run:

   ``transaction 10``

   to get details of the transaction that created the contract:

   .. code-block:: none

     Offset: 11
     Effective at: 1970-01-01T00:00:00Z
     Command ID: 1b8af77a91ad1102
     Events:
     - [#10:0] Created #10:0 as Iou

   Then, run:

   ``contract #10:0``

   to see the contract for the new BobsCoin:

   .. code-block:: none

      Id: #10:0
      TemplateId: Iou.Iou@317057d06d4bc4bb91bf3cfe3292bf3c2467c5e004290e0ba20b993eb1e40931
      Argument:
        observers:
        
        issuer: Bob
        amount: 1.0
        currency: BobsCoin
        owner: Bob
      Created:
        EventId: #10:0
        TransactionId: 10
        WorkflowId: 1ba8521c395096e3
      Archived: Contract is active

#. You can transfer the coin to Alice by running:

   ``exercise #10:0 Iou_Transfer with {newOwner="Alice"}``

There are lots of other things you can do with the Navigator Console.

- One of its most powerful features is that you can query its local databases using SQL, with the ``sql`` command.

  For example, you could see all of the `Iou` contracts by running ``sql select * from contract where template_id like 'Iou.Iou@%'``. For more examples, take a look at the :doc:`Navigator Console database documentation </tools/navigator/database>`.
- For a full list of commands, run ``help``. You can also look at the :doc:`Navigator Console documentation page </tools/navigator/console>`.
- For help on a particular command, run ``help <name of command>``.
- When you are done exploring the shell, run ``quit`` to exit.


Installing and starting Navigator Console
*****************************************

Navigator Console is installed as part of the DAML SDK. See :doc:`/getting-started/installation` for instructions on how to install the DAML SDK.

If you want to use Navigator Console independent of the SDK, see the :ref:`navigator-console-advanced-usage` section.

To run Navigator Console:

1. Open a terminal window and navigate to your DAML SDK project folder.

2. If the Sandbox isn't already running, run it with the command ``da start``.

   The sandbox prints out the port on which it is running - by default, port ``6865``.

3. Run ``da run navigator -- console localhost 6865``.
   Replace ``6865`` by the port reported by the sandbox, if necessary.

When Navigator Console starts, it displays a welcome message::

       _  __          _           __
      / |/ /__ __  __(_)__ ____ _/ /____  ____
     /    / _ `/ |/ / / _ `/ _ `/ __/ _ \/ __/
    /_/|_/\_,_/|___/_/\_, /\_,_/\__/\___/_/
                     /___/
    Version X.Y.Z
    Welcome to the console. Type 'help' to see a list of commands.

Getting help
************

To see all available Navigator Console commands and how to use them, use the ``help`` command::

    >help 
    Available commands:
    choice               Print choice details
    command              Print command details
    commands             List submitted commands
    contract             Print contract details
    create               Create a contract
    diff_contracts       Print diff of two contracts
    event                Print event details
    exercise             Exercises a choice
    help                 Print help
    graphql              Execute a GraphQL query
    graphql_examples     Print some example GraphQL queries
    graphql_schema       Print the GraphQL schema
    info                 Print debug information
    package              Print DAML-LF package details
    packages             List all DAML-LF packages
    parties              List all parties available in Navigator
    party                Set the current party
    quit                 Quit the application
    set_time             Set the (static) ledger effective time
    templates            List all templates
    template             Print template details
    time                 Print the ledger effective time
    transaction          Print transaction details
    version              Print application version
    sql_schema           Return the database schema
    sql                  Execute a SQL query

To see the help for the given command, run ``help <command>``::

    >help create 
    Usage: create <template> with <argument>

    Create a contract
    Parameters:
    <template>           Template ID
    <argument>           Contract argument

Exiting Navigator Console
*************************

To exit Navigator Console, use the ``quit`` command::

    >quit
    Bye.

Using commands
**************

This section describes how to use some common commands.

.. note::
    Navigator Console has several features to help with typing commands:

    * Press the **Tab** key one or more times to use auto-complete and see a list of suggested text to complete the command.
    * Press the **Up** or **Down** key to scroll through the history of recently used commands.
    * Press **Ctrl+R** to search in the history of recently used commands.

Displaying status information
=============================

To see useful information about the status of both Navigator Console and the ledger, use the ``info`` command::

    >info 
       _  __          _           __
      / |/ /__ __  __(_)__ ____ _/ /____  ____
     /    / _ `/ |/ / / _ `/ _ `/ __/ _ \/ __/
    /_/|_/\_,_/|___/_/\_, /\_,_/\__/\___/_/
                     /___/
    Version 1.0.14 commit a3e1d1c30d84261fa9b6db95c69036da14bc9e7e
    General info:
        Ledger host: localhost
        Ledger port: 6865
        Secure connection: false
        Application ID: Navigator-c06fae89-d8ed-4656-b085-388e24569ecf #5b21103194967935
    Ledger info:
        Connection status: Connected
        Ledger ID: sandbox-051e2468-c679-43df-b99f-9c72dcd8ffa0
        Ledger time: 1970-01-01T00:16:40Z
        Ledger time type: static
    Akka system:
        OPERATOR: Actor running
        BANK2: Actor running
        BANK1: Actor running
    Local data:
        BANK1:
            Packages: 1
            Contracts: 0
            Active contracts: 0
            Last transaction: ???
        BANK2:
            Packages: 1
            Contracts: 0
            Active contracts: 0
            Last transaction: ???
        OPERATOR:
            Packages: 1
            Contracts: 1001
            Active contracts: 1001
            Last transaction: scenario-transaction-2002

Choosing a party
================

Privacy is an important aspect of a Digital Asset ledger: parties can only access the contracts on the ledger that they are authorized to. This means that, before you can interact with the ledger, you must assume the role of a particular party.

The currently active party is displayed left of the prompt sign (``>``).
To assume the role of a different party, use the ``party`` command::

    BANK1>party BANK2
    BANK2>

.. note:: The list of available parties is configured when the Sandbox starts.
   (See the :doc:`/tools/assistant` or :ref:`navigator-console-advanced-usage` for more instructions.)

Advancing time
==============

You can advance the time of the DAML Sandbox. This can be useful when testing, for example, when entering a trade on one date and settling it on a later date.

(For obvious reasons, this feature does not exist on the Digital Asset ledger.)

To display the current ledger time, use the ``time`` command::

    >time
    1970-01-01T00:16:40Z

To advance the time to the time you specify, use the ``set_time`` command::

    >set_time 1970-01-02T00:16:40Z
    New ledger effective time: 1970-01-02T00:16:40Z

Inspecting templates
====================

To see what templates are available on the ledger you are connected to, use the ``templates`` command::

    >templates
    ╔════════════════════════╤════════╤═══════╗
    ║Name                    │Package │Choices║
    ╠════════════════════════╪════════╪═══════╣
    ║Main.RightOfUseAgreement│07ca8611│0      ║
    ║Main.RightOfUseOffer    │07ca8611│1      ║
    ╚════════════════════════╧════════╧═══════╝

To get detailed information about a particular template, use the ``template`` command::

    >template Offer<Tab>
    >template Main.RightOfUseOffer@07ca8611d05ec14ea4b973192ef6caa5d53323bba50720a8d7142c2a246cfb73
    Name: Main.RightOfUseOffer
    Parameter:
        landlord: Party
        tenant: Party
        address: Text
        expirationDate: Time
    Choices:
    - Accept

.. note:: 
    Remember to use the **Tab** key. In the above example, typing "Offer" followed by the **Tab** key auto-completes the fully qualified name of the "RightOfUseOffer" template.

To get detailed information about a choice defined by a template, use the ``choice`` command::

    >choice Main.RightOfUseOffer Accept
    Name: Accept
    Consuming: true
    Parameter: Unit

Inspecting contracts, transactions, and events
==============================================

The ledger is a record of transactions between authorized participants on the distributed network. Transactions consist of events that create or archive contracts, or exercise choices on them.

To get detailed information about a ledger object, use the singular form of the command (``transaction``, ``event``, ``contract``)::

    >transaction 2003
    Offset: 1004
    Effective at: 1970-01-01T00:16:40Z
    Command ID: 732f6ac4a63c9802
    Events:
    - [#2003:0] Created #2003:0 as RightOfUseOffer

::

    >event #2003:0
    Id: #2003:0
    ParentId: ???
    TransactionId: 2003
    WorkflowId: e13067beec13cf4c
    Witnesses:
    - Scrooge_McDuck
    Type: Created
    Contract: #2003:0
    Template: Main.RightOfUseOffer
    Argument:
        landlord: Scrooge_McDuck
        tenant: Bentina_Beakley
        address: McDuck Manor, Duckburg
        expirationDate: 2020-01-01T00:00:00Z

::

    >contract #2003:0
    Id: #2003:0
    TemplateId: Main.RightOfUseOffer
    Argument:
        landlord: Scrooge_McDuck
        tenant: Bentina_Beakley
        address: McDuck Manor, Duckburg
        expirationDate: 2020-01-01T00:00:00Z
    Created:
        EventId: #2003:0
        TransactionId: 2003
        WorkflowId: e13067beec13cf4c
    Archived: Contract is active
    Exercise events:

Querying data
=============

To query contracts, transactions, events, or commands in any way you'd like, you can query the Navigator Console's local database(s) directly.

Because of the strong DAML privacy model, each party will see a different subset of the ledger data. For this reason, each party has its own local database.

To execute a SQL query against the local database for the currently active party, use the ``sql`` command::

    >sql select id, template_id, archive_transaction_id from contract
    ╔═══════╤════════════════════╤══════════════════════╗
    ║id     │template_id         │archive_transaction_id║
    ╠═══════╪════════════════════╪══════════════════════╣
    ║#2003:0│Main.RightOfUseOffer│null                  ║
    ║#2004:0│Main.RightOfUseOffer│null                  ║
    ╚═══════╧════════════════════╧══════════════════════╝

See the :doc:`Navigator Local Database <database>` documentation for details on the database schema and how to write SQL queries.

.. note:: 
    The local database contains a copy of the ledger data, created using the Ledger API. If you modify the local database, you might break Navigator Console, but it will not affect the data on the ledger in any way.

Creating contracts
==================

Contracts in a ledger can be created directly from a template, or when you exercise a choice. You can do both of these things using Navigator Console.

To create a contract of a given template, use the ``create`` command.
The contract argument is written in JSON format (DAML primitives are strings, DAML records are objects, DAML lists are arrays)::

    >create Main.RightOfUseOffer@07ca8611d05ec14ea4b973192ef6caa5d53323bba50720a8d7142c2a246cfb73 with {"landlord": "BANK1", "tenant": "BANK2", "address": "Example Street", "expirationDate": "2018-01-01T00:00:00Z"}
    CommandId: 1e4c1610eadba6b
    Status: Success
    TransactionId: 2005

.. note:: 
    Again, you can use the **Tab** key to auto-complete the template name.

The Console waits briefly for the completion of the create command and prints basic information about its status.
To get detailed information about your create command, use the ``command`` command::

    >command 1e4c1610eadba6b
    Command:
        Id: 1e4c1610eadba6b
        WorkflowId: a31ea1ca20cd5971
        PlatformTime: 1970-01-02T00:16:40Z
        Command: Create contract
        Template: Main.RightOfUseOffer
        Argument:
            landlord: Scrooge_McDuck
            tenant: Bentina_Beakley
            address: McDuck Manor, Duckburg
            expirationDate: 2020-01-01T00:00:00Z
    Status:
        Status: Success
        TransactionId: 2005

Exercising choices
==================

To exercise a choice on a contract with the given ID, use the ``exercise`` command::

    >exercise #2005:0 Accept
    CommandID: 8dbbcbc917c7beee
    Status: Success
    TransactionId: 2006

::

    >exercise #2005:0 Accept with {tenant="BANK2"}
    CommandID: 8dbbcbc917c7beee
    Status: Success
    TransactionId: 2006

.. _navigator-console-advanced-usage:

Advanced usage
**************

Using Navigator outside the SDK
===============================

This section explains how to work with the Navigator if you have a project created outside of the normal SDK workflow and want to use the Navigator to inspect the ledger and interact with it.

.. note:: If you are using the Navigator as part of the DAML SDK, you do not need to read this section.

The Navigator is released as a "fat" Java `.jar` file that bundles all required dependencies. This JAR is part of the SDK release and can be found using the SDK Assistant's ``path`` command::

  da path navigator

To launch the Navigator JAR and print usage instructions::

  da run navigator

Provide arguments at the end of a command, following a double dash. For example::

  da run navigator -- console \
    --config-file my-config.conf \
    --port 8000 \
    localhost 6865

The Navigator needs a configuration file specifying each user and the party they act as. It has a ``.conf`` ending by convention. The file has this format::

  users {
      <USERNAME> {
          party = <PARTYNAME>
      }
      ..
  }

In many cases, a simple one-to-one correspondence between users and their respective parties is sufficient to configure the Navigator. Example::

  users {
      BANK1 { party = "BANK1" }
      BANK2 { party = "BANK2" }
      OPERATOR { party = "OPERATOR" }
  }

Using Navigator with the Digital Asset ledger
=============================================

By default, Navigator is configured to use an unencrypted connection to the ledger.

To run Navigator against a secured Digital Asset Ledger, configure TLS certificates using the ``--pem``, ``--crt``, and ``--cacrt`` command line parameters.

Details of these parameters are explained in the command line help::

  da run navigator -- --help
