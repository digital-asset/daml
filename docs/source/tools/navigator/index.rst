.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Navigator
#########

The Navigator is a front-end that you can use to connect to any DAML Ledger and inspect and modify the ledger. You can use it during DAML development to explore the flow and implications of the DAML models.

The first sections of this guide cover use of the Navigator with the DAML SDK. Refer to :ref:`navigator-manual-advanced-usage` for information on using Navigator outside the context of the SDK.

Navigator functionality
***********************

Connect Navigator to any DAML Ledger and use it to:

- View templates
- View active and archived contracts
- Exercise choices on contracts
- Advance time (This option applies only when using Navigator with the DAML Sandbox ledger.)

Installing and starting Navigator
*********************************

Navigator ships with the DAML SDK. To launch it:

1. Start Navigator via a terminal window running :doc:`SDK Assistant </tools/assistant>` by typing ``daml start``

2. The Navigator web-app is automatically started in your browser. If it fails to start,
   open a browser window and point it to the Navigator URL

  When running ``daml start`` you will see the Navigator URL. By default it will be `<http://localhost:7500/>`_.

.. note:: Navigator is compatible with these browsers: Safari, Chrome, or
   Firefox.

For information on how to launch and use Navigator outside of the SDK, see :ref:`navigator-manual-advanced-usage` below.

Choosing a party / changing the party
*************************************

The ledger is a record of transactions between authorized participants on the distributed network.
Before you can interact with the ledger, you must assume the role of a particular party.
This determines the contracts that you can access and the actions you are permitted to perform on the ledger.
The first step in using Navigator is to use the drop-down list on the Navigator home screen to select from the available
parties.

.. image:: images/choose-party.png
  :width: 30%
  :align: center

.. note:: The party choices are configured on startup. (Refer to
   :doc:`/tools/assistant` or :ref:`navigator-manual-advanced-usage` for more instructions.)

.. TODO: Consider repeating instructions instead of cross-referencing.

The main Navigator screen will be displayed, with contracts that this party is entitled to view in the main pane and
the  option to switch from contracts to templates in the pane at the left. Other options allow you to filter the
display, include or exclude archived contracts, and exercise choices as described below.

.. image:: images/example.png
  :width: 85%
  :align: center

To change the active party:

#. Click the name of the current party in the top right corner of the screen.

#. On the home screen, select a different party.

.. image:: images/sign-out.png
  :width: 30%
  :align: center

You can act as different parties in different
browser windows. Use Chrome's profile feature
https://support.google.com/chrome/answer/2364824 and sign in as
a different party for each Chrome profile.

Logging out
***********

To log out, click the name of the current party in the top-right corner of the screen.

.. COMMENT: Why should I log out?? What if I don't?

Viewing templates or contracts
******************************

DAML *contract ​templates* are ​models ​that contain ​the ​agreement ​statement, ​all ​the ​applicable
parameters, ​and ​the ​choices ​that ​can ​be ​made ​in ​acting ​on ​that ​data.
They ​specify ​acceptable input ​and ​the ​resulting ​output. ​A ​contract ​template ​contains ​placeholders ​rather ​than ​actual names, ​amounts, ​dates, ​and ​so ​on. In ​a *contract ​instance,* ​the ​placeholders ​have ​been ​replaced ​with ​actual ​data.

The Navigator allows you to list templates or contracts, view contracts based on a template, and view template and contract details.

Listing templates
=================

To see what contract templates are available on the ledger you are connected to, choose **Templates** in the left pane of the main Navigator screen.

.. image:: images/templates.png
  :width: 85%
  :align: center

Use the **Filter** field at the top right to select template IDs that include the text you enter.

Listing contracts
=================

To view a list of available contracts, choose **Contracts** in the left pane.

.. image:: images/contracts.png
  :width: 85%
  :align: center

In the Contracts list:

- Changes to the ledger are automatically reflected in the list of contracts. To
  avoid the automatic updates, select the **Frozen** checkbox. Contracts will still be marked as archived, but the contracts list will not change.

.. COMMENT: 2nd sentence above is rather cryptic. Why would I want to avoid automatic updates when all i'm doing is listing contracts - it's not clear?? Is this relevant in the SDK context - is it perhaps an Advanced feature???

- Filter the displayed contracts by entering text in the
  **Filter** field at the top right.

- Use the **Include Archived** checkbox at the top to include or exclude archived contracts.

Viewing contracts based on a template
=====================================

You can also view the list of contracts that are based on a particular template.

#. You will see icons to the right of template IDs in the template list with a number indicating how many contracts are based on this template.

#. Click the number to display a list of contracts based on that template.

**Number of Contracts**

.. image:: images/template-contracts-icon.png
  :width: 85%
  :align: center

**List of Contracts**

.. image:: images/template-contracts.png
  :width: 85%
  :align: center

Viewing template and contract details
=====================================

To view template or contract details, click on a template or contract in the list. The template or contracts detail page is displayed.

**Template Details**

.. image:: images/template-details.png
  :width: 85%
  :align: center

**Contract Details**

.. image:: images/contract-details.png
  :width: 85%
  :align: center

Using Navigator
***************
.. _navigator-manual-creating-contracts:

Creating contracts
==================

Contracts in a ledger are created automatically when you exercise
choices. In some cases, you create a contract directly from a
template. This feature can be particularly useful for testing and experimenting during development.

To create a contract based on a template:

#. Navigate to the template detail page as described above.

#. Complete the values in the form

#. Choose the **Submit** button.

.. image:: images/create-contract.png
  :width: 85%
  :align: center

When the command has been committed to the ledger, the loading indicator in the navbar at the top
will display a tick mark.

While loading...

.. image:: images/command-loading-new.png
  :width: 50%
  :align: center

When committed to the ledger...

.. image:: images/command-confirmed-new.png
  :width: 50%
  :align: center

Exercising choices
==================

To exercise a choice:

1. Navigate to the contract details page (see above).

2. Click the choice you want to exercise in the choice list.

3. Complete the form.

4. Choose the **Submit** button.

.. image:: images/choice-exercise.png
  :width: 85%
  :align: center

Or


1. Navigate to the choice form by clicking the wrench icon in a contract list.

2.  Select a choice.

.. image:: images/choice-select.png
  :width: 20%
  :align: center

You will see the loading and confirmation indicators, as pictured above in Creating Contracts.

Advancing time
==============

It is possible to advance time against the DAML Sandbox. (This is not true of all DAML Ledgers.) This advance-time functionality can be useful when testing, for example, when entering a trade on one date and settling it on a later date.

To advance time:

1. Click on the ledger time indicator in the navbar at the top of the screen.

2. Select a new date / time.

3. Choose the **Set** button.

.. image:: images/advance-time.png
  :width: 25%
  :align: center

.. _navigator-authentication:

Authenticating Navigator
************************

If you are running Navigator against a Ledger API server that requires authentication, you must provide the access token when you start the Navigator server.

The access token retrieval depends on the specific DAML setup you are working with: please refer to the ledger operator to learn how.

Once you have retrieved your access token, you can provide it to Navigator by storing it in a file and provide the path to it using the ``--access-token-file`` command line option.

If the access token cannot be retrieved, is missing or wrong, you'll be unable to move past the Navigator's frontend login screen and see the following:

.. image:: images/access-denied.png
  :width: 50%
  :align: center

.. _navigator-manual-advanced-usage:

Advanced usage
**************

.. _navigator-custom-views:

Customizable table views
========================

Customizable table views is an advanced rapid-prototyping feature,
intended for DAML developers who wish to customize the Navigator UI without
developing a custom application.

.. COMMENT: Suggest changing para below to procedure format.

To use customized table views:

1. Create a file ``frontend-config.js`` in your project root folder (or the folder from which you run Navigator) with the content below::

    import { DamlLfValue } from '@da/ui-core';

    export const version = {
      schema: 'navigator-config',
      major: 2,
      minor: 0,
    };

    export const customViews = (userId, party, role) => ({
      customview1: {
        type: "table-view",
        title: "Filtered contracts",
        source: {
          type: "contracts",
          filter: [
            {
              field: "id",
              value: "1",
            }
          ],
          search: "",
          sort: [
            {
              field: "id",
              direction: "ASCENDING"
            }
          ]
        },
        columns: [
          {
            key: "id",
            title: "Contract ID",
            createCell: ({rowData}) => ({
              type: "text",
              value: rowData.id
            }),
            sortable: true,
            width: 80,
            weight: 0,
            alignment: "left"
          },
          {
            key: "template.id",
            title: "Template ID",
            createCell: ({rowData}) => ({
              type: "text",
              value: rowData.template.id
            }),
            sortable: true,
            width: 200,
            weight: 3,
            alignment: "left"
          }
        ]
      }
    })

2. Reload your Navigator browser tab. You should now see a sidebar item titled "Filtered contracts" that links to a table with contracts filtered and sorted by ID.

To debug config file errors and learn more about the
config file API, open the Navigator ``/config`` page in your browser
(e.g., `<http://localhost:7500/config>`_).

Using Navigator outside the SDK
===============================

This section explains how to work with the Navigator if you have a project created outside of the normal SDK workflow and want to use the Navigator to inspect the ledger and interact with it.

.. note:: If you are using the Navigator as part of the DAML SDK, you do not need to read this section.

The Navigator is released as a "fat" Java `.jar` file that bundles all required
dependencies. This JAR is part of the SDK release and can be found using the
SDK Assistant's ``path`` command::

  da path navigator

Use the ``run`` command to launch the Navigator JAR and print usage instructions::

  da run navigator

Arguments may be given at the end of a command, following a double dash. For example::

  da run navigator -- server \
    --config-file my-config.conf \
    --port 8000 \
    localhost 6865

The Navigator requires a configuration file specifying each user and the party
they act as. It has a ``.conf`` ending by convention. The file follows this
form::

  users {
      <USERNAME> {
          party = <PARTYNAME>
      }
      ..
  }

In many cases, a simple one-to-one correspondence between users and their
respective parties is sufficient to configure the Navigator. Example::

  users {
      BANK1 { party = "BANK1" }
      BANK2 { party = "BANK2" }
      OPERATOR { party = "OPERATOR" }
  }

Using Navigator with a DAML Ledger
==================================

By default, Navigator is configured to use an unencrypted connection to the ledger.
To run Navigator against a secured DAML Ledger,
configure TLS certificates using the ``--pem``, ``--crt``, and ``--cacrt`` command line parameters.
Details of these parameters are explained in the command line help::

  daml navigator --help
