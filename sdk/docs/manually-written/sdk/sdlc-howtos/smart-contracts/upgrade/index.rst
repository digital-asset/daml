.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _upgrades-index:

Upgrading and Extending Daml Applications
#########################################

.. .. toctree::
   :hidden:

   extend
   upgrade
   smart-contract-upgrades
   automation

Database schemas tend to evolve over time. A new feature in your application
might need an additional choice in one of your templates. Or a change in your
data model will make you application perform better. We distinguish two kinds of
changes to a Daml model:

  - A Daml model extension
  - A Daml model upgrade

An *extension* adds new templates and data structures to your model, while
leaving all previously written definitions unchanged.

An *upgrade* changes previously defined data structures and templates.

Whether extension or upgrade, your new code needs to be compatible with data
that is already live in a production system. In the following sections, we'll
show you how to extend your Daml model, and two ways to upgrade your Daml models,
depending on your data transformation and downtime requirements.
