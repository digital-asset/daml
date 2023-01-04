.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Upgrading and Extending Daml Applications
#########################################

.. toctree::
   :hidden:

   extend
   upgrade
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
that is already live in a production system. The next two sections show how to
extend and upgrade Daml models. The last section shows how to automate the data
migration process.
