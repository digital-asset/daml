.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Write Off-Ledger Automation Using Daml
======================================

The Daml smart contract language is mostly meant to provide a way to define on-ledger logic, i.e.
code that defines how a transaction happens on ledger. Daml is not meant to be used as a general
purpose language that can interact with your file system or network; instead, the templates and
choices defined with Daml are available to be used by off-ledger logic that interacts with the
ledger API. Usually this off-ledger logic is written in a general-purpose language like Java or
JavaScript and the codegen allows to interact with models defined in Daml without boilerplate.

However, there are times when it would be nice to write your off-ledger logic in Daml. For
relatively simple automations that don't require full access to your system's capabilities,
using Daml means that you don't have to map from your on-ledger Daml types and their
representation on a separate off-ledger general purpose language (either through the codegen
or by manipulating the Protobuf representation of Daml types directly).

There are two tools that allow you to use Daml as an off-ledger language:

- :doc:`Daml Script</daml-script/index>` allows you to write automations that can be triggered
  by any off-ledger condition, such as the availability of a file in a folder, a message
  coming from a broker or a user interacting with the system directly.

- :doc:`Daml Triggers</triggers/index>` allow a similar approach but
  triggered by on-ledger events, such as the creation of a contract.

In their interactions with a traditional database system Daml Scripts and Daml
Triggers are analogous to SQL scripts and SQL triggers.

