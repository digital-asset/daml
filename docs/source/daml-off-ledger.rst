.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Write Off-Ledger Automation Using Daml
======================================

The Daml smart contract language is mostly meant to provide a way to define on-ledger logic, i.e.
code that defines how a transaction happens on ledger. The templates and choices defined with Daml
are available to be used by off-ledger logic that interacts with the Ledger API. Usually this
off-ledger logic is wrote in general purpose language like Java or JavaScript and the codegen
allows to interact with Daml types with minimal boilerplate.

However, there are cases in which it would be nice to write your off-ledger logic in Daml as well.
While Daml is not meant to be used as a general purpose language that can interact with your
file system or network, for relatively simple automations that don't require full access to your
system's capabilities using Daml means that you don't have to map from your on-ledger Daml types
and their representation on a separate off-ledger general purpose language (either through the
codegen or by manipulating the Protobuf representation of Daml types directly).

There are two tools that allow to use Daml as an off-ledger language:

- :doc:`Daml Script</daml-script/index>` allows to write automations that can be triggered
  by any off-ledger condition, such as the availability of a file in a folder, a message
  coming from a broker or simply a user interacting with the system directly.

- :doc:`Daml Triggers</triggers/index>` allow a similar approach but by having automation
  triggered by on-ledger events, such as the creation of a contract.

In the way in which they interact with a traditional database system, Daml Scripts and Daml
Triggers are analogous to SQL scripts and SQL triggers.

