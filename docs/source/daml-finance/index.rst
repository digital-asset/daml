.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml Finance
############

.. toctree::
   :hidden:

   architecture
   core-concepts
   glossary
   getting-started/getting-started
   getting-started/settlement
   getting-started/lifecycling
   tutorial/bond-extension
   tutorial/derivative-extension
   tutorial/contingent-claims-instrument

Daml Finance is currently an :doc:`Early Access Feature in Alpha status </support/status-definitions>`.

Introduction
************

Daml Finance is a library that supports customers modeling financial use-cases in Daml. It
comes with Daml packages that cover the following areas:

-  *Instruments*: modeling a set of financial instruments and their
   economic terms (that is, their rights and obligations)
-  *Holdings*: modeling relationships of ownership in a financial
   instrument (that is, clearly define who owns the rights, who has the
   obligations and the amount/quantity)
-  *Settlement*: atomic settlement of transactions involving holdings
-  *Lifecycling*: evolution of financial instruments over their lifetime
   based on their economic terms

The main goal of Daml Finance is to decrease time-to-market, and to increase productivity of development teams. Without it, every single customer would have to “reinvent the wheel” to produce the same models required in most financial use cases.

Example use-cases
*****************

- *Synchronized Derivatives Lifecycling*: Atomic, intermediated lifecycling and settlement of cash flows across investors and custodians
- *Cross-entity Structured Products Issuance*: Atomic, Multi-party issuance across investors, issuer, risk book, treasury
- *Asset-agnostic Trading Facility*: Generic DvP instructing + immediate, guaranteed settlement
- *CBDC Sandbox*: RLN-style cash settlement of CBDC and commercial bank / private money
- *Non-fungible tokens*: Issuance and distribution of NFTs

Next steps
**********

The :doc:`Architecture <architecture>` page gives you an overview of the available packages and their API.

The :doc:`Core Concepts <core-concepts>` page defines the fundamental templates and interfaces provided by the library.

The :doc:`Getting Started <getting-started/getting-started>` tutorial introduces the basic functionalities of the library through a practical example.
