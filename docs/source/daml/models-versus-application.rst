.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

What functionality belongs in DAML models versus application code?
##################################################################

The answer to this question depends on how you're using your ledger and what is important to you. Consider two different use cases: a ledger encoding legal rights and obligations between companies versus using a ledger as a conduit for internal data synchronization. Each of these solutions would be deployed in very different environments and are on either end of the trust and coordination spectrums. Internally to a company, trust is high and the ability to coordinate change is high. External to a company, the opposite is true. 

The rest of this page will talk about how to organize things in either case. For your particular solution, it is important to similarly identify the what factors are important to you, then separate along those lines.

.. contents:: :local:

Looking at the ledger from a legal perspective
**********************************************

When the ledger is encoding legal rights and obligations between external counterparties, a defensive/minimalistic approach to functionality in DAML models may be prudent. The reasons for this are:

* It is a litigious environment where the ledger’s state may require examination in court
* The ledger is a valuable source of legal information and shouldn’t be contaminated with non-business oriented logic
* The more functionality in shared models, the more which needs to be agreed upon upfront by all companies involved. Further updating shared models is hard since all companies need to coordinate

As a result, shared functionality in DAML models needs careful scrutinization. This minimalistic approach might only include:

* Contracts representing, and going into the servicing of, traditional legal contracts
* Contracts narrowly associated with the business process such as obligations for payment/delivery
* Contractual eligibility checks prior to obligation creation - e.g. prerequisites for creating an insurance claim
* Operations requiring atomicity such as swapping of ownership
* Calculations resulting in legal obligations such as the payout of a call option

Functionality not going into the DAML models then must go into the application. These non-business oriented items may include:

* Commonly available libraries like calendars or date calculations
* Code to parse messages - e.g. FIX trade confirmation messages
* Code to orchestrate a batch calculation
* Calculations specific to a participant

Looking at the ledger from a data synchronization perspective
*************************************************************

On the other hand, when doing data synchronization most of the inter-process communication between parties belongs on the ledger. This perspective is grounded in the fact that DA’s platform acts as a messaging bus where the messages are subject to certain guarantees:

* The initiating party is authentic
* Messages conform to DAML model specification
* Messages are approved by all participants hosting stakeholders of the message 

Therefore, when doing data synchronization all of the above functionality is eligible to go into the DAML models and have the application be a lightweight router. However, there are still some things for which it isn’t sensible to put on the ledger. For examples of these, see the section on :doc:`/daml/anti-patterns`. 

