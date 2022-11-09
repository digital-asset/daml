.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


An Introduction To Multi-Party Applications and Daml
####################################################

Multi-party applications, and multi-party application platforms like Daml, solve problems that were nearly impossible to solve with the technologies and architectures that came before. Successfully building multi-party applications requires learning a few new concepts, including architectural principles and patterns. This document explains:
 - why multi-party applications matter 
 - what a multi-party application is 
 - important concepts in multi-party applications
 - key architectural concepts in Daml
 - a transfer example using Daml

Why Do Multi-Party Applications Matter?
***************************************

Have you ever wondered why bank transfers, stock purchases or healthcare claims take days to process? Given our technological advances, including the speed of networks, you might expect these transactions to take less than a second to complete. An inefficient protocol like email takes only a few seconds to send and receive, while these important business workflows take days or weeks. 

What delays these transactions? The processes in question all involve multiple organizations that each keep their own records, resulting in data silos. The processes to ensure consistency between those data silos are complex and slow. When inconsistencies arise, the correction processes (sometimes referred to as reconciliation) are expensive, time-consuming and often require human intervention to determine why two parties have differing views of the result of a business interaction. There are a myriad of reasons for these discrepancies, including differences in data models and error handling logic, inconsistent business process implementations and system faults.

Here’s a deeper look at the problem via the example of a transfer of $100 from Alice’s account in Bank A to Bob’s account in Bank B. (Money is an easily understood example of an asset transferred between parties. The same problems occur in other industries with other assets, for example, healthcare claims, invoices or orders.) Money cannot simply appear or disappear during a transfer. The banks need to ensure that at some point in time, T_0, $100 are in Alice’s account, and at the next point in time, T_1, those $100 are gone from Alice’s account and present in Bob’s account – but at no point are the $100 present in both accounts or in neither account. 

In legacy systems, each bank keeps track of cash holdings independently of the other banks. Each bank stores data in its own private database. Each bank performs its own processes to validate, secure, modify and regulate the workflows that transfer money. The coordination between multiple banks is highly complex. The banks have an obligation to limit their counterparty risk - the probability that the other party in the transaction may not fulfill its part of the deal and may default on the contractual obligations.

Today’s common, albeit highly inefficient and costly, solution for a bank account transfer involves the following steps:
#. Bank A sends a message to Bank B via a messaging standard and provider like SWIFT or SEPA.
#. Bank A and Bank B determine a settlement plan, possibly including several intermediaries. Gaining an agreement on the settlement plan is time-consuming and often includes additional fees. 
#. The settlement process entails (i) debiting $100 from Alice’s account at Bank A, (ii) crediting the commercial account at Bank B, and (iii) once Bank B has the money, crediting Bob’s account at Bank B.

In order to make this process atomic (that is, to make it take place between a point T_0 and a point T_1) banks discretize time into business days. On day T_0 the instruction is made and a settlement plan is created. Outside of business hours between day T_0 and day T_1, the plan is executed through end of day netting and settlement processes. In a sense, banks agree to stop time outside of business hours.

If intermediaries are involved, the process is more complex. Cross-border payments or currency conversion add yet more complexity. The resulting process is costly and takes days. During this multi-day process the $100 is locked within the system where it is useless to both Alice and Bob. Delays are common, and if there are problems reconciliation is hugely expensive. Consolidating through centralized intermediaries introduces systemic risk, including the risk of unauthorized disclosure and privacy breaches - and with that risk comes increased latency. Banks insist on this approach, despite the downsides, to reduce counterparty risk and to comply with regulations. At every point in time, ownership of the money is completely clear. (To learn more about cash transfers in traditional banking systems, read `this accessible writeup on international money transfers <https://web.archive.org/web/20220731223958/https://medium.com/@yudapramad/how-international-money-transfers-actually-work-bac65f075bb5>`_.)

Services like PayPal, Klarna and credit cards, which provide an experience of instant payments internationally, do this by accepting the counterparty risk or acting as banks themselves. If a shop accepts credit cards and you pay with a credit card, both you and the shop have an account with the credit card company. When you purchase, the credit card company can instantly debit $100 from your account and credit $100 to the shop’s account because it is as if both Alice and Bob are using accounts at the same bank – the bank is certain that Alice has $100 in her account and can execute a simple transaction that deducts $100 from Alice’s account and adds $100 to Bob’s.

Wouldn’t it be great if a system existed that allowed multiple parties to transact with each other with the same immediacy and consistency guarantees a single organization can achieve on a database while each kept sovereignty and privacy of their data? That’s Daml!

Daml is a platform and framework for building real-time multi-party systems, enabling organizations to deliver the experiences modern users expect without assuming counterparty risk or the expense of reconciliation. The sections below describe how Daml achieves this, including the architectural concepts and considerations necessary to build and deploy a solution with Daml effectively.

What Is a Multi-Party Application?
**********************************

A multi-party application is one in which data, and the rules and workflows that govern the data, are shared between two or more parties without any party having to give up sovereignty or any single party (including the application provider) being able to control or override the agreed rules of the system. A party could be a company, a department within a company, an organization, an individual or a person. The specific definition of a party will be unique to the application and the domain of that application, much like the objects within an object-oriented application are unique to that application.

A well-designed multi-party application provides several benefits:
 - a clean, consistent view of all data managed by the application across all parties
 - consistent, connected, and efficient processes between all parties involved in the application
 - privacy controls over portions of the shared data, such that each party sees only the data that it is explicitly entitled to view and/or modify 
 - individual party ownership of and responsibility for sensitive data 

In most cases, no single party can view all of the data within a multi-party application.

Multi-party applications solve complex operational processes while keeping data clean and consistent, thereby eliminating isolated, disconnected and inefficient processes that often require expensive reconciliation. Multi-party applications manage the relationships, agreements and transactions between parties, providing consistent real-time views of all data.

Multi-party solutions utilize distributed ledger (blockchain) technology to ensure each party has an immutable, consistent view of the shared data and business processes that govern the data. By providing a consistent view of data with all counterparties, a multi-party application removes friction, cost, and risk within a joint business process. A distributed ledger protects against a malicious participant in the network, attempting to write or overwrite data to the detriment of other parties.

Important Concepts in Multi-Party Applications
**********************************************

For a multi-party application to fully deliver its value, the following conditions must be met:

Multiple involved parties have data sovereignty – that is, they keep their data within their own systems and require strong guarantees that no external party can access or modify that data outside of pre-agreed rules.
Shared state and rules are codified into an executable schema that determines what data can move between parties, who can read that data, and how that data is manipulated.
Processes happen in real time as there is no additional reconciliation or manual processing required between organizations.

For each individual party to gain the full benefits of a multi-party system, it should:
 - Integrate the application - Bank A must treat the multi-party infrastructure as the golden source of truth for payment information and integrate it as such with the rest of their infrastructure. Otherwise they are merely trading inter-bank reconciliation for intra-bank reconciliation.
 - Utilize composability by building advanced systems that rely on the base-level multi-party agreements. For example, a healthcare claim application should be built using the payment solution. Integrating one multi-party application with another preserves all the properties of each across both applications. In this example, the patient privacy requirements of a health claims application are retained, as are the financial guarantees of the payment application. Without composability, multi-party applications become bigger silos and you end up reconciling the healthcare claims multi-party application with the payments multi-party application.

Smart contracts, distributed ledgers, and blockchains are commonly used to build and deliver multi-party applications. A smart contract codifies the terms of the agreement between parties, including the rights and obligations of each party, directly written into lines of code. The code controls the execution, and transactions are trackable and irreversible. In a multi-party application, the smart contract defines the data workflow through actions taken by the parties involved.

Distributed ledgers and blockchains provide consensus between the parties, with a cryptographic audit trail maintained and validated by the system. Within multi-party solutions, the distributed ledger ensures no one party can unilaterally change the system's state and protects data sovereignty, while the distributed ledger synchronizes the nodes securely in real time.

Key Architectural Concepts in Daml
**********************************

Daml comprises two layers necessary for building multi-party applications: the Daml smart contract language and the Canton blockchain and protocol.

The Daml language is a smart contract language for multi-party applications. Conceptually, Daml is similar to the Structured Query Language (SQL) used in traditional database systems, describing the data schema and rules for manipulating the data. 

The Daml language:
 - defines the shared state between the parties, including process permissions and data ownership 
 - defines workflows, execution policies, and read/write permissions 
 - enables developers to build rich transactions that codify strict business rules
 - defines the APIs through which multi-party applications can talk to each other and compose

The Daml code that collectively makes up the data schema and rules for an application is called a Daml model. Increasingly sophisticated and valuable solutions are composed from existing Daml models, enabling a rich ecosystem that accelerates application development. 

Using the Daml language, developers define the schema for a virtual shared system of record (VSSR). A VSSR is the combined data from all parties involved in the application. The Canton protocol ensures that each party gets a unique view into the VSSR, which is their projection of the full system.

In the execution model for Canton, each party of the application is hosted on a Participant Node (Diagram 1). The Participant Node stores the party’s unique projection and history of the shared system of record. Participant Nodes synchronize by running a consensus protocol (the Canton Protocol) between them. The protocol is executed by sending encrypted messages through Domains, which route messages and offer guaranteed delivery and order consistency. Domains are also units of access control and availability, meaning an application can be additionally protected from interference by other applications or malicious actors by synchronizing it only through a given domain, and restricting which participants can connect to it. 

Diagram 1:

.. figure:: arch-intro-1.png
   :alt: A Domain (center) with four Participant Nodes. Participant Node One hosts Party A; Participant Node Two hosts Party B; Participant Node Three hosts Party C; and Participant Node Four hosts Parties D, E, and F. The Domain can be centralized or distributed, public or private. 

In a composed solution, each domain is a sub-network. A Participant Node connects to one or more Domains, enabling transactions that span Domains (Diagram 2).

Diagram 2:

.. figure:: arch-intro-2.png
   :alt: Three Domains with five Participant Nodes, each hosting one or more parties. Domains A (HL Fabric) and B (Ethereum) have two Domain Nodes each, while Domain C (SQL) has a single Domain Node. Each Participant Node can connect to different Domain Nodes across different Domains.


Transfer Example Using Daml
***************************

Consider the transfer example described above with Alice and Bob. Using Daml, the process looks like this:
#. Alice logs into her online banking at Bank A and enters a transfer to Bob at Bank B.
#. The online banking backend creates a transaction that deducts $100 from Alice’s account and creates a transfer to Bob at Bank B. 
#. When Bank B accepts the transfer, Bank A credits $100 to Bank B’s account at Bank A and Bank B simultaneously credits Bob’s account by $100.
#. Bob’s online banking interfaces with the Daml Ledger and can see the incoming funds in real time.

At every point, ownership of the $100 is completely clear and all systems are fully consistent. 

Next Steps
**********

The suggested next steps are:

* Learn about the Daml language and the Daml Ledger Model. :doc:`Writing Daml <daml/intro/0_Intro>` will introduce you to the basics of a Daml contract, the Daml Ledger model, and the core features of the Daml language. You’ll notice that testing your contracts, including :doc:`testing for failures <intro_2_failure>`, is presented very early in this introduction. We strongly recommend that you write tests as part of the initial development of every Daml project.
* Learn about operating a Daml application with the :doc:`Ledger Administration Introduction <operate-a-daml-ledger/administration-introduction/index>`.

