.. Called introduction here because an "Overview" chapter inside the "Overview" subsite is confusing

Introduction
============

The purpose of the `Overview` manual section is to:
* Introduce you to the importance and problems of multi-party applications.
* Provide a high-level overview of how Canton can help you build and run multi-party applications.
* Explain the different components of the Canton ecosystem and how they fit together.
* Show how the different products of Digital Asset allow you to join the Canton Network as a user or a provider of services.

Multi-Party Applications
------------------------

We use software applications everyday. They help us communicate, share information, and make decisions.
But what happens when we need to collaborate with multiple parties on the same set of data? How do we ensure that
everyone has access and can operate on the same information? Who controls the data? Who controls who can collaborate?
If we trust each other or work in the same organization, we delegate this to a central authority operating a
centralised application, such as the IT department of our  company or one of the large cloud providers. But what if we
are not part of the same organization? What if we cannot agree on who should operate the application? What if we do
not trust each other but still want to collaborate? Then we end up with a **multi-party application**.

Originally, multi-party applications are built by each party implementing their own version of the application, and
exchanging messages to signal data changes. Such applications are very bilateral in nature and require
a lot of coordination between the parties to keep the implementations and the data in sync. These technical challenges
impact the efficiency and the business value, leading to high cost, low flexibility and long lead times. Well known
examples of such applications can be found in the financial markets or in the health care sector, requiring large
and well-funded organizations that can afford to build and maintain them.

Technically, the problem can be stated as follows: how do you coordinate reads and writes across multiple databases of
multiple, potentially unknown parties who do not trust each other nor a central authority, but with the
strong guarantee that the data is always correct and consistent, and only accessed and modified according to the
individually agreed rules for the given data set?

This is the problem that Canton solves out of the box, enabling a network of self-sovereign, tightly-coupled, multi-party
applications built for seamless real world business success: the Canton Network.

Continue diving into :ref:`Multi-Party Applications <overview-multiparty-applications-intro>` in the respective section of the manual.

Ledger Properties
-----------------

In order to solve the problem of complex, real-world multi-party applications, Canton is designed to provide the following properties:

* **Privacy**: The data is only visible to the parties that are allowed to see it. No one else can access it.
* **Integrity**: The data is always correct and consistent. No one can change it in a way that would violate the pre-agreed rules.
* **App Composability**: The applications can unilaterally be composed together to create new applications. This allows to build complex applications that are built on top of existing ones, but in a way that does not impact existing users.
* **Network Composability** and **Horizontal Scalability**: The network can be extended by adding new validators and synchronizers, such that the network can be extended unilaterally and scaled horizontally. It still remains a single network of networks.
* **Regulatory Compliance**: The data is shared, stored and operated by the parties owning it. This allows to comply with existing regulations of the jurisdictions where the parties operate.

Continue diving into the :externalref:`Canton requirements <requirements>` and learn more about how Canton
is :brokenref:`different from other ledgers <comparison-blockchain>`.

Elements of the Canton Network
------------------------------

The **Canton Network** is the union of all applications and users that are built and operated using the software **Canton**,
which connects individual instances through the **Canton Protocol**.

The shared data items managed and synchronized by Canton are called **contract instances**. Every contract instance is
associated with a **template** that defines the **smart contract choices** for the contract instance. These choices
represent the possible update commands on the ledger, archiving a given set of contracts and creating new ones
atomically. These rules are written in the **DAML** programming language and contain the business logic and authorization
rules.

The entirety of the contract instances and templates is called the **Ledger**. Each contract is owned by a group of
stakeholders, called **Parties**. Each party participates in the network through their validator node. This validator
node has only visibility to the subset of contracts of the parties associated. As validator nodes are able to connect
to different networks, the entire network forms a single **Virtual Global Ledger** which is not controlled by any
single entity.

The software **Canton** has two infrastructure components: **Synchronizers** and **Validators** (formerly named participants).
Validators process and validate transactions, managing the ledger state of their parties. Validators do not expose any public port,
but instead connect to synchronizers to form a network with other validators. Each validator can connect to multiple
synchronizers unilaterally, creating a network of networks.

In order to transact with each other on a given transaction, validators need to find at least one synchronizer where
they are all connected to. This synchronizer will then order, **buffer and forward the encrypted messages** to the respective
validators without understanding what is being transacted on. There is one specific instance of a synchronizer that is
called the **Global Synchronizer** (`https://sync.global <https://sync.global>`_), serving as the global backbone of the network.

Contracts are not tied to synchronizers. Synchronizers are not aware of each other. Instead, validators can use any
suitable synchronizer for their transactions, creating an **natively interoperable network** of synchronizers and validators.
This allows the Canton Network to **scale horizontally** by just adding more synchronizers and validators whenever needed.

Ledger concepts expressed in database terms
-------------------------------------------

The concepts can be related to databases: templates are table schemas, contract instances are table rows, choices
are stored procedures performing a set of atomic delete and insert statements. Different actors on the network now
exchange which stored procedures they want to run through the Canton protocol. The protocol orders, validates and distributes
these requests to the affected parties such that they can apply it to their local data stores deterministically.

The protocol uses the synchronizer to distribute and buffer the encrypted messages between the different data stores to
provide a reliable total order multi-cast.

Join the Canton Network
-----------------------
In the Canton Network, you can either be a user, an application provider, or an infrastructure operator.
Digital Asset provides a set of products and services that allow you to join the Canton Network conveniently and easily.
* Use Canton Network Portfolio as a hosted solution to hold and use your digital assets on the network.
* Build your own applications using our SDK.
* Use our financial application building blocks to accelerate the development of your own applications.
* Run your own validator node as part of an existing network.
* Create your own sub-network by deploying your own synchronizer.

Read more about the different ways to join the Canton Network in the :brokenref:`How to Engage with the Network <how-to-engage>` section.
