Why Daml
########

Why Daml: Maximizing Value Through Architectural Alignment
**********************************************************
To maximize the value of Daml and Canton, it is essential to align architectural decisions with the organization’s business goals. This alignment begins with a clear understanding of “why Daml,” focusing on the unique value it provides. This documentation outlines the core value drivers of a Daml application, providing guidance for architectural decisions impacting the two primary stakeholders: :ref:`app providers <app-provider>` and :ref:`app users <app-user>`.

Core Value Drivers
******************
Daml applications provide two key value drivers that are fundamental to deriving maximum value for organizations:

Synchronization Within an Application
=====================================
The first key value driver is synchronization and connectivity between users within an application that spans organizational boundaries. This value is realized in the form of saving operational costs associated with traditional multi-day processes. Daml enables the automation of business processes that involve data sharing and synchronization across organizational boundaries, which hasn’t been possible until now due to the limitations of existing technologies. 
  
For example, asset issuance, registry, and lifecycle management greatly benefit from this synchronization. With Daml, a single system records asset ownership, allowing organizations to avoid the latencies and reconciliation costs associated with traditional technology. This leads to real-time settlement at reduced cost.

Synchronization Across Applications
===================================
The second value driver is synchronization and connectivity across multiple applications. For instance, two separate asset registries – potentially in different organizations – can be composed into an atomic Delivery vs. Payment (DvP) system. This system guarantees that either both the payment and the delivery are settled or neither asset moves.

This cross-application synchronization delivers significant benefits:

* Elimination of counterparty risk and the risk of partial settlements.
* Creation of new market opportunities, including intraday repo trading, asset mobility, and collateral optimization across geographic and organizational boundaries.
* Generation of network effects, where the value of the Daml ecosystem grows exponentially as more users and applications join. Existing users create demand for new assets and services, making it increasingly attractive for others to join and opening up opportunities for further connected or enhanced services through composition.

Stakeholder Perspectives
************************
These values apply differently from the perspectives of app providers and app users.

.. _app-provider:

From the App Provider’s Perspective
===================================
An app provider is an organization that builds and operates a Daml application, offering enhanced services to its app users.

App providers can offer real-time synchronization and connectivity between app-users and applications while preserving stakeholders’ sovereignty and privacy. For example, a bond registry application could synchronize the records of bond ownership between bond issuers and holders. This is a multi-organization business process as issuers and holders are typically different organizations.

.. _app-user:

From the App User’s Perspective
===============================
An app user is an organization that subscribes to the Daml application to use the services provided by the app provider.

Apart from the operational benefits such as significant reduction in operational costs, an app user can participate in cross-organization workflows without the reconciliation challenges. The Daml app relieves the app users from maintaining their own record-keeping systems. They can now focus on building their decision-making system because the Daml app gives them a real-time fully-reconciled system of records with their counterparties.

Enterprise Requirements: Privacy and Independent Control
********************************************************
Daml is targeted at enterprise use cases. App providers and app users are enterprises that operate in a regulatory regime, which imposes constraints on the technologies used by these enterprises. For financial services companies there are two properties that a technology must afford in business transactions to be able to comply with the regulations.

Privacy
=======
Privacy — specifically sub-transaction privacy — is crucial in financial and other regulated industries. In a tokenization application, confidentiality around assets and transfers must be maintained at a sub-transaction level. For example, it is not acceptable for the cap table of a tokenized financial asset to be publicly visible, nor should transfers be visible to parties not involved in the transaction.

Daml addresses this need for privacy through the Canton protocol, which decomposes transaction data into views or projections. These views allow stakeholders to see only the information relevant to their role, as defined in the smart contracts. This sub-transaction privacy ensures confidentiality by distributing sensitive data on a strict need-to-know basis to authorized parties.

Design Considerations to Ensure Privacy
---------------------------------------
To ensure privacy, Daml contracts must be designed to model who gets to see which data. Appropriate roles should be assigned to stakeholders within the contract, ensuring that sub-transaction privacy is maintained in compliance with regulatory requirements.

Independent Control
===================
The second key requirement is independent control. Issuers and custodians of digital assets have regulatory and fiduciary responsibilities to control the availability, scalability, and performance of the technology employed for tokenized asset transactions. They must also comply with regulatory requirements such as Know Your Customer (KYC) and Anti-Money Laundering (AML). Therefore, they need to control the access to assets and transactions. They need control over the smart contracts for any special scenarios that need corrections or overriding. Finally, the technology must be able to evolve to continue to comply with new regulations.

Architectural Considerations to Ensure Independent Control
----------------------------------------------------------
For these reasons, app users must have full control over their legal system of records. If an app user relies solely on the app provider’s participant node, reconciliation challenges may resurface, since it would need to integrate its system of records with the app provider’s participant node. Instead, the app user should deploy their own participant node that serves as their primary source of data to maintain data sovereignty and independent control.

From here, app users can choose to:

* Use the frontend published by the app provider or develop their own for full control and customized functionalities.
* Develop and maintain their own backend to perform backend automation and integration with internal systems. 

App users can gradually build their infrastructure through a staged adoption model, gaining flexibility in how much control they want. They can begin by using the app provider’s frontend and gradually move toward fully customizing their backend to meet their unique operational and regulatory needs. This approach allows app users to independently control adoption costs while steadily gaining full control over access, availability, scalability, and performance. 

Long-Term Value
***************
Strategic Value of Cross-Application Transactions
=================================================
The second key value driver, synchronization and connectivity across multiple applications, enables long-term strategic benefits by creating a broader Daml and Canton ecosystem. In this ecosystem, Daml and Canton act not just as record-keeping systems, but as platforms that enable network effects and exponential value through interconnected applications.

For example, an app provider may start by tokenizing assets and maintaining an asset registry. Once the registry is operational, other app providers can develop complementary services, such as trading and settlement apps, which further increase the utility of the assets and extend the operational gains, such as removal of reconciliation costs, risk-free settlement, and reduction in turnaround times.

By connecting these applications, the platform transforms into a marketplace, attracting more app providers to build additional apps and app users to subscribe to those services. This results in exponentially growing value through an expanding network.

Enablement of Native Interoperability
=====================================
To realize the strategic value, the smart contracts of these applications must be deployed and integrated at the same protocol level to enable interoperability. 

Canton enables interoperability by combining two types of smart contracts: 
* Internal smart contracts that model the business logic
* Integration smart contracts that provide an API, allowing external applications to connect to 

By designing internal and integration smart contracts, app providers can compose higher-order apps and derive long-term value from their Daml applications.

Key Takeaways
*************
1. The first value driver of a Daml app is synchronization within an application, which reduces operational costs and opens opportunities for real-time workflows.
2. The second value driver is synchronization across applications, creating long-term value by enabling network effects and opening new market opportunities.
3. App providers can offer real-time synchronization and connectivity between app users and applications while preserving stakeholders’ sovereignty and privacy. App users benefit from cost savings and real-time cross-organizational workflows.
4. Privacy and independent control are critical for regulatory compliance. Daml ensures privacy through role-specific views and independent control through participant nodes.
5. To maximize value, app providers should design for interoperability, enabling the composition of interconnected applications that extend the utility of the platform.

