.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _identity-package-management:

Identity and Package Management
###############################

Since DAML ledgers enable parties to automate the management of their rights and obligations through smart contract code, they also have to provide party and code management functions.
Hence, this document addresses:

1. Management of parties' digital identifiers in a DAML ledger.

2. Distribution of smart contract code between the parties connected to the same DAML ledger.

The access to this functionality is usually more restricted compared to the other Ledger API services, as they are part of the administrative API.
This document is intended for the users and implementers of this API.

The administrative part of the Ledger API provides both a :ref:`party management service <com.daml.ledger.api.v1.admin.PartyManagementService>` and a :ref:`package service <com.daml.ledger.api.v1.admin.PackageManagementService>`.
Any implementation of the party and package services is guaranteed to accept inputs and provide outputs of the format specified by these services.
However, the services' *behavior* -- the relationship between the inputs and outputs that the various parties observe -- is largely implementation dependent.
The remainder of the document will present:

#. The minimal behavioral guarantees for identity and package services across all ledger implementations. The service users can rely on these guarantees, and the implementers must ensure that they hold.

#. Guidelines for service users, explaining how the :ref:`ledger's topology <daml-ledger-topologies>` influences the unspecified part of the behavior.

.. _identity-management:

Identity Management
*******************

A DAML ledger may freely define its own format of party and :ref:`participant node <participant-node-def>` identifiers, with some minor constraints on the identifiers' serialized form.
For example, a ledger may use human-readable strings as identifiers, such as "Alice" or "Alice's Bank".
A different ledger might use public keys as identifiers, or the keys' fingerprints.
The applications should thus not rely on the format of the identifier -- even a software upgrade of a DAML ledger may introduce a new format.

By definition, identifiers identify parties, and are thus unique for a ledger.
They do not, however, have to be unique across different ledgers.
That is, two identical identifiers in two different ledgers do not necessarily identify the same real-world party.
Moreover, a real-world entity can have multiple identifiers (and thus parties) within the same ledger.

Since the identifiers might be difficult to interpret and manage for humans, the ledger may also accompany each identifier with a user-friendly **display name**.
Unlike the identifier, the display name is not guaranteed to be unique, and two different participant nodes might return different display names for the same party identifier.
Furthermore, a display name is in general not guaranteed to have any link to real world identities.
For example, a party with a display name "Attorney of Nigerian Prince" might well be controlled by a real-world entity without a bar exam.
However, particular ledger deployments might make stronger guarantees about this link.
Finally, the association of identifiers to display names may change over time.
For example, a party might change its display name from "Bruce" to "Caitlyn" -- as long as the identifier remains the same, so does the party.

.. _provisioning-ledger-identifiers:

Provisioning Identifiers
========================

The set of parties of any DAML ledger is dynamic: new parties may always be added to the system.
The first step in adding a new party to the ledger is to provision a new identifier for the party.
The Ledger API provides an :ref:`AllocateParty <com.daml.ledger.api.v1.admin.AllocatePartyRequest>` method for this purpose.
The method, if successful, returns an new party identifier.
The ``AllocateParty`` call can take the desired identifier and display name as optional parameters, but these are merely hints and the ledger implementation may completely ignore them.

If the call returns a new identifier, the :ref:`participant node <participant-node-def>` serving this call is ready to host the party with this identifier.
In global state topologies, the returned identifier is guaranteed to be **unique** in the ledger; namely, no other call of the ``AllocateParty`` method at this or any other ledger participant may return the same identifier.
In partitioned state topologies, the identifier is also unique as long as the participant node is configured correctly (in particular, it does not share its private key with other participant nodes).
If the ledger has a :ref:`global state topology <global-state-topologies>`, the new identifier will generally be allocated and vetted by the operator of the writer node(s).
For example, in the `replicated committer topology <replicated-committer-topology>`__, the committers can jointly decide on whether to approve the provisioning, and which identifier to return.
If they refuse to provision the identifier, the method call fails.

After an identifier is returned, the ledger is set up in such a way that the participant node serving the call is allowed to issue commands and receive transactions on behalf of the party.
However, the newly provisioned identifier need not be visible to the other participant nodes.
For example, consider the setup with two participants ``P1`` and ``P2``, where the party ``Alice_123`` is hosted on ``P1``.
Assume that a new party ``Bob_456`` is next successfully allocated on ``P2``.
This does not yet guarantee that ``Alice_123`` can now submit a command creating a new contract with ``Bob_456`` as an observer.
In general, ``Alice_123`` will be able to do this in a ledger with a :ref:`global state topology <global-state-topologies>`.
In such ledgers, the nodes holding the physical shared ledger typically also maintain a central directory of all parties in the system.
However, such a directory may not exist for a ledger with a :ref:`partitioned topology <partitioned-topologies>`.
In fact, in such a ledger, the participants ``P1`` and ``P2`` might not have a way to communicate to each other, or might not even be aware of each other's existence.

For diagnostics, the ledger also provides a :ref:`ListKnownParties <com.daml.ledger.api.v1.admin.ListKnownPartiesRequest>` method which lists parties known to the participant node.
The parties can be local (i.e., hosted by the participant) or not.

.. _identifiers-and-authentication:

Identifiers and Ledger Authentication
=====================================

To issue commands or receive transactions on behalf of a newly provisioned party, an application must authenticate itself to the party's hosting participant as someone authorized to represent the party.
Before the newly provisioned party can be used, the application will have to obtain a token for this party.
The issuance of tokens is specific to each ledger and independent of the Ledger API.
The same is true for the policy which the participants use to decide whether to accept a token.

To learn more about ledger API authentication, please read the :doc:`Authentication documentation </app-dev/authentication>`.

.. _identifiers-and-real-world:

Identifiers and the Real World
==============================

The "substrate" on which DAML workflows are built are the real-world obligations of the parties in the workflow.
To give value to these obligations, they must be connected to parties in the real world.
However, the process of linking party identifiers to real-world entities is left to the ledger implementation.

A :ref:`global state topology <global-state-topologies>` might simplify the process by trusting the operator of the writer node(s) with providing the link to the real world.
For example, if the operator is a stock exchange, it might guarantee that a real-world exchange participant whose legal name is "Bank Inc." is represented by a ledger party with the identifier "Bank Inc.".
Alternatively, it might use a random identifier, but guarantee that the display name is "Bank Inc.".
Ledgers with :ref:`partitioned topologies <partitioned-topologies>` in general might not have such a single store of identities.
The solutions for linking the identifiers to real-world identities could rely on certificate chains, `verifiable credentials <https://www.w3.org/TR/vc-data-model/>`__, or other mechanisms.
The mechanisms can be implemented off-ledger, using DAML workflows (for instance, a "know your customer" workflow), or a combination of these.

.. _package-management:

Package Management
******************

All DAML ledgers implement endpoints that allow for provisioning new DAML code to the ledger.
The vetting process for this code, however, depends on the particular ledger implementation and its configuration.
The remainder of this section describes the endpoints and general principles behind the vetting process.
The details of the process are ledger-dependent.

.. _package-formats-and-identifiers:

Package Formats and Identifiers
===============================

Any code -- i.e., DAML templates -- to be uploaded must compiled down to the :ref:`DAML-LF <daml-lf>` language.
The unit of packaging for DAML-LF is the :ref:`.dalf <dar-file-dalf-file>` file.
Each ``.dalf`` file is uniquely identified by its **package identifier**, which is the hash of its contents.
Templates in a ``.dalf`` file can reference templates from other ``.dalf`` files, i.e., ``.dalf`` files can depend on other ``.dalf`` files.
A :ref:`.dar <dar-file-dalf-file>` file is a simple archive containing multiple ``.dalf`` files, and has no identifier of its own.
The archive provides a convenient way to package ``.dalf`` files together with their dependencies.
The Ledger API supports only ``.dar`` file uploads.
Internally, the ledger implementation need not (and often will not) store the uploaded ``.dar`` files, but only the contained ``.dalf`` files.

.. _package-management-api:

Package Management API
======================

The package management API supports two methods:

- :ref:`UploadDarFile <com.daml.ledger.api.v1.admin.UploadDarFileRequest>` for uploading ``.dar`` files.
  The ledger implementation is, however, free to reject any and all packages and return an error.
  Furthermore, even if the method call succeeds, the ledger's vetting process might restrict the usability of the template.
  For example, assume that Alice successfully uploads a ``.dar`` file to her participant containing a ``NewTemplate`` template.
  It may happen that she can now issue commands that create ``NewTemplate`` instances with Bob as a stakeholder, but that all commands that create ``NewTemplate`` instances with Charlie as a stakeholder fail.

- :ref:`ListKnownPackages <com.daml.ledger.api.v1.admin.ListKnownPackagesRequest>` that lists the ``.dalf`` package vetted for usage at the participant node.
  Like with the previous method, the usability of the listed templates depends on the ledger's vetting process.

.. _package-management-vetting:

Package Vetting
===============

Using a DAML package entails running its DAML code.
The DAML interpreter ensures that the DAML code cannot interact with the environment of the system on which it is executing.
However, the operators of the ledger infrastructure nodes may still wish to review and vet any DAML code before allowing it to execute.
One reason for this is that the DAML interpreter currently lacks a notion of reproducible resource limits, and executing a DAML contract might result in high memory or CPU usage.

Thus, DAML ledgers generally allow some form of vetting a package before running its code on a node.
Not all nodes in a DAML ledger must vet all packages, as it is possible that some of them will not execute the code.
For example, in :ref:`global state topologies <global-state-topologies>`, every :ref:`trust domain <trust-domain>` that controls how commits are appended to the shared ledger must execute DAML code.
Thus, the operators of these trust domains will in general be allowed to vet the code before they execute it.
The exact vetting mechanism is ledger-dependent.
For example, in the :ref:`DAML Sandbox <sandbox-manual>`, the vetting is implicit: uploading a package through the Ledger API already vets the package, since it's assumed that only the system administrator has access to these API facilities.
In a replicated ledger, the vetting might require consent from all or a quorum of replicas.
The vetting process can be manual, where an administrator inspects each package, or it can be automated, for example, by accepting only packages with a digital signature from a trusted package issuer.

In :ref:`partitioned topologies <partitioned-topologies>`, individual trust domains store only parts of the ledger.
Thus, they only need to approve packages whose templates are used in the ledger part visible to them.
For example, in DAML on `R3 Corda <https://www.corda.net>`__, participants only need to approve code for the contracts in their parties' projections.
If non-validating Corda notaries are used, they do not need to vet code.
If validating Corda notaries are used, they can also choose which code to vet.
In `Canton <https://canton.io>`__, participant nodes also only need to vet code for the contracts of the parties they host.
As only participants execute contract code, only they need to vet it.
The vetting results may also differ at different participants.
For example, participants ``P1`` and ``P2`` might vet a package containing a ``NewTemplate`` template, whereas ``P3`` might reject it.
In that case, if Alice is hosted at ``P1``, she can create ``NewTemplate`` instances with stakeholder Bob who is hosted at ``P2``, but not with stakeholder Charlie if he's hosted at ``P3``.

.. _package-upgrades:

Package Upgrades
================

The Ledger API does not have any special support for package upgrades.
A new version of an existing package is treated the same as a completely new package, and undergoes the same vetting process.
Upgrades to active contracts can be done by the DAML code of the new package version, by archiving the old contracts and creating new ones.
