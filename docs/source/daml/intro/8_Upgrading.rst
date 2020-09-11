.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

8 Upgrading
===========

The application from Chapter 7 is a complete and secure model for atomic swaps of assets, but it leaves a lot to be desired. The trade process requires several transactions to set up, first to get the right quantity on an ``Asset`` contract, then to create tha approval. Client applications need to handle Cids and pass them into choices. Trades only swap one ``Asset`` for another, there are no multi-leg swaps. In this chapter, we will improve the model from Chapter 7 in several respects, and run an Upgrade, meaning we will move from the Chapter 7 model to the Chapter 8 model while preserving data. In the process, you will learn about

- The software architecture of the DAML Stack
- Dependencies and Data Dependencies
- Identifiers
- Structuring Upgrade projects
- Writing Upgrade Contracts and performing an upgrade


Since we are upgrading from chapter 7, the setup for this chapter is slightly more complex:

#. In a base directory, load the chapter 7 project using ``daml new 7_Composing daml-intro-7``. The directory ``7_Composing`` here is important as it'll be referenced by the other projects we are creating.
#. In the same directory, load the chapter 8 Asset project using ``daml new 8_Assets daml-intro-8-assets``.
#. In the same directory, load the chapter 8 Upgrade project using ``daml new 8_Upgrade daml-intro-8-upgrade``.

``8_Assets`` is an improved version of the chapter 7 model, and ``8_Upgrade`` is a DAML model used to upgrade an existing, running application of the chapter 7 model to the new improved ``8_Assets`` model. The upgraded model has one major new feature: Multi-signatory owners. Wherever we had ``owner : Party`` in chapter 7, we now have ``owner : [Party]`` or something similar. This changes almost all templates. We can't just use composition and add new templates to the ledger, we need to convert old ``Asset`` contracts into new ones. We will use this feature to explore control flow, typeclasses, functions, and the Standard Libary in chapter 9, but for now, we will concentrate on the Upgrading part.

DAR, DALF, DAML-LF, and the Engine
----------------------------------

In :doc:`7_Composing` you already learnt a little about projects, DAML-LF, DAR files, and dependencies. In this chapter we will actually need to have dependencies from the upgrade project to the chapter 7 and 8 asset projects so it's time to learn a little more about all this.

Let's have a look inside the DAR file of chapter 7. DAR files, like Java JAR files are just ZIP archives, but the DAML SDK also has a utility to inspect DARs out of the box:

#. Navigate into the ``7_Composing`` directory.
#. Build using ``daml build -o assets.dar``
#. Run ``daml damlc inspect-dar assets.dar``

You'll get a whole lot of output. Under the header "DAR archive contains the following files:" you'll see that the DAR contains

#. ``*.dalf`` files for the project and all its dependencies
#. The original DAML source code
#. ``*.hi`` and ``*.hie`` files for each ``*.daml`` file
#. Some meta-inf and config files

The first file is something like ``7_Composing-1.0.0-887056cbb313b94ab9a6caf34f7fe4fbfe19cb0c861e50d1594c665567ab7625.dalf`` which is the actual compiled package for the project. ``*.dalf`` files contain DAML-LF, which is DAML's intermediate language. The file contents are a binary encoded protobuf message from the `daml-lf schema <https://github.com/digital-asset/daml/tree/master/daml-lf/archive>`_.  DAML-LF is evaluated on the Ledger by the DAML Engine, which is a JVM component that is part of tools like the IDE's Script runner, the Sandbox, or proper production ledgers. If DAML-LF is to DAML what Java Bytecode is to Java, the DAML Engine is to DAML what the JVM is to Java.

Hashes and Identifiers
----------------------

The  ``*.hi`` and ``*.hie`` files are interface files, which tell the DAML compiler how to map the bytecode back to DAML types.

Under the heading "DAR archive contains the following packages:" you get a similar looking list of package names, paired with only the long random string repeated. That hexadecimal string, ``887056cbb313b94ab9a6caf34f7fe4fbfe19cb0c861e50d1594c665567ab7625`` in this case, is the package hash and the primary and only identifier for a package that's guaranteed to be available and preserved. Meta information like name ("7_Composing") and version ("1.0.0") help make it human readable but should not be relied upon. You may not always get DAR files from your compiler, but be loading them from a running Ledger, or get them from an artifact repository.

We can see this in action. When a DAR file gets deployed to a ledger, not all meta information is preserved. 

#. Note down your main package hash from running ``inspect-dar`` above
#. Start the project using ``daml start``
#. Open a second terminal and run ``daml ledger fetch-dar --host localhost --port 6865 --main-package-id "887056cbb313b94ab9a6caf34f7fe4fbfe19cb0c861e50d1594c665567ab7625" -o assets_ledger.dar``, making sure to replace the hash with the appropriate one.
#. Run ``daml damlc inspect-dar assets_ledger.dar``

You'll notice two things. Firstly, a lot of the dependencies have lost their names, they are now only identifyable by hash. We could of course also create a second project ``7_Composing-1.0.0`` with completely different contents so even when name and version are avaialble, package hash is the only safe identifier.

That's why over the Ledger API, all types, like templates and records are identified by the triple ``(entity name, module name, package hash)``. Your client application should know the package hashes it wants to interact with. To aid that, ``inspect-dar`` also provides a machine-readable format for the information it emits: ``daml damlc inspect-dar --json assets_ledger.dar``. The ``main_package_id`` field in the resulting JSON payload is the package hash of our project.

Secondly, you'll notice that all the ``*.daml``, ``*.hi`` and ``*.hie`` files are gone. This leads us to data dependencies.

Dependencies and Data Dependencies
----------------------------------

Dependencies under the ``daml.yaml`` ``dependencies`` group rely on the ``*.hi`` and ``*.hie`` files, which are interface files that allow the compiler to map the low-level DAML-LF back to high level DAML types and typeclasses. This sort of information is crucial for dependencies like the Standard Library, which provides functions, types and typeclasses.

However, as you can see above, this information isn't preserved. Furthermore, preserving this information may not even be desireable. Imagine we had built ``7_Composing`` with SDK 1.100.0, and are building ``8_Upgrading`` with SDK 1.101.0. An inbuilt typeclass like ``Eq`` may have changed in between so we now have two different ``==`` functions. The one from the 1.100.0 Standard Library and the one from 1.101.0. This gets messy fast, which is why the SDK does not support ``dependencies`` across SDK versions. For dependencies on contract models that were fetched from a ledger, or come from an older SDK version, there is a simpler kind of dependency called ``data-dependencies``. The syntax for ``data-dependencies`` is the same, but they only rely on the "binary" ``*.dalf`` files. The name tries to confer that the main purpose of such dependencies is to handle data: Records, Choices, Templates. The stuff one needs to use contract composability across projects.

For an upgrade model like this one, ``data-dependencies`` are appropriate so the ``upgrade`` project includes both the old and new asset models that way.

.. literalinclude:: daml/daml-intro-8-upgrade/daml.yaml.template
  :language: yaml
  :start-after:   - daml-stdlib
  :end-before: sandbox-options:

Structuring Projects for Upgrades
---------------------------------

There are two important things to not about how the new version of the Asset model, and the Upgrade model are structured:

1. The new version of the Asset model does not depend on the old one
2. All modules in the new model are prefixed with ``Intro.Asset.V2`` rather than just ``Intro.Asset``

The first measure has to do with breaking dependency chains. If we put the new model and the upgrade in a single package, we get a dependency chain that look like this:

.. code-block: none

  V1 <- V2 <- V3 <- V4 <- ...


Any ``Asset`` contract from any future version will depend on all old versions. We can never forget about old packages. By separating out upgrade package from new version, we get a graph that looks like this:

.. code-block: none

    U12  U23  U34  ...
  ┌─┴─┐┌─┴─┐┌─┴─┐┌─┴─┐
  V1   V2   V3   V4   ...

An ``Asset`` in version N depends only on its own package, not on anything that came before.

The second measure has to do with keeping it easy to distinguish versions and avoid name collisions. As described above, we can always distinguish an ``Asset`` from version 1 from an ``Asset`` from version 2 by looking at the module. If we used the same module names, we would have to mitigate module name collisions as described in :ref:`module_collisions`, and use the package hash to distinguish verisons on the Ledger API. It's easily possible, but less comfortable during development.

Anatomy of an Upgrade
---------------------

A general DAML model does not have a central party which can unilaterally change the rules. The Chapter 7 model is a good example of this. The ``AssetHolder`` and ``Asset`` contracts are signed by both ``issuer`` and ``owner``. The two parties have agreed on the rules governing ``Asset`` contracts, and if those rules are to be changed, it needs mutual agreement. However, it's not quite as simple as getting everyone's agreement and then upgrading the world. Issuers and owners are in a many-to-many relationship and not everyone may want to upgrade at the same time. Given DAML's privacy model, it may even be the case that parties don't know about each other's involvement in the application at all. 

From the above, we can see that for any upgrade, one has to determine the right "unit" of upgrade. It has to be small enough that it's feasible to get the needed stakeholder agreement. Granularity of upgrades has to be balanced with complexity and compatibility though. A possible design would be to say that individual contracts can be upgraded and downgraded again, and that the V2 model should offer full backwards compatibility in the sense that everywhere a V2 contract can be used, a V1 contract will also work. It's possible to do that, but would require the V2 model to depend on V1, and adds a lot of complexity.

For this chapter we will make the following choices:

- The upgrade is agreed per issuer/owner relationship.
- Once the upgrade is mutually agreed, it's forward only. There are no rollbacks or backwards compatibility.
- Agreeing to the upgrade upgrades the ``AssetHolder`` contract, preventing the owner from generating new v1 ``TransferApprovals``, and the issuer issuing new v1 assets. Ie after accepting the upgrade, the total pool of v1 assets the owner can ever hold is limite by existing ``Asset`` and ``TransferApproval`` contracts. They can no longer start new v1 ``Trade`` workflows as they can't get the ``TransferApprovals``. This incentivizes the ``owner`` to upgrade their V1 ``Asset``\ s to V2 in bulk.
- ``Trade``, ``TransferProposal`` and ``TransferApproval`` contracts can not be upgraded. They have to be cancelled.
- The *completion* of the upgrade has to be mutually agreed between issuer and owner to make sure neither can leave contract in v1 lying around.

All of this is encoded in the ``Intro.Asset.Upgrade.V2`` module of the upgrade project.

.. literalinclude:: daml/daml-intro-8-upgrade/daml/Intro/Asset/Upgrade/V2.daml
  :language: daml

The ``issuer`` initiates an upgrade by creating ``UpgradeInvite``, the ``owner`` uses ``Accept_Invite`` followed by ``UpgradeContracts`` to perform the upgrade, and finally indicates completion with an ``UpgradeConfirmation``. The ``issuer`` then signs this off using ``CompleteUpgrade``.

You'll notice that the model doesn't enforce that no V1 contracts remain. That's because it can't. Other than contract keys, DAML has no non-existence query functionality so you can't assert that no V1 contracts exist. But that's not really a problem. Because both parties need to sign off that the upgrade is complete, you can only leave V1 contarcts active and still complete the upgrade if both parties agree to this. And in that case, they could just re-establish a V1 ``AssetHolder`` contract so such a safe-guard would achieve nothing. The constraint that no V1 contracts should remain has to be enforced byt he stakeholders because it's in their interest.

Because all we are doing is changing the data type of ``Asset``, the actual "upgrade" is encapsulated in just three small snippets of code:

#. The top-level function ``mapAssetV1ToV2`` which takes a V1 ``Asset`` and maps it to a V2 ``Asset`` by wrapping the ``owner`` in a list.
#. The locally defined function ``upgradeAsset`` which checks preconditions on a V1 ``Asset`` and then uses ``archive``, ``mapAssetV1ToV2`` and ``create`` to substitute a V1 ``Asset`` contract with a V2 contract.
#. A ``mapA`` statment, which performs ``upgradeAsset`` on a list of ``ContractId Asset``.

You'll learn more about functions and ``mapA`` in :doc:`9_Functional101`.

And that's really it. The upgrade model could be extended using rollback capabilities or similar, but the complexity of upgrades is less in the upgrade contracts than in actually performing the upgrade.

Executing an Upgrade
--------------------

The ``Test`` modules of the ``8_Upgrade`` project demonstrate the upgrade using DAML Script. 

You'll notice a module ``Test.Intro.Asset.Upgrade.V1Setup``, which is almost a carbon copy of the V1 trade setup Scripts. ``data-dependencies`` is designed to use existing contracts and data types. DAML Script is not imported. In practice, we also shouldn't expect that the DAR file we download from the ledger using ``daml ledger fetch-dar`` contains test scripts. For larger projects it's good practice to keep them separate and only deploy templates to the ledger.

The entry point for the upgrade is script ``Test.Intro.Asset.Upgrade.V1Setup.testUpgrade``. The ledger is set up using the V1 trade setup scripts. The four relationships between banks and asset holders are then captured in a ``Relationship`` type, and then executed using a ``forA_`` statement. We'll revisit ``forA`` and its underscored version later. For now, read ``forA_ xs fn`` as "For each ``x`` in ``xs``, perform the action ``fn x``". The action in question here is ``runCompleteUpgrade``, which as the name suggests, outlines a complete upgrade for the relationship ``r``:

.. literalinclude:: daml/daml-intro-8-upgrade/daml/Test/Intro/Asset/Upgrade/V2.daml
  :language: daml
  :start-after: -- RUN_COMPLETE_UPGRADE_BEGIN
  :end-before: -- RUN_COMPLETE_UPGRADE_END

We won't go in depth into each of the steps in this chapter, but here are the steps performed, in line with the "Anatomy" abovve:

#. In ``initiateUpgrade`` the ``issuer`` of the upgrade checks for V1 ``AssetHolder`` and  ``AssetHolderInvite``, and creates corresponding ``UpgradeInvite`` or V2  ``AssetHolderInvite``.
#. In ``acceptUpgrade`` the ``owner`` matches up V1 ``AssetHolder`` contracts with ``UpgradeInvite`` contracts, and calls the ``Accept_Upgrade`` choice for each pair.
#. In ``performUpgrade`` the ``owner`` tidies up by upgrading all V1 ``Asset`` contracts and cancelling all ``TransferApproval`` and ``TransferProposal`` contracts.
#. In ``ownerCheckUpgradeComplete`` the ``owner`` checks whether they think the upgrade for the given relationship is complete.
#. In ``confirmComplation`` the ``owner`` creates the ``UpgradeConfirmation`` contract for each ``Upgrade`` contract.
#. In ``issuerCheckUpgradeComplete`` the ``issuer`` checks whether they think the upgrade is complete.
#. In ``completeUpgrade`` the ``issuer`` matches up ``Upgrade`` with ``UpgradeConfirmation`` contracts and completes the process.

In a soon to come Chapter 10 of the introduction to DAML you'll learn why the Scripts are structured the way they are, and how to use them to test beyond the IDE.

This concludes this introduction to upgrades. If you want to learn more, you can also check out the standalone docs pages on upgrading: :doc:`/upgrade/index`.

Next up
-------

Both the V2 ``Asset`` model as well as the upgrade process itself involves considerably more complex control flow and data handling than previous models. In :doc:`9_Functional101` you'll learn how to write more advanced logic: control flow, folds, common typeclasses, custom functions, and the Standard Library. We'll be using the same projects so don't delete your Chapter 7 and 8 fodlers just yet.