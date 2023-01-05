.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Work with Dependencies
======================

The application from :doc:`7_Composing` is a complete and secure model for atomic swaps of assets, but there is plenty of room for improvement. However, one can't implement all features before going live with an application so it's important to understand how to change already running code. There are fundamentally two types of change one may want to make:

1. Upgrades, which change existing logic. For example, one might want the ``Asset`` template to have multiple signatories.
2. Extensions, which merely add new functionality through additional templates.

Upgrades are covered in their own section outside this introduction to Daml: :doc:`/upgrade/index` so in this section we will extend the :doc:`7_Composing` model with a simple second workflow: a multi-leg trade. In doing so, you'll learn about:

- The software architecture of the Daml Stack
- Dependencies and Data Dependencies
- Identifiers

Since we are extending :doc:`7_Composing`, the setup for this chapter is slightly more complex:

#. In a base directory, load the :doc:`7_Composing` project using ``daml new intro7 --template daml-intro-7``. The directory ``intro7`` here is important as it'll be referenced by the other project we are creating.
#. In the same directory, load this chapter's project using ``daml new intro9 --template daml-intro-9``.

``Dependencies`` contains a new module ``Intro.Asset.MultiTrade`` and a corresponding test module ``Test.Intro.Asset.MultiTrade``.

DAR, DALF, Daml-LF, and the Engine
----------------------------------

In :doc:`7_Composing` you already learnt a little about projects, Daml-LF, DAR files, and dependencies. In this chapter we will actually need to have dependencies from the current project to the :doc:`7_Composing` project so it's time to learn a little more about all this.

Let's have a look inside the DAR file of :doc:`7_Composing`. DAR files, like Java JAR files, are just ZIP archives, but the SDK also has a utility to inspect DARs out of the box:

#. Navigate into the ``intro7`` directory.
#. Build using ``daml build -o assets.dar``
#. Run ``daml damlc inspect-dar assets.dar``

You'll get a whole lot of output. Under the header "DAR archive contains the following files:" you'll see that the DAR contains:

#. ``*.dalf`` files for the project and all its dependencies
#. The original Daml source code
#. ``*.hi`` and ``*.hie`` files for each ``*.daml`` file
#. Some meta-inf and config files

The first file is something like ``intro7-1.0.0-887056cbb313b94ab9a6caf34f7fe4fbfe19cb0c861e50d1594c665567ab7625.dalf`` which is the actual compiled package for the project. ``*.dalf`` files contain Daml-LF, which is Daml's intermediate language. The file contents are a binary encoded protobuf message from the `daml-lf schema <https://github.com/digital-asset/daml/tree/main/daml-lf/archive>`_. Daml-LF is evaluated on the Ledger by the Daml Engine, which is a JVM component that is part of tools like the IDE's Script runner, the Sandbox, or proper production ledgers. If Daml-LF is to Daml what Java Bytecode is to Java, the Daml Engine is to Daml what the JVM is to Java.

Hashes and Identifiers
----------------------

Under the heading "DAR archive contains the following packages:" you get a similar looking list of package names, paired with only the long random string repeated. That hexadecimal string, ``887056cbb313b94ab9a6caf34f7fe4fbfe19cb0c861e50d1594c665567ab7625`` in this case, is the package hash and the primary and only identifier for a package that's guaranteed to be available and preserved. Meta information like name ("intro7") and version ("1.0.0") help make it human readable but should not be relied upon. You may not always get DAR files from your compiler, but be loading them from a running Ledger, or get them from an artifact repository.

We can see this in action. When a DAR file gets deployed to a ledger, not all meta information is preserved.

#. Note down your main package hash from running ``inspect-dar`` above
#. Start the project using ``daml start``
#. Open a second terminal and run ``daml ledger fetch-dar --host localhost --port 6865 --main-package-id "887056cbb313b94ab9a6caf34f7fe4fbfe19cb0c861e50d1594c665567ab7625" -o assets_ledger.dar``, making sure to replace the hash with the appropriate one.
#. Run ``daml damlc inspect-dar assets_ledger.dar``

You'll notice two things. Firstly, a lot of the dependencies have lost their names, they are now only identifiable by hash. We could of course also create a second project ``intro7-1.0.0`` with completely different contents so even when name and version are available, package hash is the only safe identifier.

That's why over the Ledger API, all types, like templates and records are identified by the triple ``(entity name, module name, package hash)``. Your client application should know the package hashes it wants to interact with. To aid that, ``inspect-dar`` also provides a machine-readable format for the information it emits: ``daml damlc inspect-dar --json assets_ledger.dar``. The ``main_package_id`` field in the resulting JSON payload is the package hash of our project.

Secondly, you'll notice that all the ``*.daml``, ``*.hi`` and ``*.hie`` files are gone. This leads us to data dependencies.

Dependencies and Data Dependencies
----------------------------------

Dependencies under the ``daml.yaml`` ``dependencies`` group rely on the ``*.hi`` files. The information in these files is crucial for dependencies like the Standard Library, which provide functions, types and typeclasses.

However, as you can see above, this information isn't preserved. Furthermore, preserving this information may not even be desirable. Imagine we had built ``intro7`` with SDK 1.100.0, and are building ``intro9`` with SDK 1.101.0. All the typeclasses and instances on the inbuilt types may have changed and are now present twice -- once from the current SDK and once from the dependency. This gets messy fast, which is why the SDK does not support ``dependencies`` across SDK versions. For dependencies on contract models that were fetched from a ledger, or come from an older SDK version, there is a simpler kind of dependency called ``data-dependencies``. The syntax for ``data-dependencies`` is the same, but they only rely on the "binary" ``*.dalf`` files. The name tries to confer that the main purpose of such dependencies is to handle data: Records, Choices, Templates. The stuff one needs to use contract composability across projects.

For an extension model like this one,``data-dependencies`` are appropriate, so the current project includes :doc:`7_Composing` that way:

.. literalinclude:: daml/daml-intro-9/daml.yaml.template
  :language: yaml
  :start-after:   - daml-stdlib
  :end-before: sandbox-options:

You'll notice a module ``Test.Intro.Asset.TradeSetup``, which is almost a carbon copy of the :doc:`7_Composing` trade setup Scripts. ``data-dependencies`` is designed to use existing contracts and data types. Daml Script is not imported. In practice, we also shouldn't expect that the DAR file we download from the ledger using ``daml ledger fetch-dar`` contains test scripts. For larger projects it's good practice to keep them separate and only deploy templates to the ledger.

Structuring Projects
--------------------

As you've seen here, identifiers depend on the package as a whole and packages always bring all their dependencies with them. Thus changing anything in a complex dependency graph can have significant repercussions. It is therefore advisable to keep dependency graphs simple, and to separate concerns which are likely to change at different rates into separate packages.

For example, in all our projects in this intro, including this chapter, our scripts are in the same project as our templates. In practice, that means changing a test changes all identifiers, which is not desirable. It's better for maintainability to separate tests from main templates. If we had done that in :doc:`7_Composing`, that would also have saved us from copying :doc:`7_Composing`.

Similarly, we included ``Trade`` in the same project as ``Asset`` in :doc:`7_Composing`, even though ``Trade`` is a pure extension to the core ``Asset`` model. If we expect ``Trade`` to need more frequent changes, it may be a good idea to split it out into a separate project from the start.

Next Up
-------

The ``MultiTrade`` model has more complex control flow and data handling than previous models. In :doc:`10_Functional101` you'll learn how to write more advanced logic: control flow, folds, common typeclasses, custom functions, and the Standard Library. We'll be using the same projects so don't delete your folders just yet.
