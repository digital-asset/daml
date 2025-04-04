.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _assistant-manual-building-dars:

Build Daml Projects
###################

.. _assistant-manual-build-a-project:

Build a project
*******************

To compile your Daml source code into a Daml archive (a ``.dar`` file), run::

  daml build

You can control the build by changing your project's ``daml.yaml``:

``name``
  The name of the project.

``version``
  The project version.

``source``
  The path to the source code.

By default, the generated ``.dar`` file is created in ``.daml/dist/${name}-${version}.dar``. To override the default location, pass the ``-o`` argument to ``daml build``::

  daml build -o path/to/darfile.dar

.. _multi-package-build:

Build multiple packages
***********************

``daml build`` supports multi-package builds, an optional feature for building
and managing multiple interdependent packages simultaneously. The multi-package
build feature consists of a configuration file and a set of flags.

You can use multi-package builds to:

-  Configure ``daml build`` to automatically rebuild DARs used in data
   dependencies, when the source code corresponding to those DARs
   changes
-  Build all of the packages in a project simultaneously, using ``daml build --all``
-  Clean all build artifacts in a project, using ``daml clean --all``

Daml Studio also supports multi-package projects, which is covered
:ref:`here <daml-studio-packages>`.

Overview
========

To use multi-package builds, create a ``multi-package.yaml`` configuration file at the root of the project. This file serves as a register of the interdependent packages in your project.

``multi-package.yaml``

.. code:: yaml

   packages:
   - <path to first package>
   - <path to second package>

Once ``multi-package.yaml`` is in place, you can run ``daml build --all`` to build all the packages listed in the ``multi-package.yaml`` file. Or you can run ``daml build`` within a package to build that package and its dependencies.

If you don't want to use multi-package builds, omit the ``multi-package.yaml`` 
file from your project.

.. note::

    ``multi-package.yaml`` assumes that dependent packages use the default
    ``daml build`` locations for DAR files. If you have shell scripts that move a
    package's DAR file after building, you'll need to use the 
    :ref:`output flag <output-flag>` instead. 

Build without multi-package
-----------------------------------

As context for the multi-package illustrations in later sections,
here's an example of how to use ``daml build`` without the multi-package 
feature:

.. code::

   > # ... make changes to Model ...
   > cd ./package-Model/  # Navigate to package-Model
   > daml build           # Build Model.dar
   > cd ../package-Logic/ # Navigate to package-Logic
   > daml build           # Build Logic.dar

Multi-package configuration
===========================

To configure which packages to build automatically, list the packages under a 
``packages:`` header in your ``multi-package.yaml`` configuration file:

``multi-package.yaml``

.. code:: yaml

  packages:
  - <path to package>
  - <path to package>

For each package, specify the relative path to the directory containing the ``daml.yaml`` file for that package.

Example
-------

In this example, the Daml project contains two packages, ``package-Model`` and ``package-Logic``. The ``multi-package.yaml`` file at the root of the project contains relative paths to the Model and Logic packages.

``multi-package.yaml``

.. code:: yaml

   packages:
   - ./package-Logic
   - ./package-Model

Here's the resulting project tree:

::

   > tree
   .
   ├── multi-package.yaml
   ├── package-Logic
   │   ├── daml/...
   │   └── daml.yaml
   └── package-Model
       ├── .daml/dist/package-Model-1.0.0.dar
       ├── daml/...
       └── daml.yaml

With this configuration, running ``daml build`` on ``package-Logic`` also automatically rebuilds ``package-Model-1.0.0.dar`` as a data dependency of ``package-Logic``.

.. code:: bash

   > # ... make changes to Model ...
   > cd ./package-Logic/ # Navigate to package-Logic
   > daml build # Build package Logic
   ...
   Dependency "package-Model" is stale, rebuilding...
   ...
   Building "package-Logic"...
   Done.

With multi-package builds configured, you can run ``daml build`` just once to build interdependent packages, with a guarantee that changes to dependencies are always propagated wherever they are needed.

Build all packages in a project
-------------------------------

To build all packages in a project, use the ``--all`` flag. This
flag builds every package listed in the specified ``multi-package.yaml``. 
With the ``--all`` flag, you can run ``daml build`` from outside a package directory.

.. _multi-package-yaml-location:

How the CLI finds multi-package.yaml
------------------------------------

In most cases, you'll run ``daml build`` and ``daml clean`` from deeper in your project structure than the root. The discovery logic for these commands follows the patterns of ``daml.yaml``:

1. If a path is specified with ``--multi-package-path PATH``, use 
   the ``multi-package.yaml`` at that location. (This is the
   ``multi-package`` equivalent to ``--project-root``.)
2. Otherwise, search up the directory tree starting from the directory where  
   either ``daml build`` or ``daml clean`` was invoked. Return the first 
   ``multi-package.yaml`` encountered.
3. If no ``multi-package.yaml`` file is found in the preceding steps, do 
   not use the multi-build feature.

Add a package to a multi-package configuration
----------------------------------------------

In the example above, the ``multi-package.yaml`` file registers two packages,
Logic and Model:

::

   > tree
   .
   ├── multi-package.yaml
   ├── package-Logic
   │   ├── .daml/dist/package-Logic-1.0.0.dar
   │   ├── daml/...
   │   └── daml.yaml
   └── package-Model
       ├── daml/.dist/package-Model-1.0.0.dar
       ├── daml/...
       └── daml.yaml

To add a new package called Tests to this example structure, run 
``daml new --template=empty-skeleton package-Tests``. This command creates an
empty package named ``package-Tests``, parallel to the existing
``package-Logic`` and ``package-Model`` packages.

::

   > tree
   .
   ├── multi-package.yaml
   ├── package-Logic
   ├── package-Model
   └── package-Tests

In the newly created ``package-Tests/daml.yaml``, remove the
``sandbox-options`` and add a dependency on the DAR for ``Logic``:

.. code:: bash

   data-dependencies:
   - ../package-Logic/.daml/dist/package-Logic-1.0.0.dar

Finally, add ``package-Testing`` to the ``multi-package.yaml`` file:

``multi-package.yaml``

.. code:: diff

     packages:
     - ./package-Logic
     - ./package-Model
   + - ./package-Tests

Run multi-package tests
^^^^^^^^^^^^^^^^^^^^^^^

There are two ways to build the new Tests package in this example:

-  Run ``daml build`` from the ``package-Testing`` directory to build
   the Tests package and its dependency package, Logic.
-  Run ``daml build --all`` from the root of the project to build all
   three packages in the project, including the Tests package.

After building the Tests package, you can run ``daml test`` from the
``package-Testing`` directory to run up-to-date tests.

.. _excluded-packages:

Packages not listed in multi-package.yaml
-----------------------------------------

Packages that are not included in the ``multi-package.yaml`` file are not
automatically recompiled, even if their source is available in the project's
source tree.

For example, a project's source tree might include a vendor library that should be built in isolation, even though the ``vendor-library.dar`` is a data dependency of
``package-Logic``.

::

   > tree
   .
   ├── multi-package.yaml
   ├── package-Logic
   │   ├── daml/...
   │   └── daml.yaml
   ├── package-Model
   │   ├── daml/...
   │   ├── daml/.dist/package-Model-1.0.0.dar
   │   └── daml.yaml
   └── vendor-library
       ├── daml/...
       ├── daml/.dist/vendor-library-1.0.0.dar
       └── daml.yaml

As long as ``vendor-library`` is not included in ``multi-package.yaml``, builds
of package Logic will **not** automatically rebuild ``vendor-library.dar``, even though its source and ``vendor-library/daml.yaml`` are available to the project. In other
words, the ``multi-package.yaml`` configuration -- not the project directory 
structure -- controls the multi-package build feature.

.. code:: bash

   > # ... make changes to vendor-library ...
   > cd ./package-Logic/ # Navigate to package-Logic
   > daml build # Build package Logic
   ...
   Building "package-Logic"... # vendor-library is not rebuilt
   Done.

In this way, ``multi-package.yaml`` provides a way to exclude packages and avoid recompiling a vendor package or other package outside the project owner's control.

Multiple ``multi-package.yaml`` files
=========================================================

Some projects use a nested structure. For example, you might have two
separate GitHub repositories, ``application`` and ``library``, in which
you regularly change the source. The application repository depends on the 
library repository via ``daml.yaml``, which points at DAR files 
within the library repository.

::

   .
   ├── application-repository
   │   ├── .git
   │   ├── multi-package.yaml
   │   ├── application-logic
   │   │   └── daml.yaml
   │   └── application-tests
   │       └── daml.yaml
   └── library-repository
       ├── .git
       ├── multi-package.yaml
       ├── library-logic
       │   └── daml.yaml
       └── library-tests
           └── daml.yaml

``application-repository/application-logic/daml.yaml``

.. code:: yaml

   version: 1.0.0
   ...
   data-dependencies:
   - ../../library-respository/library-logic/.daml/dist/library-logic-1.0.0.daml

Each repository has its own ``multi-package.yaml`` that points to
the respective logic and tests packages, so that you can work
effectively in each repository on its own.

.. code:: bash

   > cd library-repository
   > daml build --all
   ...
   Building "library-logic"...
   Building "library-tests"...
   Done.
   > cd application-repository
   > daml build --all
   ...
   Building "application-logic"...
   Building "application-tests"...
   Done.

But occasionally you might want to make changes to both repositories
simultaneously. In this example, neither repository is aware
of the other, so builds run from within the application repository will
*not* rebuild dependencies within the library repository.

.. code:: bash

   > cd library-repository
   > editor library-logic/... # make some changes
   > cd ../application-repository
   > daml build --all
   Nothing to do. # changes from library-logic are not picked up and not rebuilt

In cases like this, you can use the ``projects`` field of ``multi-package.yaml``
to include external ``multi-package.yaml`` files in the build.

``application-repository/multi-package.yaml``

.. code:: yaml

   packages:
   - ./application-logic
   - ./application-tests
   # Add the path to library-repository, which includes a multi-package.yaml file
   projects:
   - ../library-repository

With this configuration, all dependencies in the external ``multi-package.yaml``
are included in multi-package builds local to the project.

.. code:: bash

   > cd library-repository
   > editor library-logic/... # make changes
   > cd ../application-repository
   > daml build --all
   Building "library-logic"... # changes from library-logic *are* picked up and rebuilt
   Building "application-logic" # application-logic is rebuilt because its library-logic dependency has changed

With the ``projects:`` field, a project can be composed of many
``multi-package.yaml`` files. Make sure your build command refers to the right 
``multi-package.yaml`` for your use case.

Nested projects
---------------

The following example explores a nested project structure. The top-level 
``main`` package has a ``multi-package.yaml`` file and a ``libs`` subdirectory. 
The ``libs`` subdirectory has its own ``libs/multi-package.yaml`` file and 
contains two packages, ``libs/my-lib`` and ``libs/my-lib-helper``.

The ``main`` package depends on ``my-lib``, which itself depends on
``my-lib-helper``:

::

   > tree
   .
   ├── multi-package.yaml
   ├── libs
   │   ├── multi-package.yaml
   │   ├── my-lib
   │   │   ├── daml
   │   │   │   └── MyLib.daml
   │   │   └── daml.yaml
   │   └── my-lib-helper
   │       ├── daml
   │       │   └── MyLibHelper.daml
   │       └── daml.yaml
   └── main
       ├── daml
       │   └── Main.daml
       └── daml.yaml

Here are the key files:

-  ``libs/multi-package.yaml``

   .. code:: yaml

      packages:
      - ./my-lib
      - ./my-lib-helper

-  ``multi-package.yaml``

   .. code:: yaml

      packages:
      - ./main
      projects:
      - ./libs

-  ``main/daml.yaml``

   .. code:: yaml

      version: 1.0.0
      ...
      data-dependencies:
      - ../libs/my-lib/.daml/dist/my-lib-1.0.0.dar # main depends on my-lib

Running ``daml build --all`` from the root of the project builds all libraries 
and ``main``:

.. code:: bash

   > # From the root of the project:
   > daml build --all
   Building "my-lib-helper"...
   Building "my-lib"...
   Building "main"...

But in this example, if you run ``daml build --all`` from the ``libs`` directory,
the CLI traverses the directory tree and encounters ``libs/multi-package.yaml``
first. Because ``libs/multi-package.yaml`` only refers to ``my-lib`` and ``my-lib-helper``, only those packages are built.

.. code:: bash

   > cd libs/
   > daml build --all
   Building "my-lib-helper"...
   Building "my-lib"...
   # Main is *not* built, because libs/multi-package.yaml was used

To use the outer ``multi-package.yaml`` from within ``libs``, add the 
``--multi-package-path`` flag:

.. code:: bash

   > cd libs/
   > daml build --all --multi-package-path ../multi-package.yaml
   Building "my-lib-helper"...
   Building "my-lib"...
   Building "main"... # Main *is* built, because the root multi-package.yaml was used

.. _output-flag:

The ``--output`` flag
---------------------

The ``daml build`` command has an optional ``--output`` flag, which 
sets the location of the generated DAR file. In multi-package builds,
the ``--output`` flag applies if specified in the relevant package's ``daml.yaml`` ``build-options``:

   ::

      build-options:
      - --output=./my-dar.dar

.. _caching:

Caching
=======

Multi-package builds can include many packages and a whole project. For 
efficiency, the ``daml build`` command caches package results and
avoids rebuilds when possible. ``daml build`` performs two checks on 
generated artifacts to ensure they are up-to-date:

-  Compare the contents of all Daml source files against those compiled
   into the DAR.
-  Compare the package IDs of all dependencies against the package
   IDs of dependencies compiled into the DAR.


To turn off caching for a build, use ``--no-cache``. This flag forces 
rebuilding of all relevant packages.

Clean the cache
-------------------------------------

To clear out project build artifacts you no longer need, use ``daml clean``:

-  ``daml clean`` clears artifacts for the current package and
   its dependencies
-  ``daml-clean --all`` clears artifacts for the entire project

When a ``multi-package.yaml`` file is in place, the ``--all``
flag clears the build artifacts of all packages in the
project.

Multi-Package Builds and Upgrades
=================================

Multi-package builds can make testing and developing upgrades much easier. For
more on this, consult the the documentation for :ref:`multi-package builds in upgrades <upgrades-multi-package>`.
