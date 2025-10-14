.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. warning::
   Daml Assistant is deprecated from 3.4.x onwards. Use :externalref:`dpm <dpm>` instead.
   Refer to the ``dpm`` :externalref:`migration guide <dpm-daml-assistant-to-dpm-migration>` for more details.

.. _daml-assistant-config-files:

Daml Assistant configuration files
##################################

Package Configuration file (daml.yaml)
**************************************
.. _daml-yaml-configuration:

The `daml.yaml` defines the following properties for a package:

- | ``sdk-version``: (required) The SDK version that this project uses.
  | The assistant will warn you when it is time to update this setting (see the ``update-check`` setting in the global config to control how often it checks, or to disable this check entirely).
- ``name``: (required) The name of the project. This is used to manage automatic upgrades between multiple versions of a package. See :ref:`Smart Contract Upgrades <smart-contract-upgrades>`.
- ``source``: (required) The root directory of your Daml source code files relative to the project root.
- ``init-script``: (optional) The name of a Daml script to run when using ``daml start``, of the form ``Module:name``.
- ``version``: (required) The project version.
- ``exposed-modules``: (optional) The Daml modules that are exposed by this project, which can be imported in other projects.
  If this field is not specified all modules in the project are exposed.
- ``dependencies``: (required) library-dependencies of this project. This should contain at least `daml-prim` and `daml-stdlib`. You may also wish to depend on `daml-script` for testing.
- ``data-dependencies``: (optional) Cross-SDK dependencies of this project, as a list of .dar file paths.

  .. _daml-yaml-module-prefixes:

- 
  ``module-prefixes``: (optional) Module import prefixes, for module name collisions.
  This is a mapping from ``<package-name>-<package-version>`` to a module prefix string. For example

  .. code:: yaml

    module-prefixes:
      my-package-1.0.0: V1
  
  The above prefix would allow you to import modules from `my-package` with the prefix `V1`, as follows:

  .. code:: daml

    import V1.Main

  This is useful for multi-version testing, see :ref:`Smart Contract Upgrades <smart-contract-upgrades>`.

  .. _daml-yaml-build-options:

- 
  ``build-options``: (optional) A list of flag for ``daml build`` (See :ref:`Daml Build Flags <daml-build-flags>`) that will be applied to any action that involves the Compiler.
  This includes ``build``, ``test``, ``damlc docs`` and ``damlc lint``.
  Included in these flags are controls for warnings, see :ref:`Recommended Build Options <recommended-build-options>` for our recommended set of warnings to enable.
- ``sandbox-options``: (optional) A list of options that will be passed to Sandbox in ``daml start``.
- ``script-options``: (optional) A list of options that will be passed to the Daml script
  runner when running the ``init-script`` as part of ``daml start``.

The ``daml.yaml`` file supports :ref:`Environment Variable Interpolation <environment-variable-interpolation>`.


Multi-package Configuration file (multi-package.yaml)
*****************************************************
The ``multi-package.yaml`` file is used to inform Daml Build and the IDE of projects containing multiple
connected Daml packages.

An example is given below:

.. code-block:: yaml

  packages:
    - ./path/to/package/a
    - ./path/to/package/b
  projects:
    - ./path/to/project/a
    - ./path/to/project/b

Here is what each field means:

- ``packages``: an optional list of directories containing Daml packages, and by extension, ``daml.yaml`` config files. These allow Daml Multi-Build to
  find the source code for dependency DARs and build them in topological order.
- ``projects``: an optional list of directories containing ``multi-package.yaml`` config files, which will extend the above package set for resolving
  the build order. These exist to allow separation of your project into sub-projects which can still be built when removed from the surrounding environment.

The multi-package also includes a ``dars`` field, for providing additional information to Daml Studio.
See :ref:`Daml Studio Jump to definition <daml-studio-jump-to-def>` for more details.

The ``multi-package.yaml`` file supports :ref:`Environment Variable Interpolation <environment-variable-interpolation>`.

Environment Variable Interpolation
**********************************
.. _environment-variable-interpolation:

Both the ``daml.yaml`` and ``multi-package.yaml`` config files support environment variable interpolation on all string fields.
Interpolation takes the form of ``${MY_ENVIRONMENT_VARIABLE}``, which is replaced with the content of ``MY_ENVIRONMENT_VARIABLE`` from the
calling shell. These can be escaped and placed within strings according to the environment variable interpolation semantics.

This allows you to extract common data, such as the sdk-version, package-name, or package-version outside of a package's ``daml.yaml``. For example,
you can use an ``.envrc`` file or have these values provided by a build system. This feature can also be used for specifying dependency DARs, enabling you to either store
your DARs in a common folder and pass its directory as a variable, shortening the paths in your ``daml.yaml``, or pass each dependency as a
separate variable through an external build system, which may store them in a temporary cache.

The following example showcases this:

.. code-block:: yaml

  sdk-version: ${SDK_VERSION}
  name: ${PROJECT_NAME}_test
  source: daml
  version: ${PROJECT_VERSION}
  dependencies:
    // Using a common directory
    ${DEPENDENCY_DIRECTORY}/my-dependency-1.0.0.dar
    ${DEPENDENCY_DIRECTORY}/my-other-dependency-1.0.0.dar
    // Passed directly by a build system
    ${DAML_FINANCE_DAR}
    ${MY_DEPENDENCY_DAR}

Escape syntax uses the ``\`` prefix: ``\${NOT_INTERPOLATED}``, and interpolation can be disallowed for a config file
by setting the ``environment-variable-interpolation`` field to ``false``.

.. code-block:: yaml

  name: ${NOT_INTERPOLATED}
  environment-variable-interpolation: false

Note that environment variables are case sensitive, meaning ``${MY_VAR}`` and ``${My_Var}`` do not reference the same variable.

Recommended ``build-options``
*****************************
.. _recommended-build-options:

The default set of warnings enabled by the Daml compiler is fairly conservative.
This is to avoid overwhelming new users with many warnings.
Here are ``build-options`` you might declare in a project's ``daml.yaml`` for a
stricter set of warnings.

.. code-block:: yaml

    build-options:
      - --ghc-option=-Wunused-top-binds
      - --ghc-option=-Wunused-matches
      - --ghc-option=-Wunused-do-bind
      - --ghc-option=-Wincomplete-uni-patterns
      - --ghc-option=-Wredundant-constraints
      - --ghc-option=-Wmissing-signatures
      - --ghc-option=-Werror

Each option enables a particular warning, except for the last one, ``-Werror``,
which turns every warning into an error; this is especially useful for CI build
arrangements.  Simply remove or comment out any line to disable that category of
warning.  See
`the Daml forum <https://discuss.daml.com/t/making-the-most-out-of-daml-compiler-warnings/739>`__
for a discussion of the meaning of these warnings and pointers to other
available warnings.
