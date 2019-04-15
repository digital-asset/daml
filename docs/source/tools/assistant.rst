.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

SDK Assistant
#############

The SDK Assistant is a command-line tool designed to help you interact with the DAML SDK. It includes commands to help you create projects, run other SDK tools, install and upgrade SDK releases, and view documentation.

Installing the SDK Assistant
****************************

Refer to :doc:`/getting-started/installation` for instructions on how to install the SDK Assistant.

All general SDK Assistant files are stored in ``~/.da/``. This folder contains the ``da`` binary itself, a global
SDK configuration file called ``da.yaml``, and the downloaded SDK releases and
related packages. On installation, the SDK Assistant will attempt to set up a
symlink in ``/usr/local/bin``.

.. CARL: Do we want to add info on what happens if the symlink is not set up?

Understanding the SDK Assistant
*******************************

The SDK Assistant is designed to make DAML development as easy and
enjoyable as possible by helping with two key tasks:

* **Initializing DAML SDK projects so you can use development tools** for project-specific application code. All SDK development should be confined within a DAML SDK project.

* **Managing DAML SDK releases from Digital Asset** by periodically checking for updates. When an update is available, it is automatically downloaded and installed, keeping your SDK instance up to date. SDK releases contain development tools and libraries -- the DAML compiler, the DAML Studio IDE, and a DAML ledger, for example.

.. note:: Because the SDK Assistant controls SDK releases and related tools, it can manage several versions of a tool and ensure that it uses the version compatible with the active project and other running tools. See :ref:`assistant-manual-managing-releases`.

Using the SDK Assistant
***********************

The SDK Assistant is invoked with the ``da`` command in a terminal. If you do not
add any arguments to the command, it will default to display a status message.

Use the ``da --help`` command to display a list of valid options and commands::

  da --help

  SDK Assistant - Version

  Usage: da [-c|--config FILENAME] [-l|--log-level debug|info|warn|error]
            [--script] [--term-width ARG] [COMMAND]
    SDK Assistant. Use --help for help.

  Available options:
    -h,--help                Show this help text
    -c,--config FILENAME     Specify what config file to use.
    -l,--log-level debug|info|warn|error
                             Set the log level. Default is 'warn'.
    --script                 Script mode -- skip auto-upgrades and similar.
    --term-width ARG         Rendering width of the terminal.
    -v,--version             Show version and exit

  Available commands:
    status                   Show SDK environment overview
    docs                     Show SDK documentation
    new                      Create a new project from template
    add                      Add a template to the current project
    project                  Manage DAML SDK projects
    template                 Manage DAML SDK templates
    upgrade                  Upgrade to latest SDK version
    list                     List installed SDK versions
    use                      Set the default SDK version, downloading it if
                             necessary
    uninstall                Remove DAML SDK versions or the complete DAML SDK
    start                    Start a given service
    restart                  Restart a given service
    stop                     Stop a given service
    feedback                 Send us feedback!
    studio                   Start DAML Studio in the current project
    navigator                Start Navigator (also runs Sandbox if needed)
    sandbox                  Start Sandbox process in current project
    compile                  Compile a DAML project into a DAR package
    path                     Show the filesystem path of an SDK component
    run                      Run the main executable of a package
    setup                    Set up SDK environment (e.g. on install or upgrade)
    subscribe                Subscribe for a namespace (of the template
                             repository).
    unsubscribe              Unsubscribe from a namespace (of the template
                             repository).
    config-help              Show config-file help
    config                   Query and manage configuration
    changelog                Show the changelog of an SDK version
    update-info              Show SDK Assistant update channel information

To get help for a particular command, use this command::

  da <command> --help

**Example**::

  da new --help

  Usage: da new PROJECT_PATH
    Create a new project

  Available options:
    -h,--help                Show this help text
    PROJECT_PATH             Path to the new project. Name of the last folder will
                             be the name of the new project.

Developing with the SDK Assistant
*********************************

To begin, create a new DAML SDK project using the SDK Assistant. A project consists
of a folder with a valid ``da.yaml`` file that, among other things, specifies
which SDK release is being used. This is important because the release determines which
versions of DAML and the DAML Sandbox the project code uses.

In the SDK
Assistant, create a DA project with this command::

  da new <project_path>

**Example**::

  da new my-project

This example creates a project folder called ``my-project`` folder that is seeded with a basic folder structure.

Change to the new project directory.

**Example**::

  cd my-project

In the project folder, use SDK Assistant functions. For example, if you have Visual Studio Code installed, open DAML Studio for your project using::

  da studio

Use the following command to start the DAML Sandbox and the Navigator against
the current project's DAML code::

  da start

These services will run in the background. To see a list of running services, use this command::

  da status

To stop any running services of the current project::

  da stop

To restart::

  da restart

The current SDK functionality is still basic. Future releases will improve and extend the current functionality and features to help you develop
automation, UIs, and other types of functionality on top of your DAML
application.

.. _assistant-manual-building-dars:

Building DAML archives
**********************

The SDK Assistant can compile your DAML source code into a DAML archive (a
``.dar`` file). To do this, run::

  da compile

This will compile the source code file set in your ``da.yaml``
configuration file (see :ref:`assistant-manual-configuring-compilation`) and
generate a ``.dar`` file in the directory ``target`` with the same name as your
project.

.. _assistant-manual-managing-releases:

Managing SDK releases
*********************

The SDK Assistant will automatically check for updates and upgrade itself if there is
a new version. This means that you will always have the latest version.

If you are working on several
projects that have code that assumes different tool and component versions, you will need to have
several SDK releases installed at the same time. Having different SDK releases available ensures all your projects will work without your having to
upgrade all of them to use the
latest libraries and tools. The SDK Assistant has commands for managing several SDK releases.

- The *active* release is the one that is currently in use. For example, each DAML SDK project specifies the SDK release to use. When you
  are in a project, the specified release is the active one.

- The *default* release is the one that will be used when you start a new
  project or in other cases when an SDK release is needed and you are not in a
  project.

To list all installed SDK releases and see which are active and which is the default, use this command::

  da list

To change the default release::

  da use <release-version>

To upgrade to the latest SDK release manually::

  da upgrade

To remove unused old releases::

  da uninstall <release-version>

.. _cli-managing-templates:

Managing SDK templates
**********************

You can use the SDK Assistant to subscribe to templates, which are stored under a specific namespace. Often these are DAML templates, but not all of them are. There are two types of template:

- Project templates, which are the starting point for a full project. These give you a folder with its own ``da.yaml`` file.
- Add-on templates, which are the starting point for a sub-project. These give you a folder that lives inside a DAML SDK project folder. Add-on templates are not always DAML templates.

The Assistant comes with several DAML templates that are pre-made example modules, which show advanced uses of DAML and provide useful library functions.

To list available project templates, run::

  da new

To list available add-on templates, run::

  da add

.. note:: ``da new`` and ``da add`` may show undocumented or unsupported templates.

To subscribe to a namespace (so you can access the template inside it), run::

  da subscribe <namespace>

To unsubscribe (remove from the list of namespaces which are checked if the listing commands are executed), run::

  da unsubscribe <namespace>

Once you've subscribed to a project template's namespace, you can start a new project from the project template::

  da new <namespace>/<project-template-name> <project-path>

Once you've subscribed to an add-on template's namespace, you can add the add-on template to your project::

  da add <namespace>/<add-on-template-name> <target-folder>

For DAML templates, the new DAML files are added to your project inside the ``daml`` folder. To use them in your project, you need to add references to these files in the ``daml/Main.daml`` file with ``import`` statements. This is not done automatically.

Running SDK tools
*****************

The SDK also comes with some packages that you can run and that can serve as important tools in the development of the project. You can run an executable package with the following command::

  da run <package> [-- ARGS]

If you run the command above without the package argument, it will display a message saying that the package is missing and will show a list of all the available packages. The available packages currently are ``damlc`` and ``navigator``.

The ``damlc`` package is a DAML compiler tool. The compiler functionality is currently intended only for internal use, so it is not described in this documentation.

The ``navigator`` package contains a browser-based tool that can also be used to interact with the ledger and is described in :doc:`/tools/navigator/index`.

.. _da-yaml-configuration:

da.yaml configuration
*********************

The SDK Assistant uses a configuration file -- ``da.yaml``-- that includes both global configuration and project configuration information. The SDK
Assistant global ``da.yaml`` file is located in the SDK Assistant home folder
(``~/.da/``). Project configuration in this file is ignored. Each project also
has a ``da.yaml`` file, which specifies project configuration and can also
override global configuration properties. To list the properties that can be specified in the configuration file, use this command::

  da config-help

Configuring parties
===================

Some tools, like the Navigator, require parties to be configured before it is started.
When using the SDK Assistant to start such services, configure parties in
the ``da.yaml`` file for the project. The ``project.parties`` property takes a list of
strings. For example::

  ...
  project:
    sdk-version: '0.6.0'
    scenario: Main:example
    name: my-project
    source: daml/Main.daml
    parties:
    - OPERATOR
    - BANK1
    - BANK2
    - '007'

This can also be formatted in YAML as::

  ...
  project:
    sdk-version: '0.6.0'
    scenario: Main:example
    name: my-project
    source: daml/Main.daml
    parties: ["OPERATOR", "BANK1", "BANK2", "007"]

.. _assistant-manual-configuring-compilation:

Configuring compilation
=======================

DAML compilation is configured with the following ``project`` variables:

``project.name``
  The name of the project.

``project.source``
  The path to the source code.

``project.output-path``
  The directory to store generated ``.dar`` files, the default is ``target``.

The generated ``.dar`` file will be stored in
``${project.output-path}/${project.name}.dar``.


Uninstalling DAML SDK
*********************

If for any reason the DAML SDK needs to be removed use::

  da uninstall all

This command will ask for confirmation and remove the complete DAML SDK home folder (``~/.da/``) and the ``da`` symlink in ``/usr/local/bin`` created upon installation.
