.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML codegen
############

Introduction
============

You can use the DAML codegen to generate Java, Scala, and JavaScript/TypeScript classes representing DAML contract templates.
These classes incorporate all boilerplate code for constructing corresponding ledger ``com.daml.ledger.api.v1.CreateCommand``,
``com.daml.ledger.api.v1.ExerciseCommand``, ``com.daml.ledger.api.v1.ExerciseByKeyCommand``, and ``com.daml.ledger.api.v1.CreateAndExerciseCommand``.

Running the DAML codegen
========================

The basic command to run the DAML codegen is::

  $ daml codegen [java|scala|js] [options]

There are two modes:

- Command line configuration, specifying **all** settings in the command line (all codegens supported)

- Project file configuration, specifying **all** settings in the ``daml.yaml`` (currently **Java** and **Scala** only)

Command line configuration
--------------------------

Help for each specific codegen::

  $ daml codegen [java|scala|js] --help

**Java** and **Scala** codegens take the same set of configuration settings::

      <DAR-file[=package-prefix]>...
                               DAR file to use as input of the codegen with an optional, but recommend, package prefix for the generated sources.
      -o, --output-directory <value>
                               Output directory for the generated sources
      -d, --decoderClass <value>
                               Fully Qualified Class Name of the optional Decoder utility
      -V, --verbosity <value>  Verbosity between 0 (only show errors) and 4 (show all messages) -- defaults to 0
      -r, --root <value>       Regular expression for fully-qualified names of templates to generate -- defaults to .*
      --help                   This help text

**JavaScript/TypeScript** codegen takes a different set of configuration settings::

      DAR-FILES                DAR files to generate TypeScript bindings for
      -o DIR                   Output directory for the generated packages
      -s SCOPE                 The NPM scope name for the generated packages;
                              defaults to daml.js
      -h,--help                Show this help text

Project file configuration (Java and Scala)
-------------------------------------------

The above settings can be configured in the ``codegen`` element of the DAML project file
``daml.yaml``.  See `this issue <https://github.com/digital-asset/daml/issues/6355>`_ for status on
this feature.

Here is an example::

    sdk-version: 1.2.0
    name: quickstart
    source: daml
    scenario: Main:setup
    parties:
      - Alice
      - Bob
      - USD_Bank
      - EUR_Bank
    version: 0.0.1
    exposed-modules:
      - Main
    dependencies:
      - daml-prim
      - daml-stdlib
    codegen:
      js:
        output-directory: ui/daml.js
        npm-scope: daml.js
      java:
        package-prefix: com.daml.quickstart.iou
        output-directory: java-codegen/src/main/java
        verbosity: 2
      scala:
        package-prefix: com.daml.quickstart.iou
        output-directory: scala-codegen/src/main/scala
        verbosity: 2

You can then run the above configuration to generate your **Java** or **Scala** code::

    $ daml codegen [js|java|scala]

The equivalent **JavaScript** command line configuration would be::

    $ daml codegen js ./.daml/dist/quickstart-0.0.1.dar -o ui/daml.js -s daml.js

and the equivalent **Java** or **Scala** command line configuration::

    $ daml codegen [java|scala| ./.daml/dist/quickstart-0.0.1.dar=com.daml.quickstart.iou --output-directory=java-codegen/src/main/java --verbosity=2

In order to compile the resulting **Java** or **Scala** classes, you need to
add the corresponding dependencies to your build tools.

For **Scala**, you can depend on::

    "com.daml" %% "bindings-scala" % YOUR_SDK_VERSION

For **Java**, add the following **Maven** dependency::

    <dependency>
      <groupId>com.daml</groupId>
      <artifactId>bindings-java</artifactId>
      <version>YOUR_SDK_VERSION</version>
    </dependency>

.. note::

  Replace ``YOUR_SDK_VERSION`` with the version of your SDK
