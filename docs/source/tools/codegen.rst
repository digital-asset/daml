.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML codegen
############

Introduction
============

You can use the DAML codegen to generate Java and Scala classes representing DAML contract templates. These classes incorporate all boilerplate code for constructing corresponding ledger :ref:`com.daml.ledger.api.v1.CreateCommand` and :ref:`com.daml.ledger.api.v1.ExerciseCommand`.

Running the DAML codegen
========================

The basic command to run the DAML codegen is::

  $ daml codegen [java|scala] [options]

There are two modes:

- command line configuration, specifying **all** settings in the command line

- project file configuration, specifying **all** settings in the ``daml.yaml``

Command line configuration
--------------------------

Help for **DAML to Java** codegen::

  $ daml codegen java --help

Help for **DAML to Scala** codegen::

  $ daml codegen scala --help

Both **DAML to Java** and **DAML to Scala** take the same set of configuration settings::

    Usage: codegen [options] <DAR-file[=package-prefix]>...

    Code generator for the DAML ledger bindings.

      <DAR-file[=package-prefix]>...
                               DAR file to use as input of the codegen with an optional, but recommend, package prefix for the generated sources.
      -o, --output-directory <value>
                               Output directory for the generated sources
      -d, --decoderClass <value>
                               Fully Qualified Class Name of the optional Decoder utility
      -V, --verbosity <value>  Verbosity between 0 (only show errors) and 4 (show all messages) -- defaults to 0
      -r, --root <value>       Regular expression for fully-qualified names of templates to generate -- defaults to .*
      --help                   This help text

Project file configuration
--------------------------

The above settings can be configured in the ``codegen`` element of the DAML project file ``daml.yaml``. Here is an example::

    sdk-version: 0.0.0
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
      java:
        package-prefix: com.daml.quickstart.iou
        output-directory: java-codegen/src/main/java
        verbosity: 2
      scala:
        package-prefix: com.daml.quickstart.iou
        output-directory: scala-codegen/src/main/scala
        verbosity: 2

You can run the above configuration to generate **Java** code::

    $ daml codegen java

and to generate **Scala** code::

    $ daml codegen scala

The equivalent **DAML to Java** command line configuration::

    $ daml codegen java ./.daml/dist/quickstart-0.0.1.dar=com.daml.quickstart.iou --output-directory=java-codegen/src/main/java --verbosity=2

and **DAML to Scala** command line configuration::

    $ daml codegen scala ./.daml/dist/quickstart-0.0.1.dar=com.daml.quickstart.iou --output-directory=scala-codegen/src/main/scala --verbosity=2

In order to compile the resulting **Scala** or **Java** classes, you need to
add the corresponding dependencies to your build tools. For **Scala**, you can
depend on::

    "com.daml" %% "bindings-scala" % 1.0.1

and for **Java**, add the following **Maven** dependency::

    <dependency>
      <groupId>com.daml</groupId>
      <artifactId>bindings-java</artifactId>
      <version>1.0.1</version>
    </dependency>

