.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _daml-codegen-java:

Generate Java code from DAML
############################

Introduction
============

When writing applications for the ledger in Java, you want to work with a representation of DAML templates and data types in Java that closely resemble the original DAML code while still being as true to the native types in Java as possible. To achieve this, you can use DAML to Java code generator ("Java codegen") to generate Java types based on a DAML model. You can then use these types in your Java code when reading information from and sending data to the ledger.

Download
========

You can download the `latest version <https://bintray.com/api/v1/content/digitalassetsdk/DigitalAssetSDK/com/daml/java/codegen/$latest/codegen-$latest.jar?bt_package=sdk-components>`__  of the Java codegen.

.. _daml-codegen-java-running:

Run the Java codegen
====================

The Java codegen takes DAML archive (DAR) files as input and generates Java files for DAML templates, records, and variants. For information on creating DAR files see :ref:`assistant-manual-building-dars`. To use the Java codegen, run this command in a terminal:

.. code-block:: none
  
  java -jar <path-to-codegen-jar>

Use this command to display the help text:

.. code-block:: none
  
  java -jar codegen.jar --help

Generate Java code from DAR files
---------------------------------

Pass one or more DAR files as arguments to the Java codegen. Use the ``-o`` or ``--output-directory`` parameter for specifying the directory for the generated Java files.

.. code-block:: none
  
  java -jar java-codegen.jar -o target/generated-sources/daml daml/my-project.dar
                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To avoid possible name clashes in the generated Java sources, you should specify a Java package prefix for each input file:

.. code-block:: none
  
  java -jar java-codegen.jar -o target/generated-sources/daml \
      daml/project1.dar=com.example.daml.project1 \
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^        
      daml/project2.dar=com.example.daml.project2
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^

Generate the decoder utility class
----------------------------------

When reading transactions from the ledger, you typically want to convert a `CreatedEvent <https://docs.daml.com/app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/CreatedEvent.html>`__ from the Ledger API to the corresponding generated ``Contract`` class. The Java codegen can optionally generate a decoder class based on the input DAR files that calls the ``fromIdAndRecord`` method of the respective generated ``Contract`` class (see :ref:`daml-codegen-java-templates`). The decoder class can do this for all templates in the input DAR files. 

To generate such a decoder class, provide the command line parameter ``-d`` or ``--decoderClass`` with a fully qualified class name:

.. code-block:: none
  
  java -jar java-codegen.jar -o target/generated-sources/daml \
      -d com.myproject.DamModelDecoder daml/my-project.dar
      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Receive feedback
----------------

By default, the logging is configured so that you'll only see error messages.

If you want to change this behavior, you can ask to receive more extensive feedback using the ``-V`` or ``--verbosity`` command-line option. This option takes a numeric parameter from 0 to 4, where 0 corresponds to the default quiet behavior and 4 represents the most verbose output possible.

In the following example the logging is set to print most of the output with detailed debugging information:

.. code-block:: none

  java -jar java-codegen.jar -o target/generated-sources/daml -V 3
                                                              ^^^^

Integrate with build tools
--------------------------

While we currently don’t provide direct integration with Maven, Groovy, SBT, etc., you can run the Java codegen as described in :ref:`daml-codegen-java-running` just like any other external process (for example the protobuf compiler). Alternatively you can integrate it as a runnable dependency in your ``pom.xml`` file for Maven.

The following snippet is an excerpt from the ``pom.xml`` that is part of the :ref:`quickstart` guide.

  .. literalinclude:: ../../getting-started/quickstart/template-root/pom.xml
    :language: xml
    :lines: 73-105,121-122
    :dedent: 12

Understand the generated Java model
===================================

The Java codegen generates source files in a directory tree under the output directory specified on the command line.

Map DAML primitives to Java types
---------------------------------

DAML built-in types are translated to the following equivalent types in
Java:

+-----------------------------------+---------------------------------------+
| DAML type                         | Java type                             |
+===================================+=======================================+
| ``Int``                           | ``java.lang.Long``                    |
+-----------------------------------+---------------------------------------+
| ``Decimal``                       | ``java.math.BigDecimal``              |
+-----------------------------------+---------------------------------------+
| ``Text``                          | ``java.lang.String``                  |
+-----------------------------------+---------------------------------------+
| ``Bool``                          | ``java.util.Boolean``                 |
+-----------------------------------+---------------------------------------+
| ``Party``                         | ``java.lang.String``                  |
+-----------------------------------+---------------------------------------+
| ``Date``                          | ``java.time.LocalDate``               |
+-----------------------------------+---------------------------------------+
| ``Time``                          | ``java.time.Instant``                 |
+-----------------------------------+---------------------------------------+
| ``List`` or ``[]``                | ``java.util.List``                    |
+-----------------------------------+---------------------------------------+
| ``Optional``                      | ``java.util.Optional``                |
+-----------------------------------+---------------------------------------+
| ``()`` (Unit)                     | Since Java doesn’t have an            |
|                                   | equivalent of DAML’s Unit type        |
|                                   | ``()`` in the standard library,       |
|                                   | the generated code uses               |
|                                   | `com.daml.ledger.javaapi.data.Unit`_  |
|                                   | from the Java Bindings library.       |
+-----------------------------------+---------------------------------------+
| ``ContractId``                    | Fields of type ``ContractId X`` refer |
|                                   | to the generated ``ContractId`` class |
|                                   | of the respective template ``X``.     |
+-----------------------------------+---------------------------------------+

.. _com.daml.ledger.javaapi.data.Unit: https://docs.daml.com/app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Unit.html

Understand the generated classes
--------------------------------

Every user-defined data type in DAML (template, record, and variant) is represented by one or more Java classes as described in this section.

The Java package for the generated classes is the equivalent of the lowercase DAML module name.

.. code-block:: daml
  :caption: DAML

  module Foo.Bar.Baz where

.. code-block:: java
  :caption: Java

  package foo.bar.baz;

Records (a.k.a product types)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A :ref:`DAML record <daml-ref-record-types>` is represented by a Java class with fields that have the same name as the DAML record fields. A DAML field having the type of another record is represented as a field having the type of the generated class for that record.

.. code-block:: daml
  :caption: Com/Acme.daml

  daml 1.2
  module Com.Acme where

  data Person = Person with name : Name; age : Decimal
  data Name = Name with firstName : Text; lastName : Text

A Java file is generated that defines the class for the type ``Person``:

.. code-block:: java
  :caption: com/acme/Person.java
  
  package com.acme;

  public class Person {
    public final Name name;
    public final BigDecimal age;

    public static Person fromValue(Value value$) { /* ... */ }

    public Person(Name name, BigDecimal age) { /* ... */ }
    public Record toValue() { /* ... */ }
  }

A Java file is generated that defines the class for the type ``Name``:

  .. code-block:: java
    :caption: com/acme/Name.java

    package com.acme;

    public class Name {
      public final String fistName;
      public final String lastName;

      public static Person fromValue(Value value$) { /* ... */ }

      public Name(String fistName, String lastName) { /* ... */ }
      public Record toValue() { /* ... */ }
    }

.. _daml-codegen-java-templates:

Templates
~~~~~~~~~

The Java codegen generates three classes for a DAML template:

  **TemplateName**
      Represents the contract data or the template fields.

  **TemplateName.ContractId**
      Used whenever a contract ID of the corresponding template is used in another template or record, for example: ``data Foo = Foo (ContractId Bar)``. This class also provides methods to generate an ``ExerciseCommand`` for each choice that can be sent to the ledger with the Java Bindings.
      .. TODO: refer to another section explaining exactly that, when we have it.

  **TemplateName.Contract**
      Represents an actual contract on the ledger. It contains a field for the contract ID (of type ``TemplateName.ContractId``) and a field for the template data (of type ``TemplateName``). With the static method ``TemplateName.Contract.fromIdAndRecord``, you can deserialize a `CreatedEvent <https://docs.daml.com/app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/CreatedEvent.html>`__ to an instance of ``TemplateName.Contract``.


  .. code-block:: daml
    :caption: Com/Acme.daml

    daml 1.2
    module Com.Acme where

    template Bar
      with
        owner: Party
        name: Text

    controller owner can
      Bar_SomeChoice: (Bool)
        with
          aName: Text
        do return True

A file is generated that defines three Java classes:

#. ``Bar``
#. ``Bar.ContractId``
#. ``Bar.Contract``

.. code-block:: java
  :caption: com/acme/Bar.java
  :emphasize-lines: 3,10,20

  package com.acme;

  public class Bar extends Template {

    public static final Identifier TEMPLATE_ID = new Identifier("some-package-id", "Com.Acme", "Bar");

    public final String owner;
    public final String name;

    public static class ContractId {
      public final String contractId;

      public ExerciseCommand exerciseArchive(Unit arg) { /* ... */ }

      public ExerciseCommand exerciseBar_SomeChoice(Bar_SomeChoice arg) { /* ... */ }

      public ExerciseCommand exerciseBar_SomeChoice(String aName) { /* ... */ }
    }

    public static class Contract {
      public final ContractId id;
      public final Bar data;

      public static Contract fromIdAndRecord(String contractId, Record record) { /* ... */ }
    }
  }

Variants (a.k.a sum types)
~~~~~~~~~~~~~~~~~~~~~~~~~~

A :ref:`variant or sum type <daml-ref-sum-types>` is a type with multiple constructors, where each constructor wraps a value of another type. The generated code is comprised of an abstract class for the variant type itself and a subclass thereof for each constructor. Classes for variant constructors are similar to classes for records.

.. code-block:: daml
  :caption: Com/Acme.daml

  daml 1.2
  module Com.Acme where

  data BookAttribute = Pages Int
                     | Authors [Text]
                     | Title Text
                     | Published with year: Int; publisher Text

The Java code generated for this variant is:

.. code-block:: java
  :caption: com/acme/BookAttribute.java

  package com.acme;

  public class BookAttribute {
    public static BookAttribute fromValue(Value value) { /* ... */ }

    public static BookAttribute fromValue(Value value) { /* ... */ }
    public Value toValue() { /* ... */ }
  }

.. code-block:: java
  :caption: com/acme/bookattribute/Pages.java

  package com.acme.bookattribute;

  public class Pages extends BookAttribute {
    public final Long longValue;

    public static Pages fromValue(Value value) { /* ... */ }

    public Pages(Long longValue) { /* ... */ }
    public Value toValue() { /* ... */ }
  }

.. code-block:: java
  :caption: com/acme/bookattribute/Authors.java

  package com.acme.bookattribute;

  public class Authors extends BookAttribute {
    public final List<String> listValue;

    public static Authors fromValue(Value value) { /* ... */ }

    public Author(List<String> listValue) { /* ... */ }
    public Value toValue() { /* ... */ }

  }

.. code-block:: java
  :caption: com/acme/bookattribute/Title.java

  package com.acme.bookattribute;

  public class Title extends BookAttribute {
    public final String stringValue;

    public static Title fromValue(Value value) { /* ... */ }

    public Title(String stringValue) { /* ... */ }
    public Value toValue() { /* ... */ }
  }

.. code-block:: java
  :caption: com/acme/bookattribute/Published.java

  package com.acme.bookattribute;

  public class Published extends BookAttribute {
    public final Long year;
    public final String publisher;

    public static Published fromValue(Value value) { /* ... */ }

    public Published(Long year, String publisher) { /* ... */ }
    public Record toValue() { /* ... */ }
  }
