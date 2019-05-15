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

You can download the `latest version <https://bintray.com/api/v1/content/digitalassetsdk/DigitalAssetSDK/com/daml/java/codegen/$latest/codegen-$latest.jar?bt_package=sdk-components>`__  of the Java codegen. Make sure that the following versions are aligned:

* the downloaded Java codegen jar file, eg. 10x.y.z
* the dependency to :ref:`bindings-java <daml-codegen-java-compiling>`, eg. 10x.y.z
* the ``sdk-version`` attribute in the :ref:`daml.yaml <daml-yaml-configuration>` file, eg. x.y.z

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

When reading transactions from the ledger, you typically want to convert a `CreatedEvent <https://docs.daml.com/app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/CreatedEvent.html>`__ from the Ledger API to the corresponding generated ``Contract`` class. The Java codegen can optionally generate a decoder class based on the input DAR files that calls the ``fromCreatedEvent`` method of the respective generated ``Contract`` class (see :ref:`daml-codegen-java-templates`). The decoder class can do this for all templates in the input DAR files.

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


.. _daml-codegen-java-compiling:

Compile the generated Java code
===============================

To compile the generated Java code, add the :ref:`Java Bindings <bindings-java-setup-maven>` library with the same version as the Java codegen to the classpath. 

With Maven you can do this by adding a ``dependency`` to the ``pom.xml`` file:

.. code-block:: xml

    <dependency>
        <groupId>com.daml.ledger</groupId>
        <artifactId>bindings-rxjava</artifactId>
        <version>x.y.z</version>
    </dependency>



Understand the generated Java model
===================================

The Java codegen generates source files in a directory tree under the output directory specified on the command line.

.. _daml-codegen-java-primitive-types:

Map DAML primitives to Java types
---------------------------------

DAML built-in types are translated to the following equivalent types in Java:

+--------------------------------+--------------------------------------------+------------------------+
| DAML type                      | Java type                                  | Java Bindings          |
|                                |                                            | Value Type             |
+================================+============================================+========================+
| ``Int``                        | ``java.lang.Long``                         | `Int64`_               |
+--------------------------------+--------------------------------------------+------------------------+
| ``Decimal``                    | ``java.math.BigDecimal``                   | `Decimal`_             |
+--------------------------------+--------------------------------------------+------------------------+
| ``Text``                       | ``java.lang.String``                       | `Text`_                |
+--------------------------------+--------------------------------------------+------------------------+
| ``Bool``                       | ``java.util.Boolean``                      | `Bool`_                |
+--------------------------------+--------------------------------------------+------------------------+
| ``Party``                      | ``java.lang.String``                       | `Party`_               |
+--------------------------------+--------------------------------------------+------------------------+
| ``Date``                       | ``java.time.LocalDate``                    | `Date`_                |
+--------------------------------+--------------------------------------------+------------------------+
| ``Time``                       | ``java.time.Instant``                      | `Timestamp`_           |
+--------------------------------+--------------------------------------------+------------------------+
| ``List`` or ``[]``             | ``java.util.List``                         | `DamlList`_            |
+--------------------------------+--------------------------------------------+------------------------+
| ``TextMap``                    | ``java.util.Map``                          | `TextMap`_             |
|                                | Restricted to using ``String`` keys.       |                        |
+--------------------------------+--------------------------------------------+------------------------+
| ``Optional``                   | ``java.util.Optional``                     | `DamlOptional`_        |
+--------------------------------+--------------------------------------------+------------------------+
| ``()`` (Unit)                  | **None** since the Java language doesn’t   | `Unit`_                |
|                                | have a direct equivalent of DAML’s Unit    |                        |
|                                | type ``()``, the generated code uses the   |                        |
|                                | Java Bindings value type.                  |                        |
+--------------------------------+--------------------------------------------+------------------------+
| ``ContractId``                 | Fields of type ``ContractId X`` refer to   | `ContractId`_          |
|                                | the generated ``ContractId`` class of the  |                        |
|                                | respective template ``X``.                 |                        |
+--------------------------------+--------------------------------------------+------------------------+


Understand escaping rules
-------------------------

To avoid clashes with Java keywords, the Java codegen applies escaping rules to the following DAML identifiers:

* Type names (except the already mapped :ref:`built-in types <daml-codegen-java-primitive-types>`)
* Constructor names
* Type parameters
* Module names
* Field names

If any of these identifiers match one of the `Java reserved keywords <https://docs.oracle.com/javase/specs/jls/se12/html/jls-3.html#jls-3.9>`__, the Java codegen appends a dollar sign ``$`` to the name. For example, a field with the name ``import`` will be generated as a Java field with the name ``import$``.

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A :ref:`DAML record <daml-ref-record-types>` is represented by a Java class with fields that have the same name as the DAML record fields. A DAML field having the type of another record is represented as a field having the type of the generated class for that record.

.. literalinclude:: ./code-snippets/ProductTypes.daml
   :language: daml
   :start-after: -- start snippet: product types example
   :end-before: -- end snippet: product types example
   :caption: Com/Acme.daml

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
^^^^^^^^^

The Java codegen generates three classes for a DAML template:

  **TemplateName**
      Represents the contract data or the template fields.

  **TemplateName.ContractId**
      Used whenever a contract ID of the corresponding template is used in another template or record, for example: ``data Foo = Foo (ContractId Bar)``. This class also provides methods to generate an ``ExerciseCommand`` for each choice that can be sent to the ledger with the Java Bindings.
      .. TODO: refer to another section explaining exactly that, when we have it.

  **TemplateName.Contract**
      Represents an actual contract on the ledger. It contains a field for the contract ID (of type ``TemplateName.ContractId``) and a field for the template data (of type ``TemplateName``). With the static method ``TemplateName.Contract.fromCreatedEvent``, you can deserialize a `CreatedEvent <https://docs.daml.com/app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/CreatedEvent.html>`__ to an instance of ``TemplateName.Contract``.


  .. literalinclude:: ./code-snippets/Templates.daml
     :language: daml
     :start-after: -- start snippet: template example
     :end-before: -- end snippet: template example
     :caption: Com/Acme.daml

A file is generated that defines three Java classes:

#. ``Bar``
#. ``Bar.ContractId``
#. ``Bar.Contract``

.. code-block:: java
  :caption: com/acme/Bar.java
  :emphasize-lines: 3,14,24

  package com.acme;

  public class Bar extends Template {

    public static final Identifier TEMPLATE_ID = new Identifier("some-package-id", "Com.Acme", "Bar");

    public final String owner;
    public final String name;

    public CreateAndExerciseCommand createAndExerciseBar_SomeChoice(Bar_SomeChoice arg) { /* ... */ }

    public CreateAndExerciseCommand createAndExerciseBar_SomeChoice(String aName) { /* ... */ }

    public static class ContractId {
      public final String contractId;

      public ExerciseCommand exerciseArchive(Unit arg) { /* ... */ }

      public ExerciseCommand exerciseBar_SomeChoice(Bar_SomeChoice arg) { /* ... */ }

      public ExerciseCommand exerciseBar_SomeChoice(String aName) { /* ... */ }
    }

    public static class Contract {
      public final ContractId id;
      public final Bar data;

      public static Contract fromCreatedEvent(CreatedEvent event) { /* ... */ }
    }
  }

Variants (a.k.a sum types)
^^^^^^^^^^^^^^^^^^^^^^^^^^

A :ref:`variant or sum type <daml-ref-sum-types>` is a type with multiple constructors, where each constructor wraps a value of another type. The generated code is comprised of an abstract class for the variant type itself and a subclass thereof for each constructor. Classes for variant constructors are similar to classes for records.

.. literalinclude:: ./code-snippets/Variants.daml
   :language: daml
   :start-after: -- start snippet: variant example
   :end-before: -- end snippet: variant example
   :caption: Com/Acme.daml

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

Parameterized types
^^^^^^^^^^^^^^^^^^^

.. note::

   This section is only included for completeness: we don't expect users to make use of the ``fromValue`` and ``toValue methods``, because they would typically come from a template that doesn't have any unbound type parameters.

The Java codegen uses Java Generic types to represent :ref:`DAML parameterized types <daml-ref-parameterized-types>`.

This DAML fragment defines the parameterized type ``Attribute``, used by the ``BookAttribute`` type for modeling the characteristics of the book:

.. literalinclude:: ./code-snippets/ParameterizedTypes.daml
   :language: daml
   :start-after: -- start snippet: parameterized types example
   :end-before: -- end snippet: parameterized types example
   :caption: Com/Acme.daml

The Java codegen generates a Java file with a generic class for  the ``Attribute a`` data type:

.. code-block:: java
  :caption: com/acme/Attribute.java
  :emphasize-lines: 3,8,10

  package com.acme;

  public class Attribute<a> {
    public final a value;

    public Attribute(a value) { /* ... */  }

    public Record toValue(Function<a, Value> toValuea) { /* ... */ }

    public static <a> Attribute<a> fromValue(Value value$, Function<Value, a> fromValuea) { /* ... */ }
  }

Convert a value of a generated type to a Java Bindings value
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

To convert an instance of the generic type ``Attribute<a>`` to a Java Bindings `Value`_, call the ``toValue`` method and pass a function as the ``toValuea`` argument for converting the field of type ``a`` to the respective Java Bindings `Value`_. The name of the parameter consists of ``toValue`` and the name of the type parameter, in this case ``a``, to form the name ``toValuea``.

Below is a Java fragment that converts an attribute with a ``java.lang.Long`` value to the Java Bindings representation using the *method reference* ``Int64::new``.

.. code-block:: java

  Attribute<Long> pagesAttribute = new Attributes<>(42L);

  Value serializedPages = pagesAttribute.toValue(Int64::new);

See :ref:`DAML To Java Type Mapping <daml-codegen-java-primitive-types>` for an overview of the Java Bindings `Value`_ types.

Note: If the DAML type is a record or variant with more than one type parameter, you need to pass a conversion function to the ``toValue`` method for each type parameter.

Create a value of a generated type from a Java Bindings value
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Analogous to the ``toValue`` method, to create a value of a generated type, call the method ``fromValue`` and pass conversion functions from a Java Bindings `Value`_ type to the expected Java type.

.. code-block:: java

  Attribute<Long> pagesAttribute = Attribute.<Long>fromValue(serializedPages,
      f -> f.asInt64().getOrElseThrow(() -> throw new IllegalArgumentException("Expected Int field").getValue());

See Java Bindings `Value`_ class for the methods to transform the Java Bindings types into corresponding Java types.


Non-exposed parameterized types
"""""""""""""""""""""""""""""""

If the parameterized type is contained in a type where the *actual* type is specified (as in the ``BookAttributes`` type above), then the conversion methods of the enclosing type provides the required conversion function parameters automatically.

Convert List and Optional values
""""""""""""""""""""""""""""""""

The conversion of the Java ``List`` and ``Optional`` types requires multiple steps to first convert the list elements or the value inside the optional creating the `DamlList`_ or `DamlOptional`_ value.

.. code-block:: java

  Attribute<List<String>> authorsAttribute = new Attribute<List<String>>(Arrays.asList("Homer", "Ovid", "Vergil"));

  Value serializedAuthors = authorsAttribute.toValue(f -> new DamlList(f.stream().map(Text::new).collect(Collectors.<Value>toList())));

The conversion of the Java ``List`` and ``Optional`` types similarly require that the Java Bindings types `DamlList`_ and `DamlOptional`_ are converted to it's Java equivalent and then all the contained elements are converted to Java types.

.. code-block:: java

  Attribute<List<String>> authorsAttribute = Attribute.<List<String>>fromValue(serializedAuthors,
      f0 -> f0.asList().orElseThrow(() -> new IllegalArgumentException("Expected DamlList field")).getValues().stream()
          .map(f1 -> f1.asText().orElseThrow(() -> new IllegalArgumentException("Expected Text element")).getValue())
              .collect(Collectors.toList()));


.. _Value: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/Value.html
.. _Int64: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Int64.html
.. _Decimal: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Decimal.html
.. _Text: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Text.html
.. _Bool: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Bool.html
.. _Party: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Party.html
.. _Date: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Date.html
.. _Timestamp: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Timestamp.html
.. _DamlList: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/DamlList.html
.. _TextMap: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/TextMap.html
.. _DamlOptional: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/DamlOptional.html
.. _Unit: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/Unit.html
.. _ContractId: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/data/ContractId.html
