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

.. _daml-codegen-java-primitive-types:

Map DAML primitives to Java types
---------------------------------

DAML built-in types are translated to the following equivalent types in
Java:

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

Parameterized types
~~~~~~~~~~~~~~~~~~~

The Java Code Generator uses Java Generic types to represent :ref:`DAML parameterized types <daml-ref-parameterized-types>`.

Below is a DAML fragment defining the parameterized type ``Attribute`` for use by the ``BookAttribute`` type for modeling
the characteristics of the book.

.. code-block:: daml
  :caption: Com/Acme.daml

  daml 1.2
  module Com.Acme where

  data Attribute a = Attribute
      with v : a

  data BookAttributes = BookAttributes with
     pages : (Attribute Int)
     authors : (Attribute [Text])
     title : (Attribute Text)

A file Java file is generated for the ``Attribute`` data type that defines the Java Generic class:

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

Serializing
"""""""""""

To serialize an instance of the ``Attribute<a>`` data type function for creating the Ledger API equivalent of the attribute value
is passed to the ``toValue`` method as the ``fromValuea`` argument (see the above ``com/acme/Attribute.java`` source extract).

Below is a Java fragment to serialize an attribute with a ``java.lang.Long`` value to Ledger API representation using the *method reference*
``Int64::new`` to create a new instance of the Java Bindings value type.

.. code-block:: java

  Attribute<Long> pagesAttribute = new Attributes<>(42L);

  Value serializedPages = pagesAttribute.toValue(Int64::new);

See :ref:`DAML To Java Type Mapping <daml-codegen-java-primitive-types>` for the Java Bindings value types need to be created in the `fromValue`` method.

Note: That if the DAML type is a record that has more that one parameterized type, then a function for creating the
Java Binding values must be supplied for *each* such type.

Deserializing
"""""""""""""

Analogous to the generated ``toValue`` method, the deserialization method ``fromValue`` takes a function to convert a Java Bindings ``Value`` type to the expected Java type.

.. code-block:: java

  Attribute<Long> pagesAttribute = Attribute.<Long>fromValue(serializedPages,
      f -> f.asInt64().getOrElseThrow(() -> throw new IllegalArgumentException("Expected Int field").getValue());

See Java Bindings `Value`_ class for the methods to transform the Java Bindings types into corresponding Java types.

.. _Value: /app-dev/bindings-java/javadocs/com/daml/ledger/javaapi/Value.html

Non-exposed parameterized types
"""""""""""""""""""""""""""""""

If the parameterized type is contained in a type where the *actual* type is specified (as in the ``BookAttributes`` type above), then the serialization
and deserialization of the enclosing type provides the necessary methods for serialization and deserialization.

DAML List and DAML Optional
"""""""""""""""""""""""""""

The serialization of the Java ``List`` and ``Optional`` types require a multiple stage conversion function where the elements must be
converted to the Java Binding Java Types before the creating the product type.

.. code-block:: java

  Attribute<List<String>> authorsAttribute = new Attribute<List<String>>(Arrays.asList("Homer", "Ovid", "Vergil"));

  Value serializedAuthors = authorsAttribute.toValue(f -> new DamlList(f.stream().map(Text::new).collect(Collectors.<Value>toList())));

The deserialization to the Java ``List`` and ``Optional`` types similarly require that the Java Bindings product type is converted to it's Java
equivalent and then all the contained elements are converted to Java types.

.. code-block:: java

  Attribute<List<String>> authorsAttribute = Attribute.<List<String>>fromValue(serializedAuthors,
      f0 -> f0.asList().orElseThrow(() -> new IllegalArgumentException("Expected DamlList field")).getValues().stream()
          .map(f1 -> f1.asText().orElseThrow(() -> new IllegalArgumentException("Expected Text element")).getValue())
              .collect(Collectors.toList()));
