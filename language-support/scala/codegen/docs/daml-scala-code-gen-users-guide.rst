.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Scala Code Generator User's Guide
===============================================================

If you happen to be reading this documentation in text mode, and would
like to see the marked up version try the following
`link <https://github.com/DACH-NY/da/blob/master/ledger-client/daml-codegen/docs/daml-scala-code-gen-users-guide.rst>`_.

If the link above has bit-rotted, an out-of-date version can be found
`here <https://github.com/DACH-NY/da/blob/ee43e62151a19a045decaa890ae6a65dd6906b3e/ledger-client/daml-codegen/docs/daml-scala-code-gen-users-guide.rst>`_.



Introduction
------------

The purpose of the DAML Scala Code Generator is to produce
Scala code that helps application developers retain the type safety
of the DAML language.

All generated code builds against something we call the Domain API support
library which is part of the Ledger Client API and has the Scala namespace
``com.daml.platform.domainapi``.

How to use it
-------------

In this example we will generate code for a DAML library whose top-level
module is ``Main.daml``. In this example we tacitly assume that this
top-level module imports many other modules.

.. code-block:: console

   $ damli export-core Main.daml.daml > Main.daml.sdaml

First change directory to ``ledger-client``. Then let's look at the
help for the tool.

.. code-block:: console

  $ sbt "project daml-codegen" "run --help"

  Scala code-generation from DAML
    -i, --input-file <value>
                             input top-level DAML module
    -p, --package name <value>
                             package name e.g. com.daml.mypackage
    -o, --output-dir <value>
                             output directory for Scala files

We can now generate code by:

.. code-block:: console

  $ sbt "project daml-codegen" "run -i Main.daml.sdaml -o /path/to/dir -p com.daml.sample"

This will produce a bunch of ``.scala`` files in ``path/to/dir`` that mirror
the module structure of the library. e.g. If the library contains
a module called ``Main.Account.Setup`` which further contains a contract template
called ``onboardParty`` then a file called
``/path/to/dir/main/account/setup/OnboardPartyContract.scala`` will be generated.
It will be defined in the namespace ``com.daml.sample.main.account.setup``

What it generates
-----------------

It generates:

1. Objects for contract templates. Each object provides methods for producing
   Create, Exercise and Archive Commands that can be used with the
   Ledger Client API.

2. Objects for user defined type synonyms for records/variants. They allow one
   to construct instances of these DAML values in Scala. These values can be used
   with contract template classes that accept such values as template or choice
   parameters. The generated classes also take care of
   serialization/deserialization to/from the ``ArgumentValue`` type used by the
   Ledger Client API.

3. An object called the *event decoder*  which listens to Events returned by the
   Ledger Client API and produces a *contract data* value for the appropriate
   contract template. This value can be used to exercise choices and archive
   the created contract.

What templates and type synonym declarations will have code generated for them?
-------------------------------------------------------------------------------

The DAML Scala code generator can only generate code for a subset of
contract templates and type synonym declarations.

- For contract templates all template and choice parameter types must be
  *valid parameter types*.
- For type synonyms the type declaration must be *valid type synonym declarations*

The following sub-sections we define precisely what are and aren't valid types.
We start with an informal introduction to the types that DAML allows. We then
characterise the restricted subset of types for both contract template/choice
parameters and type synonyms.

A short introduction to DAML's types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section is just meant to refresh your understanding of DAML types.
*Valid types* are a restriction of the types presented here.

A DAML type is either a *primitive type* or compound type created using a
*combining form*.

**Primitives types**

Primitive types are either:

- *Built-in types*.
  The built-in types are either unary, meaning they need to be applied to
  one type argument, or nullary, meaning they are types are their own.

  The nullary types are:

  * ``Bool``
  * ``Integer``
  * ``Decimal``
  * ``Text``
  * ``RelTime``
  * ``Time``
  * ``Party``
  * ``ContractId``
  * ``Update``
  * ``Scenario``

  There is only one unary built-in type: ``List``. (In DAML 1.0 ``Update``
  and ``Scenario`` will become unary types.)

- *type variables*. Type variables allow for *polymorphism*. In DAML, currently,
  they can only be *used* on the right-hand-side of type synonym declaration.
  They must be *declared* on the left hand side of type synonym declaration.

  e.g. ``type Maybe a = <just:a, nothing: {}>;``

- *top-level type references*
  e.g. ``Date``

  These are only valid if a corresponding *type synonym declaration* exists.
  e.g. ``type Date = Time;`` exists.

- *anonymous record type*
  e.g. ``{ entity: Text, party: Party }``

- *anonymous variant type*
  e.g. ``<circle: Decimal, square: Decimal>``;

**Combining forms**

You can create compound types using the following two combining forms:

- *type application*. One type is applied to one or more type parameters.
  ``List Integer``, ``Maybe Decimal``, ``Either Text Decimal``

- *arrow/function type*. Defines a function from an input type to an output type.

  e.g. ``Integer -> Decimal``, ``Party -> Contract``

  The arrow associates to the right so ``Text -> Text -> Text -> Text`` is
  really ``Text -> (Text -> (Text -> Text))``

Valid parameter types for contract template code generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We now define the subset of DAML types that are *valid* as contract
template/choice parameters. To be _valid_ types must not:

- contain function types
- contain the ``Update``, ``Scenario`` built-in types.
- be polymorphic i.e. contain type variables
- contain nested occurrences of anonymous records or variants

Here we use the word "contain" to mean either that the type is itself
a member of a class of types, or have nested occurrences of such types.
e.g. `Text -> Text` is itself a function type whereas `{ label: Text -> Text }`
has a nested occurrence of a function type. Both *contain* function types.

If any of the contract template/choice parameters are not valid then a
corresponding Scala object/class will not be generated. This does not mean one
should not write any DAML contract templates that violate the rules above. There
are many good reasons for writing such contract templates. However, they should
not be interacted with via nanobots, but rather by other DAML code.

In the following sub-sections we cover the reasons why these types have been
disallowed.

**Why are function types disallowed?**

Function types are disallowed, first, because this raises the question of how
function values would be serialized when being sent over
the Ledger Client API. Second, using function values in application-side
code would be firmly outside the scope of what nanobots should be used for
in the first place.

**Why are ``Update``, ``Scenario``, etc built-ins disallowed?**

The reasons are similar to why function types are disallowed. How do we
serialize such values? Also, nanobots really shouldn't be creating such values.

**Why are polymorphic types disallowed?**

Polymorphic types are disallowed because only concrete values can be serialized
and deserialized over the Ledger Client API.

However, declaring a polymorphic parameter type is not something you'll have to
worry about since (at the time of writing) "forall" types can't be introduced
directly and the only form of polymorphism that can be introduced is through
polymorphic type synonym declarations.

**Why are anonymous record/variant types disallowed?**

DAML Scala code generator's primary purpose is to *preserve the type safety of DAML on the application-side*.

Scala doesn't have a way to represent anonymous records or variants, but
can represent an equivalent DAML record/variant as a *class*. That is, the
type must be *nominal* (i.e. named) in Scala.

For this reason we require that any record/variant used as a contract
template/choice parameter be made nominal by *declaring it as a type synonym*.

Say you have the following code (which is invalid for code generation)

.. DamlVersion 0.1
.. ExcludeFromDamlParsing
.. code-block:: daml

    template settle (entityParty: { entity: Text, party: Party }) = ...

You need to rewrite this to be:

.. ExcludeFromDamlParsing
.. code-block:: daml

    type EntityParty = { entity: Text, party: Party };

    template settle (entityParty: EntityParty) = ...

**Examples**

In the following examples we present examples of valid and invalid
template parameters. The invalid examples are actually valid DAML but
a Scala object cannot be generated from them.

Invalid:

- ``template foo (fun: Integer -> Decimal) = ...``.
  because parameter ``fun`` is a function type.
- ``template foo (r: { employee: Text, salary: Decimal }) = ...``.
  because parameter ``r`` is anonymous record.

Valid:

.. ExcludeFromDamlParsing
.. code-block:: daml

    type Unit = {};
    type Maybe a = <just: a, nothing: Unit>;

    template foo (mbText: Maybe Text)


Valid type synonym declarations for code generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section presents which type synonym declarations are *valid* for
code generation. We also give the reason why these restrictions have been made.

The right-hand side of the type synonym (i.e. the part after the ``=`` sign)
must not:

- contain a function type
- contain *nested* occurrence of anonymous records or variants. Naturally,
  the right hand side may itself *be* an anonymous record or variant.

The Scala code generator will do one of two things. If the right-hand side of the
type synonym is a:

- record or variant then a Scala object is generated

- is any other type then a Scala type alias will be generated for it. The
  generated code will be polymorphic if the right-hand side contains type
  variables.

**Why are these types disallowed?**

Function types are disallowed for the same reason they are disallowed
as contract template/choice parameter types.

Nested occurrences of record/variants are disallowed for a similar reason to why
we don't allow anonymous record/variants as contract template/choice parameters.
Nested occurrences are disallowed because the generated Scala code needs to
refer to these nested types. It cannot do this unless it has a name to refer to
it by.

**Examples**

Invalid:

- ``type Maybe a = <just: a, nothing: {}>;``. Contains nested occurrence of record.
- ``type Foo = Integer -> Decimal;``. Is a function type.
- ``type Bar = List (Integer -> Decimal);``. Contains an function type.

Valid:

- ``type Maybe a = <just: a, nothing: Unit>;`` (``Unit`` must be previously
  declared).
- ``type Date = Time;``
- ``type MaybeEither a b = Maybe (Either a b);``
- ``type PolyEntityParty a = { entity: a, party: Party};``

The Domain API support library
------------------------------

The Domain API support library is part of the Ledger Client API (in namespace
``com.daml.platform.domainapi``)

It contains (among other things):

1. A trait ``DamlValue`` and case classes extending it for each of the primitive
   DAML values e.g. ``DamlInteger``, ``DamlDecimal``, etc

2. A Scala type class called ``ArgumentValueProtocol``. It is based on the
   design of the Scala ``spray.json`` library and is used to convert
   ``DamlValue`` values to ``ArgumentValue`` values (see package
   `da-java/platform/platform-api-scala``).

The Domain API
~~~~~~~~~~~~~~

There is also a ``DomainApi`` object
(in a *different* namespace ``com.daml.platform.client.api``) that is
used to simplify working with the Reflection API.

For now, the best place to see an example of the how one would use the Domain
API is by looking at the integration test in ``ledger-client/daml-codegen-sample-app/src/test/scala/com/digitalasset/codegen/ScalaCodeGenIT.scala``

We recommend you first read the section below before attempting to understand
this code.

Also, if you are unfamiliar with Akka streams it is also recommended that you
read the documentation at: http://doc.akka.io/docs/akka/2.5.3/scala/stream/index.html

A complete example
------------------

Introduction
~~~~~~~~~~~~

In this section we look at a real example of using the DAML Scala code generator.
It is derived from the SBT (Scala Build Tool) project in directory
``ledger-client/daml-codegen-sample-app``. We strongly recommend that you take a
look at the generated code after reading through the following sub-sections.

Generating the code in ``ledger-client/daml-codegen-sample-app``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a freshly checked out DAML repository to the following:

.. code-block:: shell

  $ cd /path/to/ledger-client/daml-codegen-sample-app
  $ sbt
  [info] Set current project to da (in build file:/path/to/ledger-client/)
  > daml-codegen-sample-app/compile

The generated files will now be found in
``.../daml-codegen-sample-app/src/main/scala/generated-sources/com/daml/sample``
and subdirectories thereof.

You can find the DAML code at ``.../daml-codegen-sample-app/src/main/daml/Main.daml``

The DAML code
~~~~~~~~~~~~~

Here is an excerpt from ``Main.daml`` which presents a rather contrived
definition of types and contract templates  buying "call" or "put" options. The
author of this document is quite aware that for the owner to choose both the
original price of the option and the new price at some point in the future makes
no sense whatsoever. Both of these values would need to come from an oracle in a
realistic setting.

.. ExcludeFromDamlParsing
.. code-block:: daml

  type OptionPrice = { symbol :: Text, price :: Decimal };

  type Option = <call :: OptionPrice, put :: OptionPrice>;

  buyOption =
    \(owner  :: Party)
     (seller :: Party)
     (option :: Option)
    -> await {
         "sell": seller chooses then create (mkOption owner seller option)
       };

  mkOption =
    \(owner  :: Party)
     (seller :: Party)
     (option :: Option)
    -> await {
         "exerciseOption":
            owner chooses then
              case option of
              { <call: optionPrice> ->
                  -- To make a profit newPrice < option["price"]
                  owner chooses newPrice :: Decimal
                  then optionCall owner seller optionPrice newPrice
              ; <put: optionPrice>  ->
                  -- To make a profit option["price"] < price
                  owner chooses newPrice :: Decimal
                  then optionPut owner seller optionPrice newPrice
              }
    };

For brevity, we have elided the ``optionCall`` and ``optionPut`` contract
templates.


The generated variant for the ``Option`` type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: Scala

  package com.daml.sample.main {
    object OptionType {
      sealed trait Value extends DamlValue;
      final case class Call(value: com.daml.sample.main.OptionPriceType.Value)
        extends Value;
      final case class Put(value: com.daml.sample.main.OptionPriceType.Value)
        extends Value;
      object Value {
        def call(value: com.daml.sample.main.OptionPriceType.Value): Value =
          Call(value);
        def put(value: com.daml.sample.main.OptionPriceType.Value): Value =
          Put(value)
      };
      object ArgumentValueProtocol {
        ... code for (de)serialization to/from ArgumentValue values ...
      }
    }
  }

Note that a "Type" suffix has been added to the name of the generated Scala
object.

The ``call`` and ``put`` variants of the DAML ``Option`` type synonym are
generated in a straightforward idiomatic way in Scala. A (scoped) ``Value``
trait is defined along with two *case classes* which extend it. It extends
the ``DamlValue`` trait which is defined in the Ledger Client API package
(in directory ``da-java/platform/client/ledger-client_2.12``)
in the namespace ``com.daml.platform.domainapi``.

A value of this type is referred to with the fully qualified name ``com.daml.sample.main.Option.Value``

The inner ``object Value`` contains two convenience methods which automatically
up-cast to the ``Value`` type.

The generated code also includes type class instances for the
``ArgumentValueProtocol``. You won't have to deal with this directly but it
is used by other generated code to automatically serialize/deserialize to/from
``ArgumentValue`` values (see package ``platform-scala-api`` for definition.)

The generated record code for the ``OptionPrice`` type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: Scala

  package com.daml.sample.main {
    object OptionPriceType {
      case class Value(symbol: DamlText, price: DamlDecimal) extends DamlValue;
      object ArgumentValueProtocol {
        implicit object OptionPriceTypeArgumentValueFormat extends ArgumentValueFormat[Value] {
          ... code for (de)serialization to/from ArgumentValue values ...
        }
      }
    }
  }

As for the variant code a "Type" suffix has been added to the name of the
gnenerated Scala object.

The encoding of a DAML record is even simpler than that of a DAML variant.
It is just a case class and is once again named ``Value``.


The generated contract template code for ``buyOption``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: scala

  ... imports ...
  package com.daml.sample.main {
    object BuyOptionContract extends DamlContract {
      val template: TemplateId = Tag("buyOption_t2@Main_8");

      def createFromArgs(args: Args): CreateCmd = ...

      def create(owner: DamlParty, seller: DamlParty,
                 option: com.daml.sample.main.OptionType.Value): CreateCmd = ...


      case class Args(owner: DamlParty, seller: DamlParty,
                      option: com.daml.sample.main.OptionType.Value)
                 extends ContractArgs {
        def toNamedArguments: NamedArguments = ...
      };

      case class Ref(absoluteContractId: AbsoluteContractId, args: Args)
                 extends ContractRef {
        def sell() = ...
        def archive(): ArchiveCmd = ...
      };

      def argsFromNamedArguments(namedArguments: NamedArguments): Option[Args] = ...
    }
  }

We will now look at each generated class/object/method in some more detail.

1. The object name is ``BuyOptionContract`` as compared to `buyOption` in DAML.
   All generated contract template objects have their named capitalized and
   suffixed with "Contract".

2.  The Core Package *template ID* of the contract template is accessible via
    the ``template`` value.

3. The ``create`` method is used to construct a value of type ``CreateCmd``
   This value should be submitted to the Ledger Client API.
   The ``create`` method is really a convenience wrapper for the
   ``createFromArgs`` method which does essentially the same thing.

4. The ``Args`` case class is an inner class that extends the ``ContractArgs``
   trait. It just represents the arguments of the contract template and
   provides a ``toNamedArguments`` method that converts to values of type
   ``NamedArguments`` (defined ``platform-api-scala`` package)

5. The ``Ref`` case class is an inner class that represents a reference to the
   contract that has already been created on the ledger.
   It will always contain an ``archive`` method which produces a value of type
   ``ArchiveCmd``. Also there will be one method for each  *choice* of the contract
   template. Each of these methods will produce a value of type ``ExerciseCmd``.
   In this case there is only one choice, ``sell``. This particular method
   takes no arguments but in general they will.

6. The ``argsFromNamedArguments`` method is used to convert a value of type
   ``NamedArguments`` to a value of type of the inner ``Args`` case class.
   If used correctly this should always succeed but neverthless has a return
   type of ``Option[Args]``. This method is used by the generated
   event decoder to decode the ``arguments`` field of incoming ``Event`` values
   coming through the Ledger Client API.
