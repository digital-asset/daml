# Developer's guide to Daml Scala code generator

For the User's guide to the Scala code generator see:
`docs/daml-scala-code-gen-user-guide.rst`

## Introduction

The Daml Scala code generator produces wrapper classes that correspond to Daml
contract templates and Daml user defined types (records/variants that are the
right hand side of a Daml type synonym). They are intended to be used by application
developers and they provide the same level of type safety as the Daml langauge
itself.

## Working with Scala macros

The code makes heavy use of Scala _macros_ and _quasiquotes_.

More information on Scala macros can be found
[here](http://docs.scala-lang.org/overviews/macros/overview.html)

There is some great documentation on _quasiquotes_
[here](http://docs.scala-lang.org/overviews/quasiquotes/setup.html)
In particular, this page is a great
[reference](http://docs.scala-lang.org/overviews/quasiquotes/syntax-summary.html)

You may also find this guide on the Scala Reflection API useful
as macros/quasiquotes make use of it. See
[here](http://docs.scala-lang.org/overviews/reflection/symbols-trees-types.html)

The reference documentation for the Reflection API is available
[here](http://www.scala-lang.org/api/current/scala-reflect/scala/reflect/api/index.html)

I have also found it very useful to use the Scala console to see the abstract
syntax tree (AST) structure

### `showRaw` is your friend

You can use the Reflection API's `showRaw` function to see which
constructors are used in a quasiquoted expression/statement/definition.

From an SBT console open a Scala console with:

    > console

Then:

    scala> val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
    universe: reflect.runtime.universe.type = scala.reflect.runtime.JavaUniverse@712e80d8

    scala> import universe._
    import universe._

Then (for example):

    scala> showRaw(q"obj.method")
    res2: String = Select(Ident(TermName("obj")), TermName("method"))

Use Ctrl-D to get back to SBT console

## Understanding the code

The best way to understand the structure of the generated code is to
read  `DamlContractTemplate.scala` and `DamlUserDefinedType.scala`. The
Scala macros are quite readable and both classes have informative comments.
