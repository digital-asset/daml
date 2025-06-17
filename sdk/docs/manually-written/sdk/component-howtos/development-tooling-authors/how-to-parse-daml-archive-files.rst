.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _how-to-parse-daml-archive-files:

How to parse Daml archive files
###############################

When a Daml project is compiled, it produces a DAR (extension ``.dar``), short
for Daml Archive. The Daml compiler exposes commands for inspecting this
archive.

.. _inspecting_dars:

Inspecting a DAR file
*********************

You can run ``daml damlc inspect-dar /path/to/your.dar`` to get a
human-readable listing of the files inside it and a list of packages
and their package ids. This is often useful to find the package id of the
project you just built.

For example, consider a package ``mypkg`` which depends on a package ``dep``:

.. code-block:: yaml
   :caption: mypkg/daml.yaml

   name: mypkg
   version: 1.0.0
   source: daml/Main.daml
   ...
   data-dependencies:
   - ../dep/.daml/dist/dep-1.0.0.dar

.. code-block:: yaml
   :caption: dep/daml.yaml

   name: dep
   version: 1.0.0
   source: daml/Dep.daml
   ...

When ``mypkg-1.0.0`` is compiled to a DAR, we can inspect that DAR to ensure that
it contains both the ``mypkg`` package and its dependency ``dep``:

.. code-block:: sh

   > daml build
   ...
   Created .daml/dist/mypkg-1.0.0.dar

   > daml damlc inspect-dar .daml/dist/mypkg-1.0.0.dar

   DAR archive contains the following files:

   ...
   mypkg-1.0.0-<mypkg-package-id>/dep-1.0.0-<dep-package-id>.dalf
   mypkg-1.0.0-<mypkg-package-id>/mypkg-1.0.0-<mypkg-package-id>.dalf
   mypkg-1.0.0-<mypkg-package-id>/MyPkg.daml
   mypkg-1.0.0-<mypkg-package-id>/MyPkg.hi
   mypkg-1.0.0-<mypkg-package-id>/MyPkg.hie
   META-INF/MANIFEST.MF

   DAR archive contains the following packages:

   ...
   dep-1.0.0-<dep-package-id> "<dep-package-id>"
   mypkg-1.0.0-<mypkg-package-id> "<mypkg-package-id>"

The first section reports all of the files in DAR, and the second section
reports the package name and package ID for every DALF in the archive.

More information on the exact structure of the zip file is :ref:`available in the explanation on Daml packages and archive files <structure-of-an-archive-file>`.

Inspecting a DAR file as JSON
*****************************

In addition to the human-readable output, you can also get the output
as JSON. This is easier to consume programmatically and it is more
robust to changes across SDK versions:

.. code-block:: sh

  > daml damlc inspect-dar --json .daml/dist/mypkg-1.0.0.dar
  {
      "files": [
          "mypkg-1.0.0-<mypkg-package-id>/dep-1.0.0-<dep-package-id>.dalf",
          "mypkg-1.0.0-<mypkg-package-id>/mypkg-1.0.0-<mypkg-package-id>.dalf",
          "mypkg-1.0.0-<mypkg-package-id>/Main.daml",
          "mypkg-1.0.0-<mypkg-package-id>/Main.hi",
          "mypkg-1.0.0-<mypkg-package-id>/Main.hie",
          "META-INF/MANIFEST.MF"
      ],
      "main_package_id": "<mypkg-package-id>",
      "packages": {
          "<mypkg-package-id>": {
              "name": "mypkg",
              "path":
                "mypkg-1.0.0-<mypkg-package-id>/mypkg-1.0.0-<mypkg-package-id>.dalf",
              "version": "1.0.0"
          },
          "<dep-package-id>": {
              "name": "dep",
              "path": "mypkg-1.0.0-<mypkg-package-id>/dep-1.0.0-<dep-package-id>.dalf",
              "version": "1.0.0"
          }
      }
  }


Note that ``name`` and ``version`` will be ``null`` for packages in Daml-LF < 1.8.

Inspecting the main package of a DAR file
*****************************************

If you'd like to inspect the code inside the main package of a DAR, the Daml
compiler provides the ``inspect`` tool; running ``daml damlc inspect <path-to-dar-file>``
prints all of the code in the main package of that DAR file in a human-readable
format.

For example, run the ``inspect`` tool on the DAR produced in the previous
section:

.. code-block:: sh

   # Human-readable dump of code in "mypkg" package inside of "mypkg" DAR
   > daml damlc inspect .daml/dist/mypkg-1.0.0.dar
   package <mypkg-package-id>
   daml-lf 2.1
   metadata mypkg-1.0.0

   module Main where
   ...

Inspecting a DALF file
**********************

The ``inspect`` tool also accepts DALF files; running ``daml damlc inspect <path-to-dalf-file>``
on a DALF file prints all of the code in that DALF file.

We can unzip a DAR to access its dalfs and inspect them, for example with the
DAR from the previous section:

.. code-block:: sh

   # Unzip the DAR to get its DALFs
   > unzip .daml/dist/mypkg-1.0.0.dar

   # Human-readable dump of code in dep
   > daml damlc inspect mypkg-1.0.0-<mypkg-package-id>/dep-1.0.0-<dep-package-id>.dalf
   package <dep-package-id>
   daml-lf 2.1
   metadata dep-1.0.0

   module Dep where
   ...

We can even inspect the main package of a DAR this way, even though running
``inspect`` directly on the DAR file would require fewer steps.

.. code-block:: sh

   # Identical to dump from `daml damlc inspect .daml/dist/mypkg-1.0.0.dar`
   > daml damlc inspect mypkg-1.0.0-<mypkg-package-id>/mypkg-1.0.0-<mypkg-package-id>.dalf
   package <mypkg-package-id>
   daml-lf 2.1
   metadata mypkg-1.0.0

   module Main where
   ...

Parsing DAR and DALF files
**************************

To parse a DAR or DALF file from within Scala code, the
``com-daml:daml-lf-archive-reader`` `library on Maven <https://mvnrepository.com/artifact/com.daml/daml-lf-archive-reader>`
provides a Scala package object ``com.digitalasset.daml.lf.archive`` with
several decoders. Below are the common types of inputs and outputs a decoder can
have, and which decoders to use depending on the input and output that is
desired. For more details on inputs, outputs, and decoders, please refer to
Maven to find the source code for the associated libraries.

Output types
""""""""""""

When decoding a package, a decoder can have one of several possible outputs,
depending on what is needed.

* When the full code of the package is needed, pick a decoder returning tuples
  ``(PackageId, Package)``.

  In this case, ``PackageId`` is a string-like type that comes from
  ``com.digitalasset.daml.lf.language.Ref`` in the ``com.daml:daml-lf-data``
  `library on Maven <https://mvnrepository.com/artifact/com.daml/daml-lf-data>`.
  ``Package`` represents the full structure of a package, and comes
  from ``com.digitalasset.daml.lf.language.Ast``, in the ``com.daml:daml-lf-language``
  `library on Maven <https://mvnrepository.com/artifact/com.daml/daml-lf-language>`.

  Because fully decoding the package takes more processing time than the next
  two examples, only use it when the full package code is needed.

  For example, the ``com.digitalasset.daml.lf.typesig.reader.SignatureReader`` class
  from the ``com-daml:daml-lf-api-type-signature`` `library on Maven <https://mvnrepository.com/artifact/com.daml/daml-lf-api-type-signature>`
  takes a ``(PackageId, Package)`` pair to produce a ``com.digitalasset.daml.lf.typesig.PackageSignature``
  (also from the ``api-type-signature``) package, which specifies all of the
  templates, datatypes, and interfaces in a package.

* When only the simplest representation of the protobuf of the package is
  needed, pick a decoder returning a ``com.digitalasset.daml.lf.ArchivePayload``
  (from the ``com-daml:daml-lf-archive`` `library on Maven <https://mvnrepository.com/artifact/com.daml/daml-lf-archive>`).
  This should only be needed when working with internal protobuf representations
  of a package.

* When only the package's byte representation and hash is needed, use a
  decoder that returns ``Archive`` (from the ``com-daml:daml-lf-archive-proto``
  `library on Maven <https://mvnrepository.com/artifact/com.daml/daml-lf-archive-proto>`).
  When using this, the decoder will not spend time decoding any of the package's
  actual content, such as its metadata or its code.

Input types
"""""""""""

A decoder can either accept DALF files, DAR files, or it can accept both.

* If a decoder accepts DALF files, it will parse the single package in that DALF
  file to its output type (one of the three specified above).
* If a decoder accepts DAR files, it will parse multiple packages
  from a DAR file to a struct ``Dar[X]``, which is a case class that encodes a DAR
  as two public fields, ``main: X`` and ``dependencies: List[X]``.
* If a decoder accepts both, it will always produce a ``Dar[X]``. When given a
  DAR, the decoder will run as a normal DAR decoder would. When given a DALF,
  the decoder will decode the DALF as a single package and return a ``Dar[X]``
  with a ``main`` package and an empty list of dependencies.

Decoders
""""""""

Decoders for reading DALFs are instances of ``GenReader[X]``, which provides the
method ``readArchiveFromFile(file: java.io.File): Either[Error, X]``.

* ``val ArchiveReader: GenReader[ArchivePayload]``

  Run ``ArchiveReader.readArchiveFromFile(new java.io.File("<path-to-dalf>"))`` to parse
  out the ``ArchivePayload`` of a dalf file.
* ``val ArchiveDecoder: GenReader[(PackageId, Ast.Package)]``

  Run ``ArchiveDecoder.readArchiveFromFile(new java.io.File("<path-to-dalf>"))`` to parse
  out the ``(Ref.PackageId, Ast.Package)`` of a dalf file.
* ``val ArchiveParser: GenReader[DamlLf.Archive]``

  Run ``ArchiveParser.readArchiveFromFile(new java.io.File("<path-to-dalf>"))`` to parse
  out the ``DamlLf.Archive`` of a dalf file.

Decoders for reading DARs are instances of ``GenDarReader``, which provides the
method ``readArchiveFromFile(file: java.io.File): Either[Error, Dar[X]]``.

* ``val DarReader: GenDarReader[ArchivePayload]``

  Run ``DarReader.readArchiveFromFile(new java.io.File("<path-to-dar>"))`` to parse
  out the ``Dar[ArchivePayload]`` of a dar file.
* ``val DarDecoder: GenDarReader[(PackageId, Ast.Package)]``

  Run ``DarDecoder.readArchiveFromFile(new java.io.File("<path-to-dar>"))`` to parse
  out the ``Dar[(Ref.PackageId, Ast.Package)]`` of a dar file.
* ``val DarParser: GenDarReader[DamlLf.Archive]``

  Run ``DarParser.readArchiveFromFile(new java.io.File("<path-to-dar>"))`` to parse
  out the ``Dar[DamlLf.Archive]`` of a dar file.

Decoders for reading DARs are instances of ``GenUniversalArchiveReader``, which
provides the method ``readFile(file: java.io.File): Either[Error, Dar[X]]``.

* ``val UniversalArchiveReader: GenUniversalArchiveReader[ArchivePayload]``

  Run ``UniversalArchiveReader.readFile(new java.io.File("<path-to-dar-or-dalf>"))``
  to parse out the ``Dar[ArchivePayload]`` of a dar file.
* ``val UniversalArchiveDecoder: GenUniversalArchiveReader[(PackageId, Ast.Package)]``

  Run ``UniversalArchiveDecoder.readFile(new java.io.File("<path-to-dar-or-dalf>"))``
  to parse out the ``Dar[(Ref.PackageId, Ast.Package)]`` of a dar file.

Example
"""""""

We can load up a Scala REPL with the ``daml-lf-archive-reader`` library to
interactively parse our ``mypkg`` DAR:

.. code-block:: scala

   scala> // Start a REPL
   scala> val darEither = DarDecoder.readArchiveFromFile(".daml/dist/mypkg-1.0.0.dar")
   val dar: Either[Error, Dar[(Ref.PackageId, Ast.Package)]]
   Right(Dar((..., GenPackage(Map(Main -> ...
   ...

   scala> // Extract the resulting value
   scala> val dar = darEither.toOption.get

   scala> :t dar.main
   (Ref.PackageId, Ast.Package)

   scala> :t dar.dependencies
   List[(Ref.PackageId, Ast.Package)]

The Dar datatype also has a method ``.all`` which returns the main package and
dependencies as a single list. Mapping ``_1`` over this gets all of the package
IDs in the DAR:

.. code-block:: scala

   scala> dar.all.map(_._1)
   val res1: List[Ref.PackageId] = List(224..., 54f..., ...)

Get the names of all the dependency packages in the DAR by using the
``.metadata.name`` field in the ``Ast.Package`` datatype:

.. code-block:: scala

   scala> dar.dependencies.map(_._2.metadata.name)
   val res2: List[com.digitalasset.daml.lf.data.Ref.PackageName] = List(daml-prim, daml-prim-DA-Exception-ArithmeticError, ...
