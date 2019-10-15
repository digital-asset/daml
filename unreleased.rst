.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------
- [DAML Ledger Integration Kit] Skew/LET/MRT/Config tests consolidated in a single suite.

+ [Java Bindings] Add helper to prepare transformer for ``Bot.wire``. See `issue #3097 <https://github.com/digital-asset/daml/issues/3097>`_.
+ [Ledger] The ledger api index server starts only after the indexer has finished initializing the database.
+ [DAML Standard Library] Add ``DA.Action.State`` module containing a ``State`` action that
  can be used for computations that modify a state variable.
+ [DAML Compiler] Fixed the location of interface files when the
  ``source`` field in ``daml.yaml`` points to a file. This is mainly
  important for when you want to use the created ``.dar`` in the
  ``dependencies`` field of another package.
  See `issue #3135 <https://github.com/digital-asset/daml/issues/3135>`_.
+ [SQL Extractor] The format used for storing Optional and Map values found in contracts
  as JSON has been replaced with :doc:`/json-api/lf-value-specification`.  See `issue
  #3066 <https://github.com/digital-asset/daml/issues/3066>`_ for specifics.
+ [Scala Codegen] Fixes for StackOverflowErrors in reading large LF archives. See `issue #3104 <https://github.com/digital-asset/daml/issues/3104>`_.
+ [Scala Bindings] Fixed a bug in the retry logic of ``LedgerClientBinding#retryingConfirmedCommands``. Commands are now only retried when the server responds with status ``RESOURCE_EXHAUSTED`` or ``UNAVAILABLE``.

+ [DAML-SDK Docker Image] The image now contains a ``daml`` user and the SDK is installed to ``/home/daml/.daml``.
  ``/home/daml/.daml/bin`` is automatically added to ``PATH``.
+ [JSON API - Experimental] Support for automatic package reload
  See `issue #2906 <https://github.com/digital-asset/daml/issues/2906>`_.
+ [Sandbox] Filter contracts or contracts keys in the database query for parties that cannot see them.
+ [DAML-LF] **Breaking** archive proto package renamed from `com.digitalasset.daml_lf` to `com.digitalasset.daml_lf_dev`
+ [DAML-LF] **Breaking** Some bintray/maven packages are renamed:
   - `com.digitalasset.daml-lf-proto` becomes `com.digitalasset.daml-lf-dev-archive-proto`
   - `com.digitalasset.daml-lf-archive` becomes `com.digitalasset:daml-lf-dev-archive-java-proto`
   - `com.digitalasset.daml-lf-archive-scala` becomes `com.digitalasset.daml-lf-archive-reader`
+ [Sandbox] Filter contracts or contracts keys in the database query for parties that cannot see them.
+ [DAML Standard Library] Add ``createAndExercise``
+ [DAML Triggers] This release contains a first version of an
  experimental DAML triggers feature that allows you to implement
  off-ledger automation in DAML.
+ [JSON API - Experimental] Add ``--address`` CLI option. IP address that HTTP JSON API service listens on. Defaults to 0.0.0.0.
+ [JSON API - Experimental] Add ``/parties`` endpoint.
+ [Sandbox] Party management fix, see `issue #3177 <https://github.com/digital-asset/daml/issues/3177>`_.
+ [DAML-LF] Add immutable bintray/maven packages for handling DAML-LF archive up to version 1.6 in a stable way:
   - `com.digitalasset.daml-lf-1.6-archive-proto`

     This package contains the archive protobuf definitions as they
     were introduced when 1.6 was freeze.  Those definitions can be
     used to read DAML-LF archive up to version 1.6.


     The main advantage of this package over the `dev` version
     (`com.digitalasset.daml-lf-dev-archive-proto`) is its
     immutability (it is guarantee it will never changed once
     introduced in the SDK) in other words one can used it without
     suffering frequent breaking change introduced in the `dev`
     version.

     Going forward the SKD will contains a similar immutable package
     containning the proto definition for at least each DAML-LF
     version the compiler support. 

     We strongly advice anyone reading daml-lf archive directly to use
     this package (or the under
     `com.digitalasset:daml-lf-1.6-archive-java-proto` package), as
     frequent breaking changes may be introduced in the `dev` without
     futher notice in the release notes.
     
   - `com.digitalasset:daml-lf-1.6-archive-java-proto`

     This package contains the java auto-generated from the package `com.digitalasset.daml-lf-1.6-archive-proto`
