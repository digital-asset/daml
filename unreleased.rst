.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [DAML Assistant] **BREAKING CHANGE** Changed the meaning of the ``source`` field in the daml.yaml
  file to be a pointer to the source directory of the DAML code contained in a project relative to
  the project root. This is breaking projects, where the ``source`` field of the project is pointing
  to a non-toplevel location in the source code directory structure.
+ [Sandbox] The sandbox now properly sets the connection pool properties ``minimumIdle``, ``maximumPoolSize``, and ``connectionTimeout``.
+ [Java codegen] Fix bug caused the generation of duplicate methods that affected sources with data constructors with type parameters that are either non-unique or not presented in the same order as in the corresponding data type declaration. See `#2367 <https://github.com/digital-asset/daml/issues/2367>`__.
* [Dev Tooling] `daml-sdk-head` now installs current-workdir versions of all the published JARs (as version `100.0.0`) to the local Maven repo (`~/.m2`), to allow local testing of unreleased versions of the SDK against JVM-based applications. (Disable with `--skip-jars`)
+ [DAML Compiler] Enable the language extension ``FlexibleContexts`` by default.
+ [DAML Compiler] **BREAKING CHANGE** Enable the language extension ``MonoLocalBinds`` by default. ``let`` and ``where`` bindings introducing polymorphic functions that are used at different types now need an explicit type annotation. Without the type annotation the type of the first use site will be inferred and use sites at different types will fail with a type mismatch error.
+ [Ledger] H2 Database support in the Ledger API Server.
+ [Ledger Api] *BREAKING CHANGE** In Protobuf ``Value`` message, rename ``decimal` field to ``numeric``.