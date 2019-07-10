.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [daml assistant] Fix VSCode path for use if not already in PATH on mac
- [daml assistant] **BREAKING**: remove `--replace=newer` option.
- [DAML Studio] Fix a bug where the extension seemed to disappear every other
  time VS Code was opened.

- [Sandbox] Fixing an issue around handling passTime in scenario loader
  See `#1953 <https://github.com/digital-asset/daml/issues/1953>`__.
- [DAML Studio] DAML Studio now displays a “Processing” indicator on the bottom
  left while the IDE is doing work in the background.
- [Sandbox] Remembering already loaded packages after reset
  See `#1979 <https://github.com/digital-asset/daml/issues/1953>`__.

- [DAML-LF]: Release version 1.6. This versions provides:

  + ``enum`` types. See `issue #105
    <https://github.com/digital-asset/daml/issues/105>`__ and `DAML-LF 1
    specification <https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst>`__
    for more details.

  + new builtins for (un)packing strings. See `issue #16
    <https://github.com/digital-asset/daml/issues/16>`__.

  + intern package IDs. See `issue #1614
    <https://github.com/digital-asset/daml/pull/1614>`__.

  + **BREAKING CHANGE** Restrict contract key lookups. In short, when looking
    up or fetching a key, the transaction submitter must be one of the key
    maintainers. The restriction was done in the DAML-LF development version
    (``1.dev``) until now.
    See `issue #1866 <https://github.com/digital-asset/daml/issues/1866>`__.
    This change is breaking, since this release makes DAML-LF ``1.6`` the
    default compiler output.

- [DAML Compiler]: Add support for DAML-LF ``1.6``. In particular:

  + **BREAKING CHANGE** Add support for ``enum`` types. DAML variant types
    that look like enumerations (i.e., those variants without type parameters
    and without arguments) are compiled to the new DAML-LF ``enum`` type when
    DAML-LF 1.6 target is selected. For instance the daml type declaration of
    the form::

      data Color = Red | Green | Blue

    will produce a DAML-LF ``enum`` type instead of DAML-LF ``variant`` type.
    This change is breaking, since this release makes DAML-LF ``1.6`` the
    default compiler output.

  + Add ``DA.Text.toCodePoints`` and ``DA.Text.fromCodePoints`` primitives to
    (un)pack strings.

  + Add support for DAML-LF intern package IDs.

- [Ledger API] Add support for ``enum`` types. Simple ``variant`` types will
  be replaced by ``enum`` types when using a DAML-LF ``1.6`` archive.

- [Java Codegen]: Add support for ``enum`` types. ``enum`` types are mapped to
  standard java enum. See `Generate Java code from DAML
  <https://github.com/digital-asset/daml/blob/master/docs/source/app-dev/bindings-java/codegen.rst>`__
  for more details.

- [Scala Codegen]: Add support for ``enum`` types.

- [Navigator]: Add support for ``enum`` types.

- [Extractor]: Add support for ``enum`` types.

- [DAML Compiler]: **BREAKING CHANGE** Make DAML-LF 1.6 the default output.
  This change activates the support of ``enum`` type describes above, and the
  `restriction about contract key lookup
  <https://github.com/digital-asset/daml/issues/1866>`__ described in the
  DAML-LF section
