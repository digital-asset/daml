.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

WARNING: THIS FILE IS NO LONGER USED AND WILL SOON BE DELETED!

Changelog additions must now be added to the end of one or more commit message bodies in a PR.

The following is an example of a commit message including a description and a body that includes changelog additions:

.. code-block:: none

  Fixes #1311

  Also fixes a typo in the Scala bindings documentation.

  CHANGELOG_BEGIN

  - [Sandbox] Introduced a new API for package management.
    See `#1311 <https://github.com/digital-asset/daml/issues/1311>`__.

  CHANGELOG_END

Please check `CONTRIBUTING.MD <https://github.com/digital-asset/daml/blob/master/CONTRIBUTING.md#pull-request-checklist>`__ for more details.
