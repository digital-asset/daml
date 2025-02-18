.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-stack-24914:

DA.Stack
========

Data Types
----------

.. _type-da-stack-types-srcloc-15887:

**data** `SrcLoc <type-da-stack-types-srcloc-15887_>`_

  Location in the source code\.

  Line and column are 0\-based\.

  .. _constr-da-stack-types-srcloc-29880:

  `SrcLoc <constr-da-stack-types-srcloc-29880_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - srcLocPackage
         - :ref:`Text <type-ghc-types-text-51952>`
         -
       * - srcLocModule
         - :ref:`Text <type-ghc-types-text-51952>`
         -
       * - srcLocFile
         - :ref:`Text <type-ghc-types-text-51952>`
         -
       * - srcLocStartLine
         - :ref:`Int <type-ghc-types-int-37261>`
         -
       * - srcLocStartCol
         - :ref:`Int <type-ghc-types-int-37261>`
         -
       * - srcLocEndLine
         - :ref:`Int <type-ghc-types-int-37261>`
         -
       * - srcLocEndCol
         - :ref:`Int <type-ghc-types-int-37261>`
         -

.. _type-ghc-stack-types-callstack-86244:

**data** `CallStack <type-ghc-stack-types-callstack-86244_>`_

  Type of callstacks constructed automatically from ``HasCallStack`` constraints\.

  Use ``callStack`` to get the current callstack, and use ``getCallStack``
  to deconstruct the ``CallStack``\.

.. _type-ghc-stack-types-hascallstack-63713:

**type** `HasCallStack <type-ghc-stack-types-hascallstack-63713_>`_
  \= IP \"callStack\" `CallStack <type-ghc-stack-types-callstack-86244_>`_

  Request a ``CallStack``\. Use this as a constraint in type signatures in order
  to get nicer callstacks for error and debug messages\.

  For example, instead of declaring the following type signature\:

  .. code-block:: daml

    myFunction : Int -> Update ()


  You can declare a type signature with the ``HasCallStack`` constraint\:

  .. code-block:: daml

    myFunction : HasCallStack => Int -> Update ()


  The function ``myFunction`` will still be called the same way, but it will also show up
  as an entry in the current callstack, which you can obtain with ``callStack``\.

  Note that only functions with the ``HasCallStack`` constraint will be added to the
  current callstack, and if any function does not have the ``HasCallStack`` constraint,
  the callstack will be reset within that function\.

Functions
---------

.. _function-da-stack-prettycallstack-78669:

`prettyCallStack <function-da-stack-prettycallstack-78669_>`_
  \: `CallStack <type-ghc-stack-types-callstack-86244_>`_ \-\> :ref:`Text <type-ghc-types-text-51952>`

  Pretty\-print a ``CallStack``\.

.. _function-da-stack-getcallstack-34576:

`getCallStack <function-da-stack-getcallstack-34576_>`_
  \: `CallStack <type-ghc-stack-types-callstack-86244_>`_ \-\> \[(:ref:`Text <type-ghc-types-text-51952>`, `SrcLoc <type-da-stack-types-srcloc-15887_>`_)\]

  Extract the list of call sites from the ``CallStack``\.

  The most recent call comes first\.

.. _function-da-stack-callstack-89067:

`callStack <function-da-stack-callstack-89067_>`_
  \: `HasCallStack <type-ghc-stack-types-hascallstack-63713_>`_ \=\> `CallStack <type-ghc-stack-types-callstack-86244_>`_

  Access to the current ``CallStack``\.
