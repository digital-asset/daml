.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _stdlib-reference-base:

The standard library
====================

The Daml standard library is a collection of Daml modules that are bundled with the SDK, and can be used to implement Daml applications.

The :ref:`Prelude <module-prelude-72703>` module is imported automatically in every Daml module. Other modules must be imported manually, just like your own project's modules. For example:

.. code-block:: daml

   import DA.Optional
   import DA.Time

Here is a complete list of modules in the standard library:

{{{body}}}
