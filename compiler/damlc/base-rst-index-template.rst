.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _stdlib-reference-base:

The standard library
====================

The DAML standard library is a collection of DAML modules that are bundled with the DAML SDK, and can be used to implement DAML applications.

The :ref:`Prelude <module-prelude-6842>` module is imported automatically in every DAML module. Other modules must be imported manually, just like your own project's modules. For example:

.. code-block:: daml

   import DA.Optional
   import DA.Time

Here is a complete list of modules in the standard library:

{{{body}}}
