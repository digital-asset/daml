.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _stdlib-reference-base:

The standard library
====================

The DAML standard library is a collection of DAML modules that can be used to implement concrete applications.

Usage
*****

The standard library is included in the DAML compiler so it can
be used straight out of the box. You can import modules from the standard library just like your own, for example:

.. code-block:: daml

   import DA.Optional
   import DA.Time

{{{body}}}