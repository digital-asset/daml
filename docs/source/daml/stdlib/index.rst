.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Standard Library
#####################

The DAML Standard Library is a collection of DAML modules organised in domains
that can be used to implement concrete applications.

Usage
*****

The standard library is included in the DAML compiler, so you can access it easily. 

To use something from the standard library, import the module it's in. For example:

.. code-block:: daml

  import DA.Optional
  import DA.Time

Domains
*******

.. toctree::
   :maxdepth: 1

   base
