.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML Standard Library
#####################

The DAML Standard Library is a collection of DAML modules organised in domains
that can be used to implement concrete applications.

Usage
*****

The standard library is included in the DAML compiler so it can
be used straight out of the box.
You can import modules from the standard library just like your own, e.g.:

.. ExcludeFromDamlParsing
.. code-block:: daml

  import DA.Optional

Domains
*******

.. toctree::
   :maxdepth: 1

   base
