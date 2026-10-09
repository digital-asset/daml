.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-internal-interface-anyview-types-13315:

DA.Internal.Interface.AnyView.Types
===================================

Data Types
----------

.. _type-da-internal-interface-anyview-types-anyview-16883:

**data** `AnyView <type-da-internal-interface-anyview-types-anyview-16883_>`_

  Existential contract key type that can wrap an arbitrary contract key\.

  .. _constr-da-internal-interface-anyview-types-anyview-58868:

  `AnyView <constr-da-internal-interface-anyview-types-anyview-58868_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getAnyView
         - Any
         -
       * - getAnyViewInterfaceTypeRep
         - `InterfaceTypeRep <type-da-internal-interface-anyview-types-interfacetyperep-5047_>`_
         -

.. _type-da-internal-interface-anyview-types-interfacetyperep-5047:

**data** `InterfaceTypeRep <type-da-internal-interface-anyview-types-interfacetyperep-5047_>`_

  .. _constr-da-internal-interface-anyview-types-interfacetyperep-24802:

  `InterfaceTypeRep <constr-da-internal-interface-anyview-types-interfacetyperep-24802_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getInterfaceTypeRep
         - TypeRep
         -

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` `InterfaceTypeRep <type-da-internal-interface-anyview-types-interfacetyperep-5047_>`_

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` `InterfaceTypeRep <type-da-internal-interface-anyview-types-interfacetyperep-5047_>`_
