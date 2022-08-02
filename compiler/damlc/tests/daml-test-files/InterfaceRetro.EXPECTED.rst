.. _module-interfaceretro-19712:

Module InterfaceRetro
---------------------

Templates
^^^^^^^^^

.. _type-interfaceretro-asset-16573:

**template** `Asset <type-interfaceretro-asset-16573_>`_

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1
  
     * - Field
       - Type
       - Description
     * - issuer
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
       - 
     * - owner
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
       - 
     * - amount
       - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
       - 
  
  + **Choice Archive**
    

Interfaces
^^^^^^^^^^

.. _type-interfaceretro-token-18810:

**interface** `Token <type-interfaceretro-token-18810_>`_

  An interface comment\.
  
  + **Choice GetRich**
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - byHowMuch
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         - 
  
  + **Choice Noop**
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - nothing
         - ()
         - 
  
  + **Choice Split**
    
    An interface choice comment\.
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - splitAmount
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         - A choice field comment\.
  
  + **Choice Transfer**
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - newOwner
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         - 
  
  + **Method getAmount \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
  
  + **Method getOwner \: **`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
    
    A method comment\.
  
  + **Method noopImpl \: **() \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ ()
  
  + **Method setAmount \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-interfaceretro-token-18810_>`_
  
  + **Method splitImpl \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interfaceretro-token-18810_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interfaceretro-token-18810_>`_)
  
  + **Method transferImpl \: **`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interfaceretro-token-18810_>`_)

Functions
^^^^^^^^^

.. _function-interfaceretro-getowner-81603:

`getOwner <function-interfaceretro-getowner-81603_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interfaceretro-token-18810_>`_ \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

.. _function-interfaceretro-getamount-86729:

`getAmount <function-interfaceretro-getamount-86729_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interfaceretro-token-18810_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

.. _function-interfaceretro-setamount-8125:

`setAmount <function-interfaceretro-setamount-8125_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interfaceretro-token-18810_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-interfaceretro-token-18810_>`_

.. _function-interfaceretro-splitimpl-8403:

`splitImpl <function-interfaceretro-splitimpl-8403_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interfaceretro-token-18810_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interfaceretro-token-18810_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interfaceretro-token-18810_>`_)

.. _function-interfaceretro-transferimpl-65645:

`transferImpl <function-interfaceretro-transferimpl-65645_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interfaceretro-token-18810_>`_ \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interfaceretro-token-18810_>`_)

.. _function-interfaceretro-noopimpl-10964:

`noopImpl <function-interfaceretro-noopimpl-10964_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interfaceretro-token-18810_>`_ \=\> t \-\> () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ ()

.. _function-interfaceretro-coimplementsassettoken-42965:

`_coimplements_Asset_Token <function-interfaceretro-coimplementsassettoken-42965_>`_
  \: CoImplementsT `Asset <type-interfaceretro-asset-16573_>`_ `Token <type-interfaceretro-token-18810_>`_
