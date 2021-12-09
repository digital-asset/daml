.. _module-interface-11558:

Module Interface
----------------

Templates
^^^^^^^^^

.. _type-interface-asset-14509:

**template** `Asset <type-interface-asset-14509_>`_

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1
  
     * - Field
       - Type
       - Description
     * - issuer
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
       - 
     * - owner
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
       - 
     * - amount
       - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_
       - 
  
  + **Choice Archive**
    

  + **implements** `Token <type-interface-token-72202_>`_

Interfaces
^^^^^^^^^^

.. _type-interface-token-72202:

**interface** `Token <type-interface-token-72202_>`_

  An interface comment\.
  
  + **Choice GetRich**
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - byHowMuch
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_
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
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - splitAmount
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_
         - A choice field comment\.
  
  + **Choice Transfer**
    
    .. list-table::
       :widths: 15 10 30
       :header-rows: 1
    
       * - Field
         - Type
         - Description
       * - newOwner
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
         - 
  
  + **Method getAmount \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_
    
    An interface comment\.
  
  + **Method getOwner \: **`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
    
    An interface comment\.
  
  + **Method noopImpl \: **() \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ ()
    
    An interface comment\.
  
  + **Method setAmount \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_ \-\> `Token <type-interface-token-72202_>`_
    
    An interface comment\.
  
  + **Method splitImpl \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Token <type-interface-token-72202_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Token <type-interface-token-72202_>`_)
    
    An interface comment\.
  
  + **Method transferImpl \: **`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Token <type-interface-token-72202_>`_)
    
    An interface comment\.

Functions
^^^^^^^^^

.. _function-interface-noopimpl-83220:

`noopImpl <function-interface-noopimpl-83220_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034>`_ t `Token <type-interface-token-72202_>`_ \=\> t \-\> () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ ()

.. _function-interface-transferimpl-81005:

`transferImpl <function-interface-transferimpl-81005_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034>`_ t `Token <type-interface-token-72202_>`_ \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Token <type-interface-token-72202_>`_)

.. _function-interface-splitimpl-48531:

`splitImpl <function-interface-splitimpl-48531_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034>`_ t `Token <type-interface-token-72202_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Token <type-interface-token-72202_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ `Token <type-interface-token-72202_>`_)

.. _function-interface-setamount-71357:

`setAmount <function-interface-setamount-71357_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034>`_ t `Token <type-interface-token-72202_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_ \-\> `Token <type-interface-token-72202_>`_

.. _function-interface-getamount-93321:

`getAmount <function-interface-getamount-93321_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034>`_ t `Token <type-interface-token-72202_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_

.. _function-interface-getowner-9315:

`getOwner <function-interface-getowner-9315_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034>`_ t `Token <type-interface-token-72202_>`_ \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
