.. _module-interface-72439:

Module Interface
----------------

Templates
^^^^^^^^^

.. _type-interface-asset-25340:

**template** `Asset <type-interface-asset-25340_>`_

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
    

  + **implements** `Token <type-interface-token-10651_>`_

Interfaces
^^^^^^^^^^

.. _type-interface-token-10651:

**interface** `Token <type-interface-token-10651_>`_

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
  
  + **Method setAmount \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-interface-token-10651_>`_
  
  + **Method splitImpl \: **`Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interface-token-10651_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interface-token-10651_>`_)
  
  + **Method transferImpl \: **`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interface-token-10651_>`_)

Functions
^^^^^^^^^

.. _function-interface-getowner-36980:

`getOwner <function-interface-getowner-36980_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interface-token-10651_>`_ \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

.. _function-interface-getamount-416:

`getAmount <function-interface-getamount-416_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interface-token-10651_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

.. _function-interface-setamount-37812:

`setAmount <function-interface-setamount-37812_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interface-token-10651_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-interface-token-10651_>`_

.. _function-interface-splitimpl-93694:

`splitImpl <function-interface-splitimpl-93694_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interface-token-10651_>`_ \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interface-token-10651_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interface-token-10651_>`_)

.. _function-interface-transferimpl-36342:

`transferImpl <function-interface-transferimpl-36342_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interface-token-10651_>`_ \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-interface-token-10651_>`_)

.. _function-interface-noopimpl-41891:

`noopImpl <function-interface-noopimpl-41891_>`_
  \: `Implements <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077>`_ t `Token <type-interface-token-10651_>`_ \=\> t \-\> () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ ()
