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
    

  + **implements** Token

Functions
^^^^^^^^^

.. _function-interface-noopimpl-83220:

`noopImpl <function-interface-noopimpl-83220_>`_
  \: Implements t Token \=\> t \-\> () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ ()

.. _function-interface-transferimpl-81005:

`transferImpl <function-interface-transferimpl-81005_>`_
  \: Implements t Token \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ Token)

.. _function-interface-splitimpl-48531:

`splitImpl <function-interface-splitimpl-48531_>`_
  \: Implements t Token \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ Token, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171>`_ Token)

.. _function-interface-setamount-71357:

`setAmount <function-interface-setamount-71357_>`_
  \: Implements t Token \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_ \-\> Token

.. _function-interface-getamount-93321:

`getAmount <function-interface-getamount-93321_>`_
  \: Implements t Token \=\> t \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728>`_

.. _function-interface-getowner-9315:

`getOwner <function-interface-getowner-9315_>`_
  \: Implements t Token \=\> t \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311>`_
