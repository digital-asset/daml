.. _module-qualifiedretroactiveinterfaceinstance-76052:

Module QualifiedRetroactiveInterfaceInstance
--------------------------------------------

Interfaces
^^^^^^^^^^

.. _type-qualifiedretroactiveinterfaceinstance-token-43978:

**interface** `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_

  **viewtype** `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_

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

  + **Method noopImpl \:** () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ ()

  + **Method setAmount \:** `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_

  + **Method splitImpl \:** `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_)

  + **Method transferImpl \:** `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_)

  + **interface instance** `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ **for** Asset

Data Types
^^^^^^^^^^

.. _type-qualifiedretroactiveinterfaceinstance-tokenview-25557:

**data** `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_

  .. _constr-qualifiedretroactiveinterfaceinstance-tokenview-72346:

  `TokenView <constr-qualifiedretroactiveinterfaceinstance-tokenview-72346_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - owner
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -
       * - amount
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -

  **instance** `HasInterfaceView <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_

  **instance** `HasField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839>`_ \"amount\" `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `HasField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839>`_ \"owner\" `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

Functions
^^^^^^^^^

.. _function-qualifiedretroactiveinterfaceinstance-setamount-51253:

`setAmount <function-qualifiedretroactiveinterfaceinstance-setamount-51253_>`_
  \: `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_

.. _function-qualifiedretroactiveinterfaceinstance-splitimpl-65579:

`splitImpl <function-qualifiedretroactiveinterfaceinstance-splitimpl-65579_>`_
  \: `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_)

.. _function-qualifiedretroactiveinterfaceinstance-transferimpl-9125:

`transferImpl <function-qualifiedretroactiveinterfaceinstance-transferimpl-9125_>`_
  \: `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_)

.. _function-qualifiedretroactiveinterfaceinstance-noopimpl-17100:

`noopImpl <function-qualifiedretroactiveinterfaceinstance-noopimpl-17100_>`_
  \: `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ \-\> () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ ()
