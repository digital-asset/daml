.. _module-qualifiedretroactiveinterfaceinstance-76052:

QualifiedRetroactiveInterfaceInstance
-------------------------------------

Interfaces
^^^^^^^^^^

.. _type-qualifiedretroactiveinterfaceinstance-token-43978:

**interface** `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_

  **viewtype** `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_

  + **Choice** Archive

    Controller\: Signatories of implementing template

    Returns\: ()

    (no fields)

  + .. _type-qualifiedretroactiveinterfaceinstance-getrich-86333:

    **Choice** `GetRich <type-qualifiedretroactiveinterfaceinstance-getrich-86333_>`_

    Controller\: (DA\.Internal\.Record\.getField @\"owner\" (view this))

    Returns\: `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - byHowMuch
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -

  + .. _type-qualifiedretroactiveinterfaceinstance-noop-81106:

    **Choice** `Noop <type-qualifiedretroactiveinterfaceinstance-noop-81106_>`_

    Controller\: (DA\.Internal\.Record\.getField @\"owner\" (view this))

    Returns\: ()

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - nothing
         - ()
         -

  + .. _type-qualifiedretroactiveinterfaceinstance-split-60457:

    **Choice** `Split <type-qualifiedretroactiveinterfaceinstance-split-60457_>`_

    An interface choice comment\.

    Controller\: (DA\.Internal\.Record\.getField @\"owner\" (view this))

    Returns\: (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_)

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - splitAmount
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         - A choice field comment\.

  + .. _type-qualifiedretroactiveinterfaceinstance-transfer-84967:

    **Choice** `Transfer <type-qualifiedretroactiveinterfaceinstance-transfer-84967_>`_

    Controller\: (DA\.Internal\.Record\.getField @\"owner\" (view this)), newOwner

    Returns\: `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_

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

  **instance** `HasFromAnyView <https://docs.daml.com/daml/stdlib/DA-Internal-Interface-AnyView.html#class-da-internal-interface-anyview-hasfromanyview-30108>`_ `Token <type-qualifiedretroactiveinterfaceinstance-token-43978_>`_ `TokenView <type-qualifiedretroactiveinterfaceinstance-tokenview-25557_>`_

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
