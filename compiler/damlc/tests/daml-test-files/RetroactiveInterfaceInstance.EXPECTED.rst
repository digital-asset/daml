.. _module-retroactiveinterfaceinstance-60009:

Module RetroactiveInterfaceInstance
-----------------------------------

Interfaces
^^^^^^^^^^

.. _type-retroactiveinterfaceinstance-token-49693:

**interface** `Token <type-retroactiveinterfaceinstance-token-49693_>`_

  **viewtype** `TokenView <type-retroactiveinterfaceinstance-tokenview-57374_>`_

  + **Choice Archive**

    (no fields)

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

  + **Method setAmount \:** `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-retroactiveinterfaceinstance-token-49693_>`_

  + **Method splitImpl \:** `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_)

  + **Method transferImpl \:** `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_)

  + **interface instance** `Token <type-retroactiveinterfaceinstance-token-49693_>`_ **for** Asset

Data Types
^^^^^^^^^^

.. _type-retroactiveinterfaceinstance-tokenview-57374:

**data** `TokenView <type-retroactiveinterfaceinstance-tokenview-57374_>`_

  .. _constr-retroactiveinterfaceinstance-tokenview-95763:

  `TokenView <constr-retroactiveinterfaceinstance-tokenview-95763_>`_

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

  **instance** `HasFromAnyView <https://docs.daml.com/daml/stdlib/DA-Internal-Interface-AnyView.html#class-da-internal-interface-anyview-hasfromanyview-30108>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_ `TokenView <type-retroactiveinterfaceinstance-tokenview-57374_>`_

  **instance** `HasInterfaceView <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_ `TokenView <type-retroactiveinterfaceinstance-tokenview-57374_>`_

  **instance** `HasField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839>`_ \"amount\" `TokenView <type-retroactiveinterfaceinstance-tokenview-57374_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `HasField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839>`_ \"owner\" `TokenView <type-retroactiveinterfaceinstance-tokenview-57374_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

Functions
^^^^^^^^^

.. _function-retroactiveinterfaceinstance-setamount-92750:

`setAmount <function-retroactiveinterfaceinstance-setamount-92750_>`_
  \: `Token <type-retroactiveinterfaceinstance-token-49693_>`_ \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Token <type-retroactiveinterfaceinstance-token-49693_>`_

.. _function-retroactiveinterfaceinstance-splitimpl-44512:

`splitImpl <function-retroactiveinterfaceinstance-splitimpl-44512_>`_
  \: `Token <type-retroactiveinterfaceinstance-token-49693_>`_ \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_, `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_)

.. _function-retroactiveinterfaceinstance-transferimpl-49252:

`transferImpl <function-retroactiveinterfaceinstance-transferimpl-49252_>`_
  \: `Token <type-retroactiveinterfaceinstance-token-49693_>`_ \-\> `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Token <type-retroactiveinterfaceinstance-token-49693_>`_)

.. _function-retroactiveinterfaceinstance-noopimpl-82337:

`noopImpl <function-retroactiveinterfaceinstance-noopimpl-82337_>`_
  \: `Token <type-retroactiveinterfaceinstance-token-49693_>`_ \-\> () \-\> `Update <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072>`_ ()
