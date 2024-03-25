.. _module-damldocformatting-30347:

DamlDocFormatting
-----------------

Templates
^^^^^^^^^

.. _type-damldocformatting-payment-18108:

**template** `Payment <type-damldocformatting-payment-18108_>`_

  Signatory\: sender, map (\\ r \-\> (DA\.Internal\.Record\.getField @\"receiver\" r)) receiverAmounts

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1

     * - Field
       - Type
       - Description
     * - sender
       - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
       -
     * - receiverAmounts
       - \[`ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_\]
       -

  + .. _type-damldocformatting-addreceiver-84828:

    **Choice** `AddReceiver <type-damldocformatting-addreceiver-84828_>`_

    Controller\: undefined \: Party

    Returns\: `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ `Payment <type-damldocformatting-payment-18108_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - receiver
         - `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_
         -

  + **Choice** Archive

    Controller\: sender, map (\\ r \-\> (DA\.Internal\.Record\.getField @\"receiver\" r)) receiverAmounts

    Returns\: ()

    (no fields)

Data Types
^^^^^^^^^^

.. _type-damldocformatting-receiveramount-1032:

**data** `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_

  .. _constr-damldocformatting-receiveramount-87105:

  `ReceiverAmount <constr-damldocformatting-receiveramount-87105_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - receiver
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -
       * - amount
         - `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_

  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"amount\" `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_ `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"receiver\" `AddReceiver <type-damldocformatting-addreceiver-84828_>`_ `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"receiver\" `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"receiverAmounts\" `Payment <type-damldocformatting-payment-18108_>`_ \[`ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"amount\" `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_ `Decimal <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"receiver\" `AddReceiver <type-damldocformatting-addreceiver-84828_>`_ `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"receiver\" `ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"receiverAmounts\" `Payment <type-damldocformatting-payment-18108_>`_ \[`ReceiverAmount <type-damldocformatting-receiveramount-1032_>`_\]
