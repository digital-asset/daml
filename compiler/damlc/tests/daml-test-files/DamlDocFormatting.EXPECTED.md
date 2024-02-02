# <a name="module-damldocformatting-30347"></a>DamlDocFormatting

## Templates

<a name="type-damldocformatting-payment-18108"></a>**template** [Payment](#type-damldocformatting-payment-18108)

> Signatory: sender, map (\\ r -\> (DA.Internal.Record.getField @"receiver" r)) receiverAmounts
>
> | Field                                                                                   | Type                                                                                    | Description |
> | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> | sender                                                                                  | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | receiverAmounts                                                                         | \[[ReceiverAmount](#type-damldocformatting-receiveramount-1032)\]                       |  |
>
> * <a name="type-damldocformatting-addreceiver-84828"></a>**Choice** [AddReceiver](#type-damldocformatting-addreceiver-84828)
>
>   Controller: undefined : Party
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Payment](#type-damldocformatting-payment-18108)
>
>   | Field                                                         | Type                                                          | Description |
>   | :------------------------------------------------------------ | :------------------------------------------------------------ | :---------- |
>   | receiver                                                      | [ReceiverAmount](#type-damldocformatting-receiveramount-1032) |  |
>
> * **Choice** Archive
>
>   Controller: sender, map (\\ r -\> (DA.Internal.Record.getField @"receiver" r)) receiverAmounts
>
>   Returns: ()
>
>   (no fields)

## Data Types

<a name="type-damldocformatting-receiveramount-1032"></a>**data** [ReceiverAmount](#type-damldocformatting-receiveramount-1032)

> <a name="constr-damldocformatting-receiveramount-87105"></a>[ReceiverAmount](#constr-damldocformatting-receiveramount-87105)
>
> > | Field                                                                                   | Type                                                                                    | Description |
> > | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> > | receiver                                                                                | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> > | amount                                                                                  | [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135)  |  |
>
> **instance** [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) [ReceiverAmount](#type-damldocformatting-receiveramount-1032)
>
> **instance** [Ord](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395) [ReceiverAmount](#type-damldocformatting-receiveramount-1032)
>
> **instance** [Show](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360) [ReceiverAmount](#type-damldocformatting-receiveramount-1032)
>
> **instance** [GetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979) "amount" [ReceiverAmount](#type-damldocformatting-receiveramount-1032) [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135)
>
> **instance** [GetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979) "receiver" [AddReceiver](#type-damldocformatting-addreceiver-84828) [ReceiverAmount](#type-damldocformatting-receiveramount-1032)
>
> **instance** [GetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979) "receiver" [ReceiverAmount](#type-damldocformatting-receiveramount-1032) [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)
>
> **instance** [GetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979) "receiverAmounts" [Payment](#type-damldocformatting-payment-18108) \[[ReceiverAmount](#type-damldocformatting-receiveramount-1032)\]
>
> **instance** [SetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311) "amount" [ReceiverAmount](#type-damldocformatting-receiveramount-1032) [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135)
>
> **instance** [SetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311) "receiver" [AddReceiver](#type-damldocformatting-addreceiver-84828) [ReceiverAmount](#type-damldocformatting-receiveramount-1032)
>
> **instance** [SetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311) "receiver" [ReceiverAmount](#type-damldocformatting-receiveramount-1032) [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)
>
> **instance** [SetField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311) "receiverAmounts" [Payment](#type-damldocformatting-payment-18108) \[[ReceiverAmount](#type-damldocformatting-receiveramount-1032)\]
