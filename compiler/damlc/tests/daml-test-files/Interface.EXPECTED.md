# <a name="module-interface-72439"></a>Module Interface

## Templates

<a name="type-interface-asset-25340"></a>**template** [Asset](#type-interface-asset-25340)

> | Field                                                                                   | Type                                                                                    | Description |
> | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> | issuer                                                                                  | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | owner                                                                                   | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | amount                                                                                  | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)          |  |
>
> * **Choice Archive**
>
>   (no fields)

> * **interface instance** [Token](#type-interface-token-10651) **for** [Asset](#type-interface-asset-25340)

## Interfaces

<a name="type-interface-token-10651"></a>**interface** [Token](#type-interface-token-10651)

> An interface comment.
>
> **viewtype** [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)
>
> * **Choice Archive**
>
>   (no fields)
>
> * **Choice GetRich**
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | byHowMuch                                                                      | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) |  |
>
> * **Choice Noop**
>
>   | Field   | Type    | Description |
>   | :------ | :------ | :---------- |
>   | nothing | ()      |  |
>
> * **Choice Split**
>
>   An interface choice comment.
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | splitAmount                                                                    | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) | A choice field comment. |
>
> * **Choice Transfer**
>
>   | Field                                                                                   | Type                                                                                    | Description |
>   | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
>   | newOwner                                                                                | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
>
> * **Method getAmount :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)
>
> * **Method getOwner :** [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)
>
>   A method comment.
>
> * **Method noopImpl :** () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
>
> * **Method setAmount :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-interface-token-10651)
>
> * **Method splitImpl :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))
>
> * **Method transferImpl :** [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))

## Data Types

<a name="type-interface-emptyinterfaceview-28816"></a>**data** [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)

> <a name="constr-interface-emptyinterfaceview-1101"></a>[EmptyInterfaceView](#constr-interface-emptyinterfaceview-1101)
>
> > (no fields)
>
> **instance** [HasFromAnyView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasfromanyview-19685) [Token](#type-interface-token-10651) [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)
>
> **instance** [HasInterfaceView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492) [Token](#type-interface-token-10651) [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)
>
> **instance** [HasToAnyView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hastoanyview-73434) [Token](#type-interface-token-10651) [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)

## Functions

<a name="function-interface-getowner-36980"></a>[getOwner](#function-interface-getowner-36980)

> : [Token](#type-interface-token-10651) -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)

<a name="function-interface-getamount-416"></a>[getAmount](#function-interface-getamount-416)

> : [Token](#type-interface-token-10651) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)

<a name="function-interface-setamount-37812"></a>[setAmount](#function-interface-setamount-37812)

> : [Token](#type-interface-token-10651) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-interface-token-10651)

<a name="function-interface-splitimpl-93694"></a>[splitImpl](#function-interface-splitimpl-93694)

> : [Token](#type-interface-token-10651) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))

<a name="function-interface-transferimpl-36342"></a>[transferImpl](#function-interface-transferimpl-36342)

> : [Token](#type-interface-token-10651) -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))

<a name="function-interface-noopimpl-41891"></a>[noopImpl](#function-interface-noopimpl-41891)

> : [Token](#type-interface-token-10651) -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
