# <a name="module-interface-72439"></a>Interface

## Templates

<a name="type-interface-asset-25340"></a>**template** [Asset](#type-interface-asset-25340)

> Signatory: issuer, owner
>
> | Field                                                                                   | Type                                                                                    | Description |
> | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> | issuer                                                                                  | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | owner                                                                                   | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | amount                                                                                  | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)          |  |
>
> * **Choice** Archive
>
>   Controller: issuer, owner
>
>   Returns: ()
>
>   (no fields)

> * **interface instance** [Token](#type-interface-token-10651) **for** [Asset](#type-interface-asset-25340)

## Interfaces

<a name="type-interface-token-10651"></a>**interface** [Token](#type-interface-token-10651)

> An interface comment.
>
> **viewtype** [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)
>
> * **Choice** Archive
>
>   Controller: Signatories of implementing template
>
>   Returns: ()
>
>   (no fields)
>
> * <a name="type-interface-getrich-60188"></a>**Choice** [GetRich](#type-interface-getrich-60188)
>
>   Controller: getOwner this
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651)
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | byHowMuch                                                                      | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) |  |
>
> * <a name="type-interface-noop-44317"></a>**Choice** [Noop](#type-interface-noop-44317)
>
>   Controller: getOwner this
>
>   Returns: ()
>
>   | Field   | Type    | Description |
>   | :------ | :------ | :---------- |
>   | nothing | ()      |  |
>
> * <a name="type-interface-split-56016"></a>**Choice** [Split](#type-interface-split-56016)
>
>   An interface choice comment.
>
>   Controller: getOwner this
>
>   Returns: ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | splitAmount                                                                    | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) | A choice field comment. |
>
> * <a name="type-interface-transfer-15068"></a>**Choice** [Transfer](#type-interface-transfer-15068)
>
>   Controller: getOwner this, newOwner
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651)
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
> **instance** [HasFromAnyView](https://docs.daml.com/daml/stdlib/DA-Internal-Interface-AnyView.html#class-da-internal-interface-anyview-hasfromanyview-30108) [Token](#type-interface-token-10651) [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)
>
> **instance** [HasInterfaceView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492) [Token](#type-interface-token-10651) [EmptyInterfaceView](#type-interface-emptyinterfaceview-28816)

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
