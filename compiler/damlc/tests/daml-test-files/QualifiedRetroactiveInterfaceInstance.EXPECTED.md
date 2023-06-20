# <a name="module-qualifiedretroactiveinterfaceinstance-76052"></a>QualifiedRetroactiveInterfaceInstance

## Interfaces

<a name="type-qualifiedretroactiveinterfaceinstance-token-43978"></a>**interface** [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978)

> **viewtype** [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
>
> * **Choice** Archive
>
>   Controller: Signatories of implementing template
>
>   Returns: ()
>
>   (no fields)
>
> * <a name="type-qualifiedretroactiveinterfaceinstance-getrich-86333"></a>**Choice** [GetRich](#type-qualifiedretroactiveinterfaceinstance-getrich-86333)
>
>   Controller: (DA.Internal.Record.getField @"owner" (view this))
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978)
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | byHowMuch                                                                      | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) |  |
>
> * <a name="type-qualifiedretroactiveinterfaceinstance-noop-81106"></a>**Choice** [Noop](#type-qualifiedretroactiveinterfaceinstance-noop-81106)
>
>   Controller: (DA.Internal.Record.getField @"owner" (view this))
>
>   Returns: ()
>
>   | Field   | Type    | Description |
>   | :------ | :------ | :---------- |
>   | nothing | ()      |  |
>
> * <a name="type-qualifiedretroactiveinterfaceinstance-split-60457"></a>**Choice** [Split](#type-qualifiedretroactiveinterfaceinstance-split-60457)
>
>   An interface choice comment.
>
>   Controller: (DA.Internal.Record.getField @"owner" (view this))
>
>   Returns: ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978))
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | splitAmount                                                                    | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) | A choice field comment. |
>
> * <a name="type-qualifiedretroactiveinterfaceinstance-transfer-84967"></a>**Choice** [Transfer](#type-qualifiedretroactiveinterfaceinstance-transfer-84967)
>
>   Controller: (DA.Internal.Record.getField @"owner" (view this)), newOwner
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978)
>
>   | Field                                                                                   | Type                                                                                    | Description |
>   | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
>   | newOwner                                                                                | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
>
> * **Method noopImpl :** () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
>
> * **Method setAmount :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978)
>
> * **Method splitImpl :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978))
>
> * **Method transferImpl :** [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978))

> * **interface instance** [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) **for** Asset

## Data Types

<a name="type-qualifiedretroactiveinterfaceinstance-tokenview-25557"></a>**data** [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)

> <a name="constr-qualifiedretroactiveinterfaceinstance-tokenview-72346"></a>[TokenView](#constr-qualifiedretroactiveinterfaceinstance-tokenview-72346)
>
> > | Field                                                                                   | Type                                                                                    | Description |
> > | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> > | owner                                                                                   | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> > | amount                                                                                  | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)          |  |
>
> **instance** [HasFromAnyView](https://docs.daml.com/daml/stdlib/DA-Internal-Interface-AnyView.html#class-da-internal-interface-anyview-hasfromanyview-30108) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
>
> **instance** [HasInterfaceView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
>
> **instance** [HasField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839) "amount" [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557) [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)
>
> **instance** [HasField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839) "owner" [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557) [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)

## Functions

<a name="function-qualifiedretroactiveinterfaceinstance-setamount-51253"></a>[setAmount](#function-qualifiedretroactiveinterfaceinstance-setamount-51253)

> : [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978)

<a name="function-qualifiedretroactiveinterfaceinstance-splitimpl-65579"></a>[splitImpl](#function-qualifiedretroactiveinterfaceinstance-splitimpl-65579)

> : [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978))

<a name="function-qualifiedretroactiveinterfaceinstance-transferimpl-9125"></a>[transferImpl](#function-qualifiedretroactiveinterfaceinstance-transferimpl-9125)

> : [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978))

<a name="function-qualifiedretroactiveinterfaceinstance-noopimpl-17100"></a>[noopImpl](#function-qualifiedretroactiveinterfaceinstance-noopimpl-17100)

> : [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
