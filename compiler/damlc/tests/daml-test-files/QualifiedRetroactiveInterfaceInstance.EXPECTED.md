# <a name="module-qualifiedretroactiveinterfaceinstance-76052"></a>Module QualifiedRetroactiveInterfaceInstance

## Interfaces

<a name="type-qualifiedretroactiveinterfaceinstance-token-43978"></a>**interface** [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978)

> **viewtype** [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
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
> **instance** [HasFromAnyView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasfromanyview-19685) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
>
> **instance** [HasInterfaceView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
>
> **instance** [HasToAnyView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hastoanyview-73434) [Token](#type-qualifiedretroactiveinterfaceinstance-token-43978) [TokenView](#type-qualifiedretroactiveinterfaceinstance-tokenview-25557)
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
