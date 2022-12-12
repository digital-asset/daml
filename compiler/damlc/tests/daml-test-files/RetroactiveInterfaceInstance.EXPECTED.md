# <a name="module-retroactiveinterfaceinstance-60009"></a>Module RetroactiveInterfaceInstance

## Interfaces

<a name="type-retroactiveinterfaceinstance-token-49693"></a>**interface** [Token](#type-retroactiveinterfaceinstance-token-49693)

> **viewtype** [TokenView](#type-retroactiveinterfaceinstance-tokenview-57374)
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
> * **Method setAmount :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-retroactiveinterfaceinstance-token-49693)
>
> * **Method splitImpl :** [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-retroactiveinterfaceinstance-token-49693), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-retroactiveinterfaceinstance-token-49693))
>
> * **Method transferImpl :** [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-retroactiveinterfaceinstance-token-49693))

> * **interface instance** [Token](#type-retroactiveinterfaceinstance-token-49693) **for** Asset

## Data Types

<a name="type-retroactiveinterfaceinstance-tokenview-57374"></a>**data** [TokenView](#type-retroactiveinterfaceinstance-tokenview-57374)

> <a name="constr-retroactiveinterfaceinstance-tokenview-95763"></a>[TokenView](#constr-retroactiveinterfaceinstance-tokenview-95763)
>
> > | Field                                                                                   | Type                                                                                    | Description |
> > | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> > | owner                                                                                   | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> > | amount                                                                                  | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)          |  |
>
> **instance** [HasFromAnyView](https://docs.daml.com/daml/stdlib/DA-Internal-Interface-AnyView.html#class-da-internal-interface-anyview-hasfromanyview-30108) [Token](#type-retroactiveinterfaceinstance-token-49693) [TokenView](#type-retroactiveinterfaceinstance-tokenview-57374)
>
> **instance** [HasInterfaceView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492) [Token](#type-retroactiveinterfaceinstance-token-49693) [TokenView](#type-retroactiveinterfaceinstance-tokenview-57374)
>
> **instance** [HasField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839) "amount" [TokenView](#type-retroactiveinterfaceinstance-tokenview-57374) [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)
>
> **instance** [HasField](https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-hasfield-52839) "owner" [TokenView](#type-retroactiveinterfaceinstance-tokenview-57374) [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)

## Functions

<a name="function-retroactiveinterfaceinstance-setamount-92750"></a>[setAmount](#function-retroactiveinterfaceinstance-setamount-92750)

> : [Token](#type-retroactiveinterfaceinstance-token-49693) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-retroactiveinterfaceinstance-token-49693)

<a name="function-retroactiveinterfaceinstance-splitimpl-44512"></a>[splitImpl](#function-retroactiveinterfaceinstance-splitimpl-44512)

> : [Token](#type-retroactiveinterfaceinstance-token-49693) -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-retroactiveinterfaceinstance-token-49693), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-retroactiveinterfaceinstance-token-49693))

<a name="function-retroactiveinterfaceinstance-transferimpl-49252"></a>[transferImpl](#function-retroactiveinterfaceinstance-transferimpl-49252)

> : [Token](#type-retroactiveinterfaceinstance-token-49693) -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-retroactiveinterfaceinstance-token-49693))

<a name="function-retroactiveinterfaceinstance-noopimpl-82337"></a>[noopImpl](#function-retroactiveinterfaceinstance-noopimpl-82337)

> : [Token](#type-retroactiveinterfaceinstance-token-49693) -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
