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

> * **implements** [Token](#type-interface-token-10651)

## Interfaces

<a name="type-interface-token-10651"></a>**interface** [Token](#type-interface-token-10651)

> An interface comment.
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
> * **Method getAmount : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)
> 
> * **Method getOwner : **[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)
>   
>   A method comment.
> 
> * **Method noopImpl : **() -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
> 
> * **Method setAmount : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-interface-token-10651)
> 
> * **Method splitImpl : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))
> 
> * **Method transferImpl : **[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))

## Functions

<a name="function-interface-getowner-36980"></a>[getOwner](#function-interface-getowner-36980)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interface-token-10651) =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)

<a name="function-interface-getamount-416"></a>[getAmount](#function-interface-getamount-416)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interface-token-10651) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)

<a name="function-interface-setamount-37812"></a>[setAmount](#function-interface-setamount-37812)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interface-token-10651) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-interface-token-10651)

<a name="function-interface-splitimpl-93694"></a>[splitImpl](#function-interface-splitimpl-93694)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interface-token-10651) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))

<a name="function-interface-transferimpl-36342"></a>[transferImpl](#function-interface-transferimpl-36342)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interface-token-10651) =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interface-token-10651))

<a name="function-interface-noopimpl-41891"></a>[noopImpl](#function-interface-noopimpl-41891)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interface-token-10651) =\> t -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()
