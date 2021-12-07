# <a name="module-interface-11558"></a>Module Interface

## Templates

<a name="type-interface-asset-14509"></a>**template** [Asset](#type-interface-asset-14509)

> | Field                                                                                   | Type                                                                                    | Description |
> | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> | issuer                                                                                  | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) |  |
> | owner                                                                                   | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) |  |
> | amount                                                                                  | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728)          |  |
> 
> * **Choice Archive**
>   
>   (no fields)

> * **implements** [Token](#type-interface-token-72202)

## Interfaces

<a name="type-interface-token-72202"></a>**interface** [Token](#type-interface-token-72202)

> An interface comment.
> 
> * **Choice GetRich**
>   
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | byHowMuch                                                                      | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) |  |
> 
> * **Choice Noop**
>   
>   | Field   | Type    | Description |
>   | :------ | :------ | :---------- |
>   | nothing | ()      |  |
> 
> * **Choice Split**
>   
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | splitAmount                                                                    | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) | A choice field comment. |
> 
> * **Choice Transfer**
>   
>   | Field                                                                                   | Type                                                                                    | Description |
>   | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
>   | newOwner                                                                                | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) |  |
> 
> * **Method getAmount : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728)
> 
> * **Method getOwner : **[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311)
> 
> * **Method noopImpl : **() -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ()
> 
> * **Method setAmount : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) -\> [Token](#type-interface-token-72202)
> 
> * **Method splitImpl : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Token](#type-interface-token-72202), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Token](#type-interface-token-72202))
> 
> * **Method transferImpl : **[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Token](#type-interface-token-72202))

## Functions

<a name="function-interface-noopimpl-83220"></a>[noopImpl](#function-interface-noopimpl-83220)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034) t [Token](#type-interface-token-72202) =\> t -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ()

<a name="function-interface-transferimpl-81005"></a>[transferImpl](#function-interface-transferimpl-81005)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034) t [Token](#type-interface-token-72202) =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Token](#type-interface-token-72202))

<a name="function-interface-splitimpl-48531"></a>[splitImpl](#function-interface-splitimpl-48531)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034) t [Token](#type-interface-token-72202) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Token](#type-interface-token-72202), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Token](#type-interface-token-72202))

<a name="function-interface-setamount-71357"></a>[setAmount](#function-interface-setamount-71357)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034) t [Token](#type-interface-token-72202) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) -\> [Token](#type-interface-token-72202)

<a name="function-interface-getamount-93321"></a>[getAmount](#function-interface-getamount-93321)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034) t [Token](#type-interface-token-72202) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728)

<a name="function-interface-getowner-9315"></a>[getOwner](#function-interface-getowner-9315)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-77034) t [Token](#type-interface-token-72202) =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311)
