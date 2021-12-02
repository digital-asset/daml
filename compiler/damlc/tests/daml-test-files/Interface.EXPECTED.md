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

> * **implements** Token

## Functions

<a name="function-interface-noopimpl-83220"></a>[noopImpl](#function-interface-noopimpl-83220)

> : Implements t Token =\> t -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ()

<a name="function-interface-transferimpl-81005"></a>[transferImpl](#function-interface-transferimpl-81005)

> : Implements t Token =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) Token)

<a name="function-interface-splitimpl-48531"></a>[splitImpl](#function-interface-splitimpl-48531)

> : Implements t Token =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-36457) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) Token, [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) Token)

<a name="function-interface-setamount-71357"></a>[setAmount](#function-interface-setamount-71357)

> : Implements t Token =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728) -\> Token

<a name="function-interface-getamount-93321"></a>[getAmount](#function-interface-getamount-93321)

> : Implements t Token =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-68728)

<a name="function-interface-getowner-9315"></a>[getOwner](#function-interface-getowner-9315)

> : Implements t Token =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311)
