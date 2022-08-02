# <a name="module-interfaceretro-19712"></a>Module InterfaceRetro

## Templates

<a name="type-interfaceretro-asset-16573"></a>**template** [Asset](#type-interfaceretro-asset-16573)

> | Field                                                                                   | Type                                                                                    | Description |
> | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> | issuer                                                                                  | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | owner                                                                                   | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
> | amount                                                                                  | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)          |  |
> 
> * **Choice Archive**
>   
>   (no fields)

## Interfaces

<a name="type-interfaceretro-token-18810"></a>**interface** [Token](#type-interfaceretro-token-18810)

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
> * **Method setAmount : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-interfaceretro-token-18810)
> 
> * **Method splitImpl : **[Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interfaceretro-token-18810), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interfaceretro-token-18810))
> 
> * **Method transferImpl : **[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interfaceretro-token-18810))

## Functions

<a name="function-interfaceretro-getowner-81603"></a>[getOwner](#function-interfaceretro-getowner-81603)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interfaceretro-token-18810) =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)

<a name="function-interfaceretro-getamount-86729"></a>[getAmount](#function-interfaceretro-getamount-86729)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interfaceretro-token-18810) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)

<a name="function-interfaceretro-setamount-8125"></a>[setAmount](#function-interfaceretro-setamount-8125)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interfaceretro-token-18810) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Token](#type-interfaceretro-token-18810)

<a name="function-interfaceretro-splitimpl-8403"></a>[splitImpl](#function-interfaceretro-splitimpl-8403)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interfaceretro-token-18810) =\> t -\> [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interfaceretro-token-18810), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interfaceretro-token-18810))

<a name="function-interfaceretro-transferimpl-65645"></a>[transferImpl](#function-interfaceretro-transferimpl-65645)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interfaceretro-token-18810) =\> t -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Token](#type-interfaceretro-token-18810))

<a name="function-interfaceretro-noopimpl-10964"></a>[noopImpl](#function-interfaceretro-noopimpl-10964)

> : [Implements](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-interface-implements-92077) t [Token](#type-interfaceretro-token-18810) =\> t -\> () -\> [Update](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-update-68072) ()

<a name="function-interfaceretro-coimplementsassettoken-42965"></a>[\_coimplements\_Asset\_Token](#function-interfaceretro-coimplementsassettoken-42965)

> : CoImplementsT [Asset](#type-interfaceretro-asset-16573) [Token](#type-interfaceretro-token-18810)
