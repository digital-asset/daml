# <a name="module-qualifiedinterface-53968"></a>QualifiedInterface

## Templates

<a name="type-qualifiedinterface-asset-82061"></a>**template** [Asset](#type-qualifiedinterface-asset-82061)

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

> * **interface instance** Token **for** [Asset](#type-qualifiedinterface-asset-82061)
