# <a name="module-iou12-32397"></a>Module Iou12

## Templates

<a name="type-iou12-iou-45923"></a>**template** [Iou](#type-iou12-iou-45923)

> | Field                                                                                       | Type                                                                                        | Description |
> | :------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------ | :---------- |
> | issuer                                                                                      | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311)     |  |
> | owner                                                                                       | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311)     |  |
> | currency                                                                                    | [Text](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-57703)            | only 3-letter symbols are allowed |
> | amount                                                                                      | [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-54602)      | must be positive |
> | regulators                                                                                  | \[[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311)\] | `regulators` may observe any use of the `Iou` |
> 
> * **Choice Archive**
>   
>   (no fields)
> 
> * **Choice DoNothing**
>   
>   (no fields)
> 
> * **Choice Merge**
>   
>   merges two "compatible" `Iou`s
>   
>   | Field                                                                                                                          | Type                                                                                                                           | Description |
>   | :----------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------- | :---------- |
>   | otherCid                                                                                                                       | [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-47171) [Iou](#type-iou12-iou-45923) | Must have same owner, issuer, and currency. The regulators may differ, and are taken from the original `Iou`. |
> 
> * **Choice Split**
>   
>   splits into two `Iou`s with smaller amounts
>   
>   | Field                                                                                  | Type                                                                                   | Description |
>   | :------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------- | :---------- |
>   | splitAmount                                                                            | [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-54602) | must be between zero and original amount |
> 
> * **Choice Transfer**
>   
>   changes the owner
>   
>   | Field                                                                                   | Type                                                                                    | Description |
>   | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
>   | newOwner                                                                                | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-50311) |  |

## Functions

<a name="function-iou12-main-35518"></a>[main](#function-iou12-main-35518)

> : [Scenario](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-scenario-45418) ()
> 
> A single test scenario covering all functionality that `Iou` implements.
> This description contains [a link](http://example.com), some bogus <inline html>,
> and words_ with_ underscore, to test damldoc capabilities.
