# <a name="module-iou12-76192"></a>Iou12

## Templates

<a name="type-iou12-iou-72962"></a>**template** [Iou](#type-iou12-iou-72962)

> Signatory: issuer
>
> | Field                                                                                       | Type                                                                                        | Description |
> | :------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------ | :---------- |
> | issuer                                                                                      | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)     |  |
> | owner                                                                                       | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)     |  |
> | currency                                                                                    | [Text](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952)            | only 3-letter symbols are allowed |
> | amount                                                                                      | [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135)      | must be positive |
> | regulators                                                                                  | \[[Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)\] | `regulators` may observe any use of the `Iou` |
>
> * **Choice** Archive
>
>   Controller: issuer
>
>   Returns: ()
>
>   (no fields)
>
> * <a name="type-iou12-donothing-75627"></a>**Choice** [DoNothing](#type-iou12-donothing-75627)
>
>   Controller: owner
>
>   Returns: ()
>
>   (no fields)
>
> * <a name="type-iou12-merge-98901"></a>**Choice** [Merge](#type-iou12-merge-98901)
>
>   merges two "compatible" `Iou`s
>
>   Controller: owner
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Iou](#type-iou12-iou-72962)
>
>   | Field                                                                                                                          | Type                                                                                                                           | Description |
>   | :----------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------- | :---------- |
>   | otherCid                                                                                                                       | [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Iou](#type-iou12-iou-72962) | Must have same owner, issuer, and currency. The regulators may differ, and are taken from the original `Iou`. |
>
> * <a name="type-iou12-split-33517"></a>**Choice** [Split](#type-iou12-split-33517)
>
>   splits into two `Iou`s with smaller amounts
>
>   Controller: owner
>
>   Returns: ([ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Iou](#type-iou12-iou-72962), [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Iou](#type-iou12-iou-72962))
>
>   | Field                                                                                  | Type                                                                                   | Description |
>   | :------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------- | :---------- |
>   | splitAmount                                                                            | [Decimal](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-decimal-18135) | must be between zero and original amount |
>
> * <a name="type-iou12-transfer-99339"></a>**Choice** [Transfer](#type-iou12-transfer-99339)
>
>   changes the owner
>
>   Controller: owner
>
>   Returns: [ContractId](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282) [Iou](#type-iou12-iou-72962)
>
>   | Field                                                                                   | Type                                                                                    | Description |
>   | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
>   | newOwner                                                                                | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |

## Functions

<a name="function-iou12-main-28537"></a>[main](#function-iou12-main-28537)

> : Script ()
>
> A single test case covering all functionality that `Iou` implements.
> This description contains [a link](http://example.com), some bogus <inline html>,
> and words_ with_ underscore, to test damldoc capabilities.
