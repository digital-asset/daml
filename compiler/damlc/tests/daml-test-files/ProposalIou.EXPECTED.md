# <a name="module-proposaliou-96142"></a>Module ProposalIou

## Templates

<a name="type-proposaliou-iou-51326"></a>**template** [Iou](#type-proposaliou-iou-51326)

> | Field                                                                                  | Type                                                                                   | Description |
> | :------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------- | :---------- |
> | issuer                                                                                 | Party                                                                                  |  |
> | owner                                                                                  | Party                                                                                  |  |
> | amount                                                                                 | [Decimal](https://docs.daml.com/daml/reference/base.html#type-ghc-types-decimal-54602) |  |
> 
> * **Choice External:Archive**
>   
>   (no fields)
> 
> * **Choice Burn**
>   
>   (no fields)

## Template Instances

<a name="type-proposaliou-proposaliou-81988"></a>**template instance** [ProposalIou](#type-proposaliou-proposaliou-81988)

> = Proposal [Iou](#type-proposaliou-iou-51326)
