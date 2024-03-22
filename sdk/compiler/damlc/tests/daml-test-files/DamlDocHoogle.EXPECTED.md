# <a name="module-damldochoogle-19200"></a>DamlDocHoogle

## Templates

<a name="type-damldochoogle-t-56109"></a>**template** [T](#type-damldochoogle-t-56109)

> T docs
>
> Signatory: p
>
> | Field                                                                                   | Type                                                                                    | Description |
> | :-------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------- | :---------- |
> | p                                                                                       | [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932) |  |
>
> * **Choice** Archive
>
>   Controller: p
>
>   Returns: ()
>
>   (no fields)
>
> * <a name="type-damldochoogle-tcall-44069"></a>**Choice** [TCall](#type-damldochoogle-tcall-44069)
>
>   TCall docs
>
>   Controller: p
>
>   Returns: ()
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | i                                                                              | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) |  |

## Interfaces

<a name="type-damldochoogle-i-6172"></a>**interface** [I](#type-damldochoogle-i-6172)

> I docs
>
> **viewtype** [View](#type-damldochoogle-view-20961)
>
> * **Choice** Archive
>
>   Controller: Signatories of implementing template
>
>   Returns: ()
>
>   (no fields)
>
> * <a name="type-damldochoogle-icall-37460"></a>**Choice** [ICall](#type-damldochoogle-icall-37460)
>
>   ICall docs
>
>   Controller: getController this
>
>   Returns: [Optional](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153) [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)
>
>   | Field                                                                          | Type                                                                           | Description |
>   | :----------------------------------------------------------------------------- | :----------------------------------------------------------------------------- | :---------- |
>   | i                                                                              | [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261) |  |
>
> * **Method getController :** [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)
>
>   getController docs

## Data Types

<a name="type-damldochoogle-view-20961"></a>**data** [View](#type-damldochoogle-view-20961)

> View docs
>
> <a name="constr-damldochoogle-view-98884"></a>[View](#constr-damldochoogle-view-98884)
>
> > (no fields)
>
> **instance** [HasFromAnyView](https://docs.daml.com/daml/stdlib/DA-Internal-Interface-AnyView.html#class-da-internal-interface-anyview-hasfromanyview-30108) [I](#type-damldochoogle-i-6172) [View](#type-damldochoogle-view-20961)
>
> **instance** [HasInterfaceView](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492) [I](#type-damldochoogle-i-6172) [View](#type-damldochoogle-view-20961)

## Functions

<a name="function-damldochoogle-getcontroller-29001"></a>[getController](#function-damldochoogle-getcontroller-29001)

> : [I](#type-damldochoogle-i-6172) -\> [Party](https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932)
