# <a name="module-defaultmethods-97307"></a>Module DefaultMethods

## Typeclasses

<a name="class-defaultmethods-d-4635"></a>**class** [D](#class-defaultmethods-d-4635) a **where**

> <a name="function-defaultmethods-x-92038"></a>[x](#function-defaultmethods-x-92038)
> 
> > : a
> 
> <a name="function-defaultmethods-y-38115"></a>[y](#function-defaultmethods-y-38115)
> 
> > : a

<a name="class-defaultmethods-foldablex-48748"></a>**class** [FoldableX](#class-defaultmethods-foldablex-48748) t **where**

> <a name="function-defaultmethods-foldrx-33654"></a>[foldrX](#function-defaultmethods-foldrx-33654)
> 
> > : (a -\> b -\> b) -\> b -\> t a -\> b

<a name="class-defaultmethods-traversablex-59027"></a>**class** ([Functor](https://docs.daml.com/daml/stdlib/index.html#class-ghc-base-functor-73448) t, [FoldableX](#class-defaultmethods-foldablex-48748) t) =\> [TraversableX](#class-defaultmethods-traversablex-59027) t **where**

> <a name="function-defaultmethods-traversex-21140"></a>[traverseX](#function-defaultmethods-traversex-21140)
> 
> > : Applicative m =\> (a -\> m b) -\> t a -\> m (t b)
> 
> <a name="function-defaultmethods-sequencex-86855"></a>[sequenceX](#function-defaultmethods-sequencex-86855)
> 
> > : Applicative m =\> t (m a) -\> m (t a)

<a name="class-defaultmethods-id-77721"></a>**class** [Id](#class-defaultmethods-id-77721) a **where**

> <a name="function-defaultmethods-id-57162"></a>[id](#function-defaultmethods-id-57162)
> 
> > : a -\> a
> 
> **instance** [Id](#class-defaultmethods-id-77721) [Int](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-int-68728)

<a name="class-defaultmethods-myshow-63359"></a>**class** [MyShow](#class-defaultmethods-myshow-63359) t **where**

> Default implementation with a separate type signature for the default method.
> 
> <a name="function-defaultmethods-myshow-41356"></a>[myShow](#function-defaultmethods-myshow-41356)
> 
> > : t -\> [Text](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-text-57703)
> > 
> > Doc for method.
> 
> **default** myShow
> 
> > : [Show](https://docs.daml.com/daml/stdlib/index.html#class-ghc-show-show-56447) t =\> t -\> [Text](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-text-57703)
> > 
> > Doc for default.
