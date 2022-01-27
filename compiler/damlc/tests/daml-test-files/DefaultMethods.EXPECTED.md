# <a name="module-defaultmethods-34992"></a>Module DefaultMethods

## Typeclasses

<a name="class-defaultmethods-d-39130"></a>**class** [D](#class-defaultmethods-d-39130) a **where**

> <a name="function-defaultmethods-x-53637"></a>[x](#function-defaultmethods-x-53637)
> 
> > : a
> 
> <a name="function-defaultmethods-y-51560"></a>[y](#function-defaultmethods-y-51560)
> 
> > : a

<a name="class-defaultmethods-foldablex-43965"></a>**class** [FoldableX](#class-defaultmethods-foldablex-43965) t **where**

> <a name="function-defaultmethods-foldrx-50503"></a>[foldrX](#function-defaultmethods-foldrx-50503)
> 
> > : (a -\> b -\> b) -\> b -\> t a -\> b

<a name="class-defaultmethods-traversablex-84604"></a>**class** ([Functor](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205) t, [FoldableX](#class-defaultmethods-foldablex-43965) t) =\> [TraversableX](#class-defaultmethods-traversablex-84604) t **where**

> <a name="function-defaultmethods-traversex-89947"></a>[traverseX](#function-defaultmethods-traversex-89947)
> 
> > : [Applicative](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257) m =\> (a -\> m b) -\> t a -\> m (t b)
> 
> <a name="function-defaultmethods-sequencex-92456"></a>[sequenceX](#function-defaultmethods-sequencex-92456)
> 
> > : [Applicative](https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257) m =\> t (m a) -\> m (t a)

<a name="class-defaultmethods-id-10050"></a>**class** [Id](#class-defaultmethods-id-10050) a **where**

> <a name="function-defaultmethods-id-52623"></a>[id](#function-defaultmethods-id-52623)
> 
> > : a -\> a
> 
> **instance** [Id](#class-defaultmethods-id-10050) [Int](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261)

<a name="class-defaultmethods-myshow-63060"></a>**class** [MyShow](#class-defaultmethods-myshow-63060) t **where**

> Default implementation with a separate type signature for the default method.
> 
> <a name="function-defaultmethods-myshow-32065"></a>[myShow](#function-defaultmethods-myshow-32065)
> 
> > : t -\> [Text](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952)
> > 
> > Doc for method.
> 
> **default** myShow
> 
> > : [Show](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360) t =\> t -\> [Text](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952)
> > 
> > Doc for default.
