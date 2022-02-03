# <a name="module-deriving-95739"></a>Module Deriving

## Data Types

<a name="type-deriving-formula-60264"></a>**data** [Formula](#type-deriving-formula-60264) t

> <a name="constr-deriving-tautology-1247"></a>[Tautology](#constr-deriving-tautology-1247)
> 
> 
> <a name="constr-deriving-contradiction-64078"></a>[Contradiction](#constr-deriving-contradiction-64078)
> 
> 
> <a name="constr-deriving-proposition-76435"></a>[Proposition](#constr-deriving-proposition-76435) t
> 
> 
> <a name="constr-deriving-negation-39767"></a>[Negation](#constr-deriving-negation-39767) ([Formula](#type-deriving-formula-60264) t)
> 
> 
> <a name="constr-deriving-conjunction-55851"></a>[Conjunction](#constr-deriving-conjunction-55851) \[[Formula](#type-deriving-formula-60264) t\]
> 
> 
> <a name="constr-deriving-disjunction-19371"></a>[Disjunction](#constr-deriving-disjunction-19371) \[[Formula](#type-deriving-formula-60264) t\]
> 
> 
> **instance** [Functor](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205) [Formula](#type-deriving-formula-60264)
> 
> **instance** [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) t =\> [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) ([Formula](#type-deriving-formula-60264) t)
> 
> **instance** [Ord](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395) t =\> [Ord](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395) ([Formula](#type-deriving-formula-60264) t)
> 
> **instance** [Show](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360) t =\> [Show](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360) ([Formula](#type-deriving-formula-60264) t)
