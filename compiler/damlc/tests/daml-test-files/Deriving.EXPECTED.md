# <a name="module-deriving-95364"></a>Module Deriving

## Data Types

<a name="type-deriving-formula-84903"></a>**data** [Formula](#type-deriving-formula-84903) t

> <a name="constr-deriving-tautology-41024"></a>[Tautology](#constr-deriving-tautology-41024)
> 
> 
> <a name="constr-deriving-contradiction-93645"></a>[Contradiction](#constr-deriving-contradiction-93645)
> 
> 
> <a name="constr-deriving-proposition-99264"></a>[Proposition](#constr-deriving-proposition-99264) t
> 
> 
> <a name="constr-deriving-negation-52326"></a>[Negation](#constr-deriving-negation-52326) ([Formula](#type-deriving-formula-84903) t)
> 
> 
> <a name="constr-deriving-conjunction-36676"></a>[Conjunction](#constr-deriving-conjunction-36676) \[[Formula](#type-deriving-formula-84903) t\]
> 
> 
> <a name="constr-deriving-disjunction-94592"></a>[Disjunction](#constr-deriving-disjunction-94592) \[[Formula](#type-deriving-formula-84903) t\]
> 
> 
> **instance** [Functor](https://docs.daml.com/daml/stdlib/index.html#class-ghc-base-functor-73448) [Formula](#type-deriving-formula-84903)
> 
> **instance** [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) t =\> [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) ([Formula](#type-deriving-formula-84903) t)
> 
> **instance** [Ord](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-ord-70960) t =\> [Ord](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-ord-70960) ([Formula](#type-deriving-formula-84903) t)
> 
> **instance** [Show](https://docs.daml.com/daml/stdlib/index.html#class-ghc-show-show-56447) t =\> [Show](https://docs.daml.com/daml/stdlib/index.html#class-ghc-show-show-56447) ([Formula](#type-deriving-formula-84903) t)
