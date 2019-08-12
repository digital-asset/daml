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
> **instance** Functor [Formula](#type-deriving-formula-84903)
> 
> **instance** Eq t =\> Eq ([Formula](#type-deriving-formula-84903) t)
> 
> **instance** Ord t =\> Ord ([Formula](#type-deriving-formula-84903) t)
> 
> **instance** Show t =\> Show ([Formula](#type-deriving-formula-84903) t)
