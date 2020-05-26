# <a name="module-constrainttuples-44760"></a>Module ConstraintTuples

## Data Types

<a name="type-constrainttuples-eq2-31733"></a>**type** [Eq2](#type-constrainttuples-eq2-31733) a b

> = ([Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) a, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) b)

<a name="type-constrainttuples-eq3-75180"></a>**type** [Eq3](#type-constrainttuples-eq3-75180) a b c

> = ([Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) a, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) b, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) c)

<a name="type-constrainttuples-eq4-2935"></a>**type** [Eq4](#type-constrainttuples-eq4-2935) a b c d

> = ([Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) a, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) b, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) c, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) d)

## Functions

<a name="function-constrainttuples-eq2-12289"></a>[eq2](#function-constrainttuples-eq2-12289)

> : [Eq2](#type-constrainttuples-eq2-31733) a b =\> a -\> a -\> b -\> b -\> [Bool](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654)

<a name="function-constrainttuples-eq2tick-62955"></a>[eq2'](#function-constrainttuples-eq2tick-62955)

> : ([Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) a, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) b) =\> a -\> a -\> b -\> b -\> [Bool](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654)

<a name="function-constrainttuples-eq3-55736"></a>[eq3](#function-constrainttuples-eq3-55736)

> : [Eq3](#type-constrainttuples-eq3-75180) a b c =\> a -\> a -\> b -\> b -\> c -\> c -\> [Bool](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654)

<a name="function-constrainttuples-eq3tick-75648"></a>[eq3'](#function-constrainttuples-eq3tick-75648)

> : ([Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) a, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) b, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) c) =\> a -\> a -\> b -\> b -\> c -\> c -\> [Bool](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654)

<a name="function-constrainttuples-eq4-56779"></a>[eq4](#function-constrainttuples-eq4-56779)

> : [Eq4](#type-constrainttuples-eq4-2935) a b c d =\> a -\> a -\> b -\> b -\> c -\> c -\> d -\> d -\> [Bool](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654)

<a name="function-constrainttuples-eq4tick-50089"></a>[eq4'](#function-constrainttuples-eq4tick-50089)

> : ([Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) a, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) b, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) c, [Eq](https://docs.daml.com/daml/stdlib/index.html#class-ghc-classes-eq-21216) d) =\> a -\> a -\> b -\> b -\> c -\> c -\> d -\> d -\> [Bool](https://docs.daml.com/daml/stdlib/index.html#type-ghc-types-bool-8654)
