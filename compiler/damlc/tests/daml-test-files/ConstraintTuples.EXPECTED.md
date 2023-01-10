# <a name="module-constrainttuples-79635"></a>ConstraintTuples

## Data Types

<a name="type-constrainttuples-eq2-29566"></a>**type** [Eq2](#type-constrainttuples-eq2-29566) a b

> = ([Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) a, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) b)

<a name="type-constrainttuples-eq3-18799"></a>**type** [Eq3](#type-constrainttuples-eq3-18799) a b c

> = ([Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) a, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) b, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) c)

<a name="type-constrainttuples-eq4-25412"></a>**type** [Eq4](#type-constrainttuples-eq4-25412) a b c d

> = ([Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) a, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) b, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) c, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) d)

## Functions

<a name="function-constrainttuples-eq2-16370"></a>[eq2](#function-constrainttuples-eq2-16370)

> : [Eq2](#type-constrainttuples-eq2-29566) a b =\> a -\> a -\> b -\> b -\> [Bool](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265)

<a name="function-constrainttuples-eq2tick-63686"></a>[eq2'](#function-constrainttuples-eq2tick-63686)

> : ([Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) a, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) b) =\> a -\> a -\> b -\> b -\> [Bool](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265)

<a name="function-constrainttuples-eq3-5603"></a>[eq3](#function-constrainttuples-eq3-5603)

> : [Eq3](#type-constrainttuples-eq3-18799) a b c =\> a -\> a -\> b -\> b -\> c -\> c -\> [Bool](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265)

<a name="function-constrainttuples-eq3tick-36081"></a>[eq3'](#function-constrainttuples-eq3tick-36081)

> : ([Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) a, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) b, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) c) =\> a -\> a -\> b -\> b -\> c -\> c -\> [Bool](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265)

<a name="function-constrainttuples-eq4-77456"></a>[eq4](#function-constrainttuples-eq4-77456)

> : [Eq4](#type-constrainttuples-eq4-25412) a b c d =\> a -\> a -\> b -\> b -\> c -\> c -\> d -\> d -\> [Bool](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265)

<a name="function-constrainttuples-eq4tick-59016"></a>[eq4'](#function-constrainttuples-eq4tick-59016)

> : ([Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) a, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) b, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) c, [Eq](https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713) d) =\> a -\> a -\> b -\> b -\> c -\> c -\> d -\> d -\> [Bool](https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265)
