# Lenses, do you get it?

**tl;dr** If you care about performance, use DAML's builtin syntax for accessing record fields.

## Introduction

I guess it is no secret that I'm not the biggest fan of lenses, particularly not in DAML. I wouldn't go as far as saying that lenses, prisms, and the other optics make my eyes bleed but there are definitely better ways to handle records is most cases. One case where the builtin syntax of DAML has a clear advantage over lenses is record access, getting the value of a field from a record:
```
record.field1.field2
```
It doesn't get much clearer or much shorter. No matter what lens library you use, your equivalent code will look something like
```
get (lens @"field1" . lens @"field2") record
```
or maybe
```
get (_field1 . _field2) record
```
if you're willing to define lenses like `_field1` and `_field2` for each record field. Either way, the standard DAML way is hard to beat. And if you need to pass a field accessor around as a function, the `(.field1)` syntax has you covered as well.

Clarity is often in the eye of the beholder and short code is not per se good code. The only thing that seems to matter universally is performance. Well, let's have a look at the performance of both of styles then.

## Van Laarhoven lenses

Before we delve into a series of benchmarks, let's quickly recap [van Laarhoven lenses](https://www.twanvl.nl/blog/haskell/cps-functional-references). The type of a lens for accessing a field of type `a` in a record of type `s` is
```haskell
type Lens s a = forall f. Functor f => (a -> f a) -> (s -> f s)
```
If it wasn't for everything related to `f` in this type, it would be the rather simple `(a -> a) -> (s -> s)`. This looks like the type of a higher-order function that turns a function for changing the value of the field into a function for changing the whole record. That sounds pretty useful in its own right and can also easily be used to implement a setter for the field: just use `\_ -> x` as the function for changing the field in order to set its value to `x`. Alas, it seems totally unclear how we could use such a higher-order function to produce a getter for the field. That's where the `f` comes into play. But before we go into the details of how to implement a getter, let's implement a few lenses first.

Given a record type
```haskell
data Record = Record with field1: Int; ...
```
a lens for `field1` can be defined by
```haskell
_field1: Lens Record Int
_field1 f r = fmap (\x -> r with field1 = x) (f r.field1)
```
In fact, this is the only way we can define a function of this type without using any "magic constants". More generally, a lens can be made out of a getter and a setter for a field in a very generic fashion:
```haskell
makeLens: (s -> a) -> (s -> a -> s) -> Lens s a
makeLens getter setter f r = setter r <$> f (getter r)
```
Again, this is the only way a `Lens s a` can be obtained from `getter` and `setter` without explictly using bottom values, such as `undefined`.

Using DAML's `HasField` class, we can even produce a lens that works for any record field:
```haskell
lens: forall x s a. HasField x s a => Lens s a
lens = makeLens (getField @x) (flip (setField @x))
```
The lens to access `field1` is now written as
```
lens @"field1"
```
**Remark.** In my opinion, the most fascinating fact about van Laarhoven lenses is that you can compose them using the regular function composition operator `(.)` and the field names appear _in the same order_ as when you use the buildin syntax for accessing record fields, as indicated by the example in the introduction.

## Implementing the getter

That's all very nice, but how do we actually use a lens to access a field of a record? As mentioned above, that's where the `f` comes into play. What we are looking for is a function
```
get: Lens s a -> s -> a
get l r = ???
```
Recall that the type `Lens s a` has the shape `forall f. Functor f => ...`. This means the implementation of `get` can choose an arbitrary functor `f` and an arbitrary function of type `(a -> f a)` and pass them as arguments to `l`. The functor that will solve our problem is the so-called "const functor" defined by
```haskell
data Const b a = Const with unConst: b

instance Functor (Const r) where
    fmap _ (Const x) = Const x
```
The key property of `Const` is that `fmap` does pretty much nothing with it (except for changing its type). We can use this to finally implement `get` as follows:
```haskell
get: Lens s a -> s -> a
get l r = (l Const r).unConst
```
With this function, we can get the value of `field1` from an arbitrary record `r` by calling
```
get (lens @"field1") r
```

For the sake of completeness, let's quickly define a setter as well. As insinuated above, we don't really need the `f` for that purpose. That's exactly what the  `Identity` functor is for:
```haskell
data Identity a = Identity with unIdentity: a

instance Functor Identity where
    fmap f (Identity x) = Identity (f x)

set: Lens s a -> a -> s -> s
set l x r = (l (\_ -> Identity x) r).unIdentity
```

## How to micro-benchmark DAML

Micro-benchmarking DAML code is unfortunately still a bit of an art form. We will use the scenario-based benchmarking approach described in the [DAML-LF readme](https://github.com/digital-asset/daml/blob/master/daml-lf/README.md#benchmarking). To this end, we need to write a scenario that runs `get (lens @"field") r` for some record `r`. The benchmark runner will then tell us how long the scenario runs on average.

In order to write such a scenario, we need to take quite a few things into consideration. Let's have a look at the code first and explain the details afterward:
```haskell
records = map Record [1..100_000]         -- (A)

benchLens = scenario do
    _ <- pure ()                          -- (B)
    let step acc r =
            acc + get (lens @"field1") r  -- (C)
    let _ = foldl step 0 records          -- (D)
    pure ()
```

The explanations for the marked lines are as follows:

* **(D)** Running a scenario has some constant overhead. By running the getter 100,000 times, we make this overhead per individual run of the getter negligible. However, folding over a list has some overhead too, including some overhead for each step of the fold. In order to account for this overhead, we use a technique that could be called "differential benchmarking": We run a slightly modified version of the benchmark above, where line (C) is replaced by `acc + r` and line (A) by `records = [1..100_000]`. The difference between both benchmarks will tell us how long it takes to execute `get (lens @"field1") r` 100,000 times.

* **(A)** In order for the differential benchmarking technique to work, we need to compute the value of `records` outside of the actual measurements since allocating a list of 100,000 records takes significantly longer than allocating a list of 100,000 numbers. To this end, we move the definition of `records` to the top-level. The DAML interpreter computes top-level values the first time their value is requested and then caches this value for future requests. The benchmark runner fills these caches by executing the scenario once _before_ measuring.

* **(B)** Due to the aforementioned caching of top-level values and some quirks around the semantics of `do` notation, we need to put our benchmark after at least one `<-` binding. Otherwise, the result of the benchmark would be cached and we would only measure the time for accessing the cache.

* **(C)** We put the code we want to benchmark into a non-trivial context to reflect its expected usage. If we dropped the `acc +`, then `get (lens @"field1") r` would be in tail position of the `step` function and hence not cause a stack allocation. However, in most use cases the `get` function will be part of a more complex expression and its result will be pushed onto the stack. Thus, it seems fair to benchmark the cost of running `get` _plus_ pushing onto the stack. The extra cost of the addition is removed by the differential benchmarking technique.

## First numbers

Recall that the objective of this blog post is to compare lenses to the builtin syntax for accessing record fields in terms of their runtime performance. To this end, we run three benchmarks:

1. `benchLens` as defined above,
2. `benchNoop`, the variant of `benchLens` described under **(D)** above,
3. `benchBuiltin`, a variant of `benchLens` where line (C) is replaced by `acc + r.field1`.

If `T(x)` denotes the time it takes to run benchmark `x`, then we can compute the time a single `get (lens @"field1") r` takes by
```
(T(benchLens) - T(benchNoop)) / 100,000
```
Similarly, the time a single `r.x` takes is determined by
```
(T(benchBuiltin) - T(benchNoop)) / 100,000
```

Running these benchmarks on my laptop produced the following numbers:

|`x`|`T(x)`|`(T(x) - T(benchNoop)) / 100,000`|
|-|-|-|
|`benchNoop`|11.1 ms|-|
|`benchLens`|188.7 ms|1776 ns|
|`benchBuiltin`|15.7 ms|46 ns|

Wow! That means a single record field access using the builtin syntax takes 46 ns whereas doing the same with `get` and `lens` takes 1776 ns, which is roughly 38 x 46 ns. That is more than 1.5 orders of magnitude slower!

## Why are lenses so slow as getters?

This is almost a death sentence for lenses as getters. But where are these huge differences coming from? If we look through the definitions of `lens` and `get`, we find that there are quite a few function calls going on and that the two typeclasses `Functor` and `HasField` are involved in this as well. Calling more functions is obviously slower. On top of that, typeclasses have a significant runtime overhead in DAML since instances are passed around as dictionary records at runtime and calling a method selects the right field from this dictionary.

If we don't want to abandon the idea of van Laarhoven lenses, we cannot eschew the  `Functor` typeclass. But what about `HasField`? If we want to be able construct lenses in a way that is polymorphic in the field name, there's no way around `HasField`. However, if we were willing to write plenty of boilerplate like the monomorphic `_field1` lens above, we could do away with `HasField`. Benchmarking this monomorphic approach yields:

|`x`|`T(x)`|`(T(x) - T(benchNoop)) / 100,000`|
|-|-|-|
|`benchMono`|92.5 ms|814 ns|

Accessing fields with monomorphic lenses is twice as fast as with their polymorphic counterparts but still 17x slower than using the builtin syntax. This implies that no matter how much better we make the implementation of `lens`, even if we used compile time specialization for it, we wouldn't get better than a 17x slowdown compared to the builtin syntax.

## Temporary stop-gap measures

If a codebase is ubiquitously using lenses as getters, then rewriting it to use the builtin syntax instead will take time. It might make sense to replace some _very_ commonly used lenses with monomorphic implementations. Although, in a codebase defining hundreds of record types, each of them with a few fields, there is most likely no small group of lenses whose monomorphization makes a difference.

Fortunately, there's one significant win we can achive without changing too much code at all. The current implementation of `lens` is pretty far away from the implementation of `_field1`. If we move `lens` closer to `_field1`, we arrive at
```haskell
fastLens: forall x r a. HasField x r a => Lens r a
fastLens f r = fmap (\x -> setField @x x r) (f (getField @x r))
```
Benchmarking this implementation gives us

|`x`|`T(x)`|`(T(x) - T(benchNoop)) / 100,000`|
|-|-|-|
|`benchFastLens`|128.9 ms|1178 ns|

These numbers are still not great, but they are at least a 1.5x speedup compared to the implementation of `lens`.

## Chains of record accesses

So far, our benchmarks were only concerned with accessing one field in one record. A pattern that occurs quite frequently in practice are nested records and chains of record accesses, as in
```
r.field1.field2.field3
```
With the builtin syntax, every record access you attach to the chain is as (in)expensive as the first record access. Benchmarks confirm this linear progression. However, we could _easily_ make every record access after the first one in a chain _significantly_ faster in the DAML interpreter.

There's a similar linear progression when using `get` and `fastLens`. Unfortunately, we have _no chance_ of optimizing chains of record accesses in any way since they are completely intransparent to the compiler and the interpreter.

## Conclusion

I think the numbers say everything there's to say:

|method|time (in ns)|slowdown vs builtin|
|-|-|-|
|builtin|46|1x|
|monomorphic lens|814|17.7x|
|polymorphic lens|1178|25.6x|
|`lens`|1776|38.6x|

In view of these numbers, I would recommend to everybody who cares about performance to use DAML's builtin syntax for accessing record fields!

Only focussing on getters might be modestly controversial since lenses also serve a purpose as setters. I expect the differences in performance between DAML's builtin syntax for updating record fields
```
r with field1 = newValue
```
and using `set` and a lens to update a single field in a single record to be in the same ballpark as for getters. When using the builtin syntax to updat multiple fields in the same record, the DAML interpreter already performs some optimizations to avoid allocating intermediate records. Such optimizations are impossible with lenses.

However, when it comes to updating fields in nested records, DAML's builtin syntax is not particularly helpful:
```
r with field1 = r.field1 with field2 = newValue
```

It gets even worse when you want to update the value of a field depending on its old value using a function `f`. In many lens libraries this function is called `over` and can be used like
```
over (lens @"field1" . lens @"field2") f r
```
Expressing the same with DAML's builtin syntax feels rather clumsy:
```
r with field1 = r.field1 with field2 = f r.field1.field2
```

If we ever want to make lenses significantly less appealing in DAML than they are today, we need to innovate and make the builtin syntax competitive when it comes to nested record updates. Who would still want to use lense if you could simply write
```
r.field1.field2 ~= f
```
in DAML?
