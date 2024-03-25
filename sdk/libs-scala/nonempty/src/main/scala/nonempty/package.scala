// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

package object nonempty {

  /** A non-empty `A`.  Implicitly converts to `A` in relevant contexts.
    *
    * Why use this instead of [[scalaz.OneAnd]]?
    *
    * `OneAnd` is ''constructively'' non-empty; there is no way to "cheat".
    * However, its focus on functorial use cases means that it is well-suited
    * for uninterpreted sequences like List and Vector, but less so those where
    * parts of the elements have some semantics, such as with Map and Set.  For
    * cases like Set you also have to do some extra work to make sure the `head`
    * doesn't break the invariant of the underlying structure.
    *
    * By contrast, `NonEmpty` is ''nominally'' non-empty.  We take care to
    * define only those primitives that will preserve non-emptiness, but it is
    * possible to make a mistake (for example, if you added flatMap but wrote
    * the wrong signature).  The benefits are that you get to treat them more
    * like the underlying structure, because there is no structural difference,
    * and operations on existing code will more transparently continue to work
    * (and preserve the non-empty property where reasonable) than the equivalent
    * port to use `OneAnd`.
    *
    * Using this library sensibly with Scala 2.12 requires `-Xsource:2.13` and
    * `-Ypartial-unification`.
    */
  type NonEmpty[+A] = NonEmptyColl.Instance.NonEmpty[A]

  /** A subtype of `NonEmpty[F[A]]` where `A` is in position to be inferred
    * properly.  When attempting to fit a type to the type params `C[T]`, scalac
    * will infer the following:
    *
    * | type shape      | C              | T    |
    * | --------------- | -------------- | ---- |
    * | NonEmpty[F[A]]  | NonEmpty       | F[A] |
    * | NonEmptyF[F, A] | NonEmpty[F, *] | A    |
    *
    * If you want to `traverse` or `foldLeft` or `map` on A, the latter is far
    * more convenient.  So any value whose type is the former can be converted
    * to the latter via the `toNEF` method.
    *
    * In fact, given any `NonEmpty[Foo]` where `Foo` can be matched to type
    * parameters `C[T]`, `toNEF` will infer a `NonEmptyF[C, T]` for whatever
    * values of C and T scalac chooses.  For example, if `bar: NonEmpty[Map[K,
    * V]]`, then `bar.toNEF: NonEmptyF[Map[K, *], V]`, because that's how scalac
    * destructures that type.
    */
  type NonEmptyF[F[_], A] = NonEmptyColl.Instance.NonEmptyF[F, A]

  val ±: : +-:.type = +-:
  val :∓ : :-+.type = :-+
}
