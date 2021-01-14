// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package object scalautil {
  val NonEmpty: NonEmptyColl.type = NonEmptyColl.Instance

  /** A non-empty `A`.  Implicitly converts to `A` in relevant contexts.
    *
    * Using this library sensibly with Scala 2.12 requires `-Xsource:2.13` and
    * `-Ypartial-unification`.
    */
  type NonEmpty[+A] = NonEmpty.NonEmpty[A]

  /** A subtype of `NonEmpty[F[A]]` where `A` is in position to be inferred
    * properly.  When attempting to fit a type to the type params `C[T]`, scalac
    * will infer the following:
    *
    * type shape      | C              | T
    * ----------------+----------------+-----
    * NonEmpty[F[A]]  | NonEmpty       | F[A]
    * NonEmptyF[F, A] | NonEmpty[F, *] | A
    *
    * If you want to `traverse` or `foldLeft` or `map` on A, the latter is far
    * more convenient.  So any value whose type is the former can be converted
    * to the latter via the `toF` method.
    *
    * In fact, given any `NonEmpty[Foo]` where `Foo` can be matched to type
    * parameters `C[T]`, `toF` will infer a `NonEmptyF[C, T]` for whatever
    * values of C and T scalac chooses.  For example, if `bar: NonEmpty[Map[K,
    * V]]`, then `bar.toF: NonEmptyF[Map[K, *], V]`, because that's how scalac
    * destructures that type.
    */
  type NonEmptyF[F[_], A] = NonEmpty.NonEmptyF[F, A]
}
