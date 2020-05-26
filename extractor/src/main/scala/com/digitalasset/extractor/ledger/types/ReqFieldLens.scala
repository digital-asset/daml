// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.ledger.types

import shapeless._
import scalaz.{Lens => _, _}
import Scalaz._

/** Lens to extract actually required values from `Option`al fields
  * as in the types like `com.daml.ledger.api.v1.value.RecordField`,
  * and display meaningful information about the field and the container value
  * when it's not possible because of a `None` value.
  *
  * Similar could be achieved by a function like
  *
  * {{{
  * def getRequiredValue[V, C](
  *   optVal: Option[V],
  *   c: C,
  *   field: String
  * ): String \/ V = {
  *   optVal.\/>(s"Required field `\$field` is empty in value: \$c")
  * }
  * }}}
  *
  * , but that would not provide any safety against doing something like
  *
  * {{{
  *   getRequiredValue(Some(1), "nah", "foo")
  * }}}
  *
  * By using the provided `ReqFieldLens.create` method, we get type safety:
  * only works with existing fields of the given type, which are also an `Option[_]`
  *
  * {{{
  *   // given we have
  *   final case class Person(name: String, age: Option[Int])
  *
  *   // then
  *   ReqFieldLens.create[Person, ???]('nah)     -> won't compile
  *   ReqFieldLens.create[Person, String]('name) -> won't compile
  *   ReqFieldLens.create[Person, Int]('age)     -> will compile \o/
  *
  *   scala> val ageLens = ReqFieldLens.create[Person, Int]('age)
  *   ageLens: com.daml.extractor.ledger.types.ReqFieldLens[Person,Int] = [...]
  *
  *   scala> ageLens(Person("Alice", None))
  *   res1: String \/ Int = -\/(Required field `age` is empty in value: Person(Alice,None))
  *
  *   scala> ageLens(Person("Bob", Some(44)))
  *   res2: String \/ Int = \/-(44)
  * }}}
  *
  */
class ReqFieldLens[C <: Product, V](lens: Lens[C, Option[V]], field: Symbol) {
  def apply(c: C): String \/ V =
    lens.get(c).\/>(s"Required field `${field.name}` is empty in value: ${c}")
}

object ReqFieldLens {
  final class Create[C <: Product, V] {
    def apply[K <: Symbol](
        s: Witness.Aux[K]
    )(
        implicit mkFieldLens: MkFieldLens.Aux[C, K, Option[V]]
    ): ReqFieldLens[C, V] = {
      new ReqFieldLens[C, V](lens[C] >> s, s.value)
    }
  }

  def create[C <: Product, V] = new Create[C, V]
}
