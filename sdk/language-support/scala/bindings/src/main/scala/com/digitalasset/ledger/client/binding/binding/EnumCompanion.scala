// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import com.daml.ledger.api.v1.{value => rpcvalue}
import com.daml.ledger.client.binding.encoding.{LfEncodable, LfTypeEncoding}
import scalaz.Liskov.<~<
import scalaz.OneAnd
import scalaz.syntax.functor._
import scalaz.std.vector._

abstract class EnumCompanion[T](implicit isEnum: T <~< EnumRef) extends ValueRefCompanion {

  val firstValue: T
  val otherValues: Vector[T]

  final def values: Vector[T] = firstValue +: otherValues

  implicit final lazy val `the enum Value`: Value[T] = new `Value ValueRef`[T] {
    private[this] val readers = values.map(e => (isEnum(e).constructor: String) -> e).toMap

    override def read(argValue: rpcvalue.Value.Sum): Option[T] =
      argValue.enum flatMap (e => readers.get(e.constructor))

    private[this] val rpcValues = values.map(e => ` enum`(isEnum(e).constructor))

    override def write(enumeration: T): rpcvalue.Value.Sum =
      rpcValues(isEnum(enumeration).index)
  }

  implicit final lazy val `the enum LfEncodable`: LfEncodable[T] = new LfEncodable[T] {

    private[this] val cases = OneAnd(firstValue, otherValues).map(x => isEnum(x).constructor -> x)

    override def encoding(lte: LfTypeEncoding): lte.Out[T] =
      lte.enumAll(` dataTypeId`, isEnum(_).index, cases)

  }

}
