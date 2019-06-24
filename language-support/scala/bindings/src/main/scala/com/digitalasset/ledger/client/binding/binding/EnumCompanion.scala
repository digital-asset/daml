// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import com.digitalasset.ledger.api.v1.{value => rpcvalue}
import com.digitalasset.ledger.client.binding.encoding.{LfEncodable, LfTypeEncoding}
import scalaz.Liskov.<~<

abstract class EnumCompanion[T](implicit isEnum: T <~< EnumRef) extends ValueRefCompanion {

  val firstValue: T
  val otherValues: Vector[T]

  final def values: Vector[T] = firstValue +: otherValues

  implicit final lazy val `the enum Value`: Value[T] = new `Value ValueRef`[T] {
    private[this] val readers = values.map(e => (isEnum(e).constructor: String) -> e).toMap

    override def read(argValue: rpcvalue.Value.Sum): Option[T] =
      argValue.enum flatMap (e => readers.get(e.constructor))

    private[this] val rpcValues = values.map(e => ` enum`(isEnum(e).constructor))

    override def write(enum: T): rpcvalue.Value.Sum =
      rpcValues(isEnum(enum).index)
  }

  implicit final val `the enum LfEncodable`: LfEncodable[T] = new LfEncodable[T] {
    override def encoding(lte: LfTypeEncoding): lte.Out[T] =
      lte.enumAll(
        ` dataTypeId`,
        lte.enumCase(isEnum(firstValue).constructor)(firstValue),
        otherValues.map(v => lte.enumCase(isEnum(v).constructor)(v)): _*
      )
  }

}
