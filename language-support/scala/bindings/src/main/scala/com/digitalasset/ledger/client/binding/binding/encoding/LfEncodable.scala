// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client
package binding
package encoding

import binding.{Primitive => P}

@annotation.implicitNotFound(msg = "Cannot find LfEncodable type class for ${A}")
abstract class LfEncodable[A] {
  def encoding(lte: LfTypeEncoding): lte.Out[A]
}

object LfEncodable extends ValuePrimitiveEncoding[LfEncodable] {
  @inline def encoding[A](lte: LfTypeEncoding)(implicit le: LfEncodable[A]): lte.Out[A] =
    le.encoding(lte)

  override implicit val valueInt64: LfEncodable[P.Int64] = new LfEncodable[P.Int64] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Int64] = lte.primitive.valueInt64
  }

  override implicit val valueNumeric: LfEncodable[P.Numeric] = new LfEncodable[P.Numeric] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Numeric] = lte.primitive.valueNumeric
  }

  override implicit val valueParty: LfEncodable[P.Party] = new LfEncodable[P.Party] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Party] = lte.primitive.valueParty
  }

  override implicit val valueText: LfEncodable[P.Text] = new LfEncodable[P.Text] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Text] = lte.primitive.valueText
  }

  override implicit val valueDate: LfEncodable[P.Date] = new LfEncodable[P.Date] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Date] = lte.primitive.valueDate
  }

  override implicit val valueTimestamp: LfEncodable[P.Timestamp] = new LfEncodable[P.Timestamp] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Timestamp] = lte.primitive.valueTimestamp
  }

  override implicit val valueUnit: LfEncodable[P.Unit] = new LfEncodable[P.Unit] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Unit] = lte.primitive.valueUnit
  }

  override implicit val valueBool: LfEncodable[P.Bool] = new LfEncodable[P.Bool] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Bool] = lte.primitive.valueBool
  }

  override implicit def valueList[A: LfEncodable]: LfEncodable[P.List[A]] =
    new LfEncodable[P.List[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.List[A]] =
        lte.primitive.valueList(LfEncodable.encoding[A](lte))
    }

  override implicit def valueContractId[A]: LfEncodable[P.ContractId[A]] =
    new LfEncodable[P.ContractId[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.ContractId[A]] =
        lte.primitive.valueContractId
    }

  override implicit def valueOptional[A: LfEncodable]: LfEncodable[P.Optional[A]] =
    new LfEncodable[P.Optional[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.Optional[A]] =
        lte.primitive.valueOptional(LfEncodable.encoding[A](lte))
    }

  override implicit def valueTextMap[A: LfEncodable]: LfEncodable[P.TextMap[A]] =
    new LfEncodable[P.TextMap[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.TextMap[A]] =
        lte.primitive.valueTextMap(LfEncodable.encoding[A](lte))
    }

  override implicit def valueGenMap[K: LfEncodable, V: LfEncodable]: LfEncodable[P.GenMap[K, V]] =
    new LfEncodable[P.GenMap[K, V]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.GenMap[K, V]] =
        lte.primitive.valueGenMap(LfEncodable.encoding[K](lte), LfEncodable.encoding[V](lte))
    }

  trait ViaFields[T] {

    /** The fields of `T`'s associated record type, each in some
      * type constructor `C`.
      *
      * @note Lowercase because `T.View` is a valid Daml type name,
      *       and this trait describes codegen output.
      */
    type view[C[_]] <: RecordView[C, view]

    def fieldEncoding(lte: LfTypeEncoding): view[lte.Field]

    def encoding(lte: LfTypeEncoding)(view: view[lte.Field]): lte.Out[T]
  }
}
