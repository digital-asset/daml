// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client
package binding
package encoding

import scala.language.higherKinds

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

  override implicit val valueDecimal: LfEncodable[P.Decimal] = new LfEncodable[P.Decimal] {
    override def encoding(lte: LfTypeEncoding): lte.Out[P.Decimal] = lte.primitive.valueDecimal
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

  override implicit def valueContractId[A: LfEncodable]: LfEncodable[P.ContractId[A]] =
    new LfEncodable[P.ContractId[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.ContractId[A]] =
        lte.primitive.valueContractId(LfEncodable.encoding[A](lte))
    }

  override implicit def valueOptional[A: LfEncodable]: LfEncodable[P.Optional[A]] =
    new LfEncodable[P.Optional[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.Optional[A]] =
        lte.primitive.valueOptional(LfEncodable.encoding[A](lte))
    }

  override implicit def valueMap[A: LfEncodable]: LfEncodable[P.Map[A]] =
    new LfEncodable[P.Map[A]] {
      override def encoding(lte: LfTypeEncoding): lte.Out[P.Map[A]] =
        lte.primitive.valueMap(LfEncodable.encoding[A](lte))
    }

  trait ViaFields[T] {

    /** The fields of `T`'s associated record type, each in some
      * type constructor `C`.
      *
      * @note Lowercase because `T.View` is a valid DAML type name,
      *       and this trait describes codegen output.
      */
    type view[C[_]] <: RecordView[C, view]

    def fieldEncoding(lte: LfTypeEncoding): view[lte.Field]

    def encoding(lte: LfTypeEncoding)(view: view[lte.Field]): lte.Out[T]
  }
}
