// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.api.refinements.ApiTypes.ContractId
import com.digitalasset.ledger.client.binding.{Template, Primitive => P}
import scalaz.~>
import spray.{json => sj}
import spray.json.JsonFormat

object JsonTypeCodecs extends ValuePrimitiveEncoding[JsonLfTypeEncoding.Out] {
  import JsonLfTypeEncoding.{Out, OutUnit}

  private val jsonProtocol: sj.BasicFormats with sj.CollectionFormats with sj.StandardFormats =
    CustomJsonFormats

  override val valueInt64: Out[P.Int64] = Out(jsonProtocol.LongJsonFormat)
  override val valueDecimal: Out[P.Decimal] = Out(jsonProtocol.BigDecimalJsonFormat)
  override val valueText: Out[P.Text] = Out(jsonProtocol.StringJsonFormat)
  override val valueUnit: Out[P.Unit] = OutUnit()
  override val valueBool: Out[P.Bool] = Out(jsonProtocol.BooleanJsonFormat)

  override val valueDate: Out[P.Date] = Out(CustomJsonFormats.dateJsonFormat)
  override val valueTimestamp: Out[P.Timestamp] = Out(CustomJsonFormats.timestampJsonFormat)

  override val valueParty: Out[P.Party] =
    Out(P.Party.subst(jsonProtocol.StringJsonFormat))

  override def valueList[A: Out]: Out[P.List[A]] = {
    implicit val fmt: JsonFormat[A] = implicitly[Out[A]].format
    Out(jsonProtocol.immSeqFormat)
  }

  override def valueContractId[Tpl <: Template[Tpl]]: Out[P.ContractId[Tpl]] =
    Out(P.substContractId(ContractId.subst(jsonProtocol.StringJsonFormat)))

  override def valueOptional[A: Out]: Out[P.Optional[A]] =
    Out(jsonProtocol.optionFormat(implicitly[Out[A]].format))

  override def valueMap[A: Out]: Out[P.Map[A]] = {
    implicit val fmt: JsonFormat[A] = implicitly[Out[A]].format
    Out(jsonProtocol.mapFormat(jsonProtocol.StringJsonFormat, implicitly[Out[A]].format))
  }

  val asJsonFormat: ValuePrimitiveEncoding[JsonFormat] = {
    val prim: JsonLfTypeEncoding.Out ~> JsonFormat = new (JsonLfTypeEncoding.Out ~> JsonFormat) {
      override def apply[A](fa: JsonLfTypeEncoding.Out[A]): JsonFormat[A] = fa.format
    }
    object list extends (JsonFormat ~> Lambda[a => JsonFormat[P.List[a]]]) {
      override def apply[A](fa: JsonFormat[A]): JsonFormat[P.List[A]] = {
        implicit val fmt: JsonLfTypeEncoding.Out[A] = JsonLfTypeEncoding.OutJson(fa)
        JsonTypeCodecs.valueList.format
      }
    }
    object option extends (JsonFormat ~> Lambda[a => JsonFormat[P.Optional[a]]]) {
      override def apply[A](fa: JsonFormat[A]): JsonFormat[P.Optional[A]] = {
        implicit val fmt: JsonLfTypeEncoding.Out[A] = JsonLfTypeEncoding.OutJson(fa)
        JsonTypeCodecs.valueOptional.format
      }
    }
    object map extends (JsonFormat ~> Lambda[a => JsonFormat[P.Map[a]]]) {
      override def apply[A](fa: JsonFormat[A]): JsonFormat[P.Map[A]] = {
        implicit val fmt: JsonLfTypeEncoding.Out[A] = JsonLfTypeEncoding.OutJson(fa)
        JsonTypeCodecs.valueMap.format
      }
    }
    ValuePrimitiveEncoding.mapped[JsonLfTypeEncoding.Out, JsonFormat](JsonTypeCodecs)(prim)(list)(
      option)(map)
  }
}
