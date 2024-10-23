// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package ledger.api

import com.digitalasset.daml.lf.value.{ValueOuterClass => ProtobufConverter}
import com.google.protobuf.ByteString
import scala.collection.immutable.{Map, Set}

trait LfValueConverterFrom[T] {
  // May throw
  def fromLfValue(value: LfValue): T
}

object LfValueConverterFrom {
  implicit val unitConverterFrom: LfValueConverterFrom[Unit] = {
    case LfValueUnit =>
      ()

    case value =>
      sys.error(s"invalid unit value: $value")
  }

  implicit val boolConverterFrom: LfValueConverterFrom[Boolean] = {
    case LfValueBool(bool) =>
      bool

    case value =>
      sys.error(s"invalid boolean value: $value")
  }

  implicit val partyConverterFrom: LfValueConverterFrom[String] = {
    case LfValueParty(party) =>
      party

    case value =>
      sys.error(s"invalid party value: $value")
  }

  implicit val intConverterFrom: LfValueConverterFrom[Int] = {
    case LfValueInt(int) =>
      int

    case value =>
      sys.error(s"invalid int value: $value")
  }

  implicit val stringConverterFrom: LfValueConverterFrom[String] = {
    case LfValueString(string) =>
      string

    case value =>
      sys.error(s"invalid string value: $value")
  }

  implicit def contractIdConverterFrom[T <: Template, CT <: TemplateId[T]](implicit
      companion: CT
  ): LfValueConverterFrom[Contract[T, CT]] = {
    case coid: LfValueContractId =>
      new Contract[T, CT](coid)

    case value =>
      sys.error(s"invalid contract id value: $value")
  }

  implicit val setConverterFrom: LfValueConverterFrom[Set[LfValue]] = {
    case LfValueSet(set) =>
      set

    case value =>
      sys.error(s"invalid set value: $value")
  }

  implicit val mapConverterFrom: LfValueConverterFrom[Map[LfValue, LfValue]] = {
    case LfValueMap(map) =>
      map

    case value =>
      sys.error(s"invalid map value: $value")
  }

  implicit val optionalConverterFrom: LfValueConverterFrom[Option[LfValue]] = {
    case LfValueOptional(opt) =>
      opt

    case value =>
      sys.error(s"invalid optional value: $value")
  }

  implicit val recordConverterFrom: LfValueConverterFrom[Map[String, LfValue]] = {
    case LfValueRecord(record) =>
      record

    case value =>
      sys.error(s"invalid record value: $value")
  }

  implicit def valueConverterFrom: LfValueConverterFrom[ProtobufConverter.Value] = { value =>
    value.toProtobuf
  }
}
