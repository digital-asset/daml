// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.ledger.types

import com.digitalasset.ledger.api.{v1 => api}
import api.value.Value.Sum
import RecordField._
import scalaz.{Optional => _, _}
import Scalaz._
import com.digitalasset.daml.lf.data.SortedMap

sealed trait LedgerValue

object LedgerValue {

  final case class Bool(value: Boolean) extends LedgerValue

  final case class Record(fields: List[RecordField]) extends LedgerValue

  final case class Variant(constructor: String, value: LedgerValue) extends LedgerValue

  final case class ContractId(value: String) extends LedgerValue

  final case class ValueList(elements: List[LedgerValue]) extends LedgerValue

  final case class ValueMap(map: SortedMap[LedgerValue]) extends LedgerValue

  final case class Int64(value: Long) extends LedgerValue

  final case class Decimal(value: String) extends LedgerValue

  final case class Text(value: String) extends LedgerValue

  final case class Timestamp(value: Long) extends LedgerValue

  final case class Party(value: String) extends LedgerValue

  final case class Date(value: Int) extends LedgerValue

  final case class Optional(value: Option[LedgerValue]) extends LedgerValue

  case object Unit extends LedgerValue

  case object Empty extends LedgerValue

  private val variantValueLens = ReqFieldLens.create[api.value.Variant, api.value.Value]('value)

  final implicit class ApiValueOps(val apiValue: api.value.Value) extends AnyVal {
    def convert: String \/ LedgerValue = apiValue.sum.convert
  }

  final implicit class ApiRecordOps(val apiRecord: api.value.Record) extends AnyVal {
    def convert: String \/ Record = convertRecord(apiRecord)
  }

  final implicit class ApiValueSumOps(val apiValueSum: api.value.Value.Sum) extends AnyVal {
    def convert: String \/ LedgerValue = apiValueSum match {
      case Sum.Variant(apiVariant) => convertVariant(apiVariant)
      case Sum.List(apiList) => convertList(apiList)
      case Sum.Record(apiRecord) => convertRecord(apiRecord)
      case Sum.Optional(apiOptional) => convertOptional(apiOptional)
      case Sum.Map(map) => convertMap(map)
      case Sum.Bool(value) => Bool(value).right
      case Sum.ContractId(value) => ContractId(value).right
      case Sum.Int64(value) => Int64(value).right
      case Sum.Decimal(value) => Decimal(value).right
      case Sum.Text(value) => Text(value).right
      case Sum.Timestamp(value) => Timestamp(value).right
      case Sum.Party(value) => Party(value).right
      case Sum.Date(value) => Date(value).right
      case Sum.Unit(_) => Unit.right
      case Sum.Empty => Empty.right
    }
  }

  private def convertList(apiList: api.value.List) = {
    for {
      values <- apiList.elements.toList.traverseU(_.convert)
    } yield ValueList(values)
  }

  private def convertVariant(apiVariant: api.value.Variant) = {
    for {
      apiValue <- variantValueLens(apiVariant)
      value <- apiValue.convert
    } yield Variant(apiVariant.constructor, value)
  }

  private def convertRecord(apiRecord: api.value.Record) = {
    for {
      fields <- apiRecord.fields.toList.traverseU(_.convert)
    } yield Record(fields)
  }

  private def convertOptional(apiOptional: api.value.Optional) = {
    apiOptional.value match {
      case None => Optional(None).right
      case Some(value) =>
        for {
          converted <- value.convert
        } yield Optional(converted.some)
    }
  }

  private def convertMap(apiMap: api.value.Map): String \/ LedgerValue.ValueMap =
    for {
      entries <- apiMap.entries.toList.traverseU {
        case api.value.Map.Entry(_, None) => -\/("value must be defined")
        case api.value.Map.Entry(k, Some(v)) => v.sum.convert.map(k -> _)
      }
      map <- SortedMap.fromSortedList(entries).fold(-\/(_), \/-(_))
    } yield LedgerValue.ValueMap(map)

}
