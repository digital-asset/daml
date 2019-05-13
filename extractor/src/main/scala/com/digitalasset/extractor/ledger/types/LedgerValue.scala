// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.ledger.types

import com.digitalasset.ledger.api.{v1 => api}
import api.value.Value.Sum
import RecordField._

import scalaz.{Optional => _, _}
import Scalaz._
import com.digitalasset.daml.lf.{data => lfdata}
import lfdata.{FrontStack, ImmArray, Ref, SortedLookupList}
import com.digitalasset.daml.lf.value.{Value => V}

object LedgerValue {

  import scala.language.higherKinds
  type OfCid[F[+ _]] = F[String]

  type Bool = V.ValueBool
  val Bool = V.ValueBool

  type Record = OfCid[V.ValueRecord]
  val Record = V.ValueRecord

  type Variant = OfCid[V.ValueVariant]
  val Variant = V.ValueVariant

  type ContractId = OfCid[V.ValueContractId]
  val ContractId = V.ValueContractId

  type ValueList = OfCid[V.ValueList]
  val ValueList = V.ValueList

  type ValueMap = OfCid[V.ValueMap]
  val ValueMap = V.ValueMap

  type Int64 = V.ValueInt64
  val Int64 = V.ValueInt64

  type Decimal = V.ValueDecimal
  val Decimal = V.ValueDecimal

  type Text = V.ValueText
  val Text = V.ValueText

  type Timestamp = V.ValueTimestamp
  val Timestamp = V.ValueTimestamp

  type Party = V.ValueParty
  val Party = V.ValueParty

  type Date = V.ValueDate
  val Date = V.ValueDate

  type Optional = OfCid[V.ValueOptional]
  val Optional = V.ValueOptional

  val Unit: V.ValueUnit.type = V.ValueUnit

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
      case Sum.Decimal(value) => lfdata.Decimal.fromString(value).disjunction map Decimal
      case Sum.Text(value) => Text(value).right
      case Sum.Timestamp(value) => lfdata.Time.Timestamp.fromLong(value).disjunction map Timestamp
      case Sum.Party(value) => Ref.Party.fromString(value).disjunction map Party
      case Sum.Date(value) => lfdata.Time.Date.fromDaysSinceEpoch(value).disjunction map Date
      case Sum.Unit(_) => Unit.right
      case Sum.Empty => -\/("uninitialized Value")
    }
  }

  private def convertList(apiList: api.value.List) = {
    for {
      values <- apiList.elements.toList.traverseU(_.convert)
    } yield ValueList(FrontStack(values))
  }

  private def convertVariant(apiVariant: api.value.Variant) = {
    for {
      tycon <- apiVariant.variantId traverseU convertIdentifier map (_.flatten)
      ctor <- Ref.Name.fromString(apiVariant.constructor).disjunction
      apiValue <- variantValueLens(apiVariant)
      value <- apiValue.convert
    } yield Variant(tycon, ctor, value)
  }

  private def convertRecord(apiRecord: api.value.Record) = {
    for {
      tycon <- apiRecord.recordId traverseU convertIdentifier map (_.flatten)
      // TODO SC: local RecordField may be elided, in which case convert
      // should just produce the tuple made here
      fields <- ImmArray(apiRecord.fields).traverseU(_.convert flatMap {
        case RecordField(lbl, vl) =>
          some(lbl) filter (_.nonEmpty) traverseU (lbl => Ref.Name.fromString(lbl).disjunction) map (
            (
              _,
              vl))
      })
    } yield Record(tycon, fields)
  }

  private def convertOptional(apiOptional: api.value.Optional) =
    apiOptional.value traverseU (_.convert) map (Optional(_))

  private def convertMap(apiMap: api.value.Map): String \/ LedgerValue.ValueMap =
    for {
      entries <- apiMap.entries.toList.traverseU {
        case api.value.Map.Entry(_, None) => -\/("value must be defined")
        case api.value.Map.Entry(k, Some(v)) => v.sum.convert.map(k -> _)
      }
      map <- SortedLookupList.fromImmArray(ImmArray(entries)).disjunction
    } yield LedgerValue.ValueMap(map)

  private def convertIdentifier(
      apiIdentifier: api.value.Identifier): String \/ Option[Ref.Identifier] = {
    val api.value.Identifier(packageId, _name @ _, moduleName, entityName) = apiIdentifier
    some(packageId)
      .filter(_.nonEmpty)
      .traverseU { _ =>
        for {
          pkgId <- Ref.PackageId.fromString(packageId)
          mod <- Ref.ModuleName fromString moduleName
          ent <- Ref.DottedName fromString entityName
        } yield Ref.Identifier(pkgId, Ref.QualifiedName(mod, ent))
      }
      .disjunction
  }
}
