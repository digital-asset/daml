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

  private val variantValueLens = ReqFieldLens.create[api.value.Variant, api.value.Value]('value)

  final implicit class ApiValueOps(val apiValue: api.value.Value) extends AnyVal {
    def convert: String \/ LedgerValue = apiValue.sum.convert
  }

  final implicit class ApiRecordOps(val apiRecord: api.value.Record) extends AnyVal {
    def convert: String \/ OfCid[V.ValueRecord] = convertRecord(apiRecord)
  }

  final implicit class ApiValueSumOps(val apiValueSum: api.value.Value.Sum) extends AnyVal {
    def convert: String \/ LedgerValue = apiValueSum match {
      case Sum.Variant(apiVariant) => convertVariant(apiVariant)
      case Sum.List(apiList) => convertList(apiList)
      case Sum.Record(apiRecord) => convertRecord(apiRecord)
      case Sum.Optional(apiOptional) => convertOptional(apiOptional)
      case Sum.Map(map) => convertMap(map)
      case Sum.Bool(value) => V.ValueBool(value).right
      case Sum.ContractId(value) => V.ValueContractId(value).right
      case Sum.Int64(value) => V.ValueInt64(value).right
      case Sum.Decimal(value) => lfdata.Decimal.fromString(value).disjunction map V.ValueDecimal
      case Sum.Text(value) => V.ValueText(value).right
      case Sum.Timestamp(value) =>
        lfdata.Time.Timestamp.fromLong(value).disjunction map V.ValueTimestamp
      case Sum.Party(value) => Ref.Party.fromString(value).disjunction map V.ValueParty
      case Sum.Date(value) => lfdata.Time.Date.fromDaysSinceEpoch(value).disjunction map V.ValueDate
      case Sum.Unit(_) => V.ValueUnit.right
      case Sum.Empty => -\/("uninitialized Value")
    }
  }

  private def convertList(apiList: api.value.List) = {
    for {
      values <- apiList.elements.toList.traverseU(_.convert)
    } yield V.ValueList(FrontStack(values))
  }

  private def convertVariant(apiVariant: api.value.Variant) = {
    for {
      tycon <- apiVariant.variantId traverseU convertIdentifier map (_.flatten)
      ctor <- Ref.Name.fromString(apiVariant.constructor).disjunction
      apiValue <- variantValueLens(apiVariant)
      value <- apiValue.convert
    } yield V.ValueVariant(tycon, ctor, value)
  }

  private def convertRecord(apiRecord: api.value.Record) = {
    for {
      tycon <- apiRecord.recordId traverseU convertIdentifier map (_.flatten)
      fields <- ImmArray(apiRecord.fields).traverseU(_.convert)
    } yield V.ValueRecord(tycon, fields)
  }

  private def convertOptional(apiOptional: api.value.Optional) =
    apiOptional.value traverseU (_.convert) map (V.ValueOptional(_))

  private def convertMap(apiMap: api.value.Map): String \/ OfCid[V.ValueMap] =
    for {
      entries <- apiMap.entries.toList.traverseU {
        case api.value.Map.Entry(_, None) => -\/("value must be defined")
        case api.value.Map.Entry(k, Some(v)) => v.sum.convert.map(k -> _)
      }
      map <- SortedLookupList.fromImmArray(ImmArray(entries)).disjunction
    } yield V.ValueMap(map)

  private def convertIdentifier(
      apiIdentifier: api.value.Identifier): String \/ Option[Ref.Identifier] = {
    val api.value.Identifier(packageId, _, moduleName, entityName) = apiIdentifier
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
