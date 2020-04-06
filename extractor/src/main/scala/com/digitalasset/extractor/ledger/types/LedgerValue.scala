// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.ledger.types

import com.daml.ledger.api.{v1 => api}
import api.value.Value.Sum
import RecordField._

import scalaz.{Optional => _, _}
import Scalaz._
import com.daml.lf.{data => lfdata}
import lfdata.{FrontStack, ImmArray, Ref, SortedLookupList}
import com.daml.lf.value.{Value => V}

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
      case Sum.Enum(apiEnum) => convertEnum(apiEnum)
      case Sum.List(apiList) => convertList(apiList)
      case Sum.Record(apiRecord) => convertRecord(apiRecord)
      case Sum.Optional(apiOptional) => convertOptional(apiOptional)
      case Sum.Map(map) => convertTextMap(map)
      case Sum.GenMap(entries) => convertGenMap(entries)
      case Sum.Bool(value) => V.ValueBool(value).right
      case Sum.ContractId(value) => V.ValueContractId(value).right
      case Sum.Int64(value) => V.ValueInt64(value).right
      case Sum.Numeric(value) =>
        lfdata.Numeric
          .fromUnscaledBigDecimal(new java.math.BigDecimal(value))
          .disjunction map V.ValueNumeric
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

  private def convertEnum(apiEnum: api.value.Enum) =
    for {
      tyCon <- apiEnum.enumId traverseU convertIdentifier map (_.flatten)
      ctor <- Ref.Name.fromString(apiEnum.constructor).disjunction
    } yield V.ValueEnum(tyCon, ctor)

  private def convertRecord(apiRecord: api.value.Record) = {
    for {
      tycon <- apiRecord.recordId traverseU convertIdentifier map (_.flatten)
      fields <- ImmArray(apiRecord.fields).traverseU(_.convert)
    } yield V.ValueRecord(tycon, fields)
  }

  private def convertOptional(apiOptional: api.value.Optional) =
    apiOptional.value traverseU (_.convert) map (V.ValueOptional(_))

  private def convertTextMap(apiMap: api.value.Map): String \/ OfCid[V.ValueTextMap] =
    for {
      entries <- apiMap.entries.toList.traverseU {
        case api.value.Map.Entry(k, Some(v)) => v.sum.convert.map(k -> _)
        case api.value.Map.Entry(_, None) => -\/("value field of Map.Entry must be defined")
      }
      map <- SortedLookupList.fromImmArray(ImmArray(entries)).disjunction
    } yield V.ValueTextMap(map)

  private def convertGenMap(apiMap: api.value.GenMap): String \/ OfCid[V.ValueGenMap] =
    apiMap.entries.toList
      .traverseU { entry =>
        for {
          k <- entry.key.fold[String \/ OfCid[V]](-\/("key field of GenMap.Entry must be defined"))(
            _.convert)
          v <- entry.value.fold[String \/ OfCid[V]](
            -\/("value field of GenMap.Entry must be defined"))(_.convert)
        } yield k -> v
      }
      .map(entries => V.ValueGenMap(ImmArray(entries)))

  private def convertIdentifier(
      apiIdentifier: api.value.Identifier): String \/ Option[Ref.Identifier] = {
    val api.value.Identifier(packageId, moduleName, entityName) = apiIdentifier
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
