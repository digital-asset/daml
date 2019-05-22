// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import java.sql.PreparedStatement

import anorm.{Column, ParameterMetaData, RowParser, SqlMappingError, SqlParser, ToStatement}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}

object Conversions {

  private def stringColumnToX[X](f: String => Either[String, X])(c: Column[String]): Column[X] =
    Column.nonNull((value: Any, meta) =>
      c(value, meta).toEither.flatMap(x => f(x).left.map(SqlMappingError)))

  private def subStringToStatement[T <: String](c: ToStatement[String]): ToStatement[T] =
    (s: PreparedStatement, index: Int, v: T) => c.set(s, index, v)

  private def subStringMetaParameter[T <: String](
      strParamMetaData: ParameterMetaData[String]): ParameterMetaData[T] =
    new ParameterMetaData[T] {
      def sqlType: String = strParamMetaData.sqlType
      def jdbcType: Int = strParamMetaData.jdbcType
    }

  // Parties

  implicit def columnToParty(implicit c: Column[String]): Column[Ref.Party] =
    stringColumnToX(Ref.Party.fromString)(c)

  implicit def partyToStatement(implicit strToStm: ToStatement[String]): ToStatement[Party] =
    subStringToStatement(strToStm)

  def party(columnName: String)(implicit c: Column[String]): RowParser[Ref.Party] =
    SqlParser.get[Ref.Party](columnName)(columnToParty(c))

  // PackageIds

  implicit def columnToPackageId(implicit c: Column[String]): Column[Ref.PackageId] =
    stringColumnToX(Ref.PackageId.fromString)(c)

  implicit def packageIdToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[PackageId] =
    subStringToStatement(strToStm)

  def packageId(columnName: String)(implicit c: Column[String]): RowParser[Ref.PackageId] =
    SqlParser.get[Ref.PackageId](columnName)(columnToPackageId(c))

  // LedgerStrings

  implicit def columnToLedgerString(implicit c: Column[String]): Column[Ref.LedgerString] =
    stringColumnToX(Ref.LedgerString.fromString)(c)

  implicit def ledgerStringToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[LedgerString] =
    subStringToStatement(strToStm)

  def ledgerString(columnName: String)(implicit c: Column[String]): RowParser[Ref.LedgerString] =
    SqlParser.get[Ref.LedgerString](columnName)(columnToLedgerString(c))

  implicit def ledgerStringMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String]): ParameterMetaData[LedgerString] =
    subStringMetaParameter(strParamMetaData)

}
