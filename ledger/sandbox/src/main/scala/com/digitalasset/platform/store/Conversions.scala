// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import java.sql.PreparedStatement

import anorm.{
  Column,
  MetaDataItem,
  ParameterMetaData,
  RowParser,
  SqlMappingError,
  SqlParser,
  ToStatement
}
import com.digitalasset.daml.lf.data.Ref.{ContractIdString, LedgerString, PackageId, Party}

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

  implicit def columnToParty(implicit c: Column[String]): Column[Party] =
    stringColumnToX(Party.fromString)(c)

  implicit def partyToStatement(implicit strToStm: ToStatement[String]): ToStatement[Party] =
    subStringToStatement(strToStm)

  def party(columnName: String)(implicit c: Column[String]): RowParser[Party] =
    SqlParser.get[Party](columnName)(columnToParty(c))

  // PackageIds

  implicit def columnToPackageId(implicit c: Column[String]): Column[PackageId] =
    stringColumnToX(PackageId.fromString)(c)

  implicit def packageIdToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[PackageId] =
    subStringToStatement(strToStm)

  def packageId(columnName: String)(implicit c: Column[String]): RowParser[PackageId] =
    SqlParser.get[PackageId](columnName)(columnToPackageId(c))

  // LedgerStrings

  implicit def columnToLedgerString(implicit c: Column[String]): Column[LedgerString] =
    stringColumnToX(LedgerString.fromString)(c)

  implicit def ledgerStringToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[LedgerString] =
    subStringToStatement(strToStm)

  implicit def columnToContractId(implicit c: Column[String]): Column[ContractIdString] =
    stringColumnToX(ContractIdString.fromString)(c)

  implicit def contractIdToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[ContractIdString] =
    subStringToStatement(strToStm)

  def emptyStringToNullColumn(implicit c: Column[String]): Column[String] = new Column[String] {
    override def apply(value: Any, meta: MetaDataItem) = value match {
      case "" => c(null, meta)
      case x => c(x, meta)
    }
  }

  def ledgerString(columnName: String)(implicit c: Column[String]): RowParser[LedgerString] =
    SqlParser.get[LedgerString](columnName)(columnToLedgerString(c))

  implicit def ledgerStringMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String]): ParameterMetaData[LedgerString] =
    subStringMetaParameter(strParamMetaData)

  def contractIdString(columnName: String)(
      implicit c: Column[String]): RowParser[ContractIdString] =
    SqlParser.get[ContractIdString](columnName)(columnToContractId(c))

  implicit def contractIdStringMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String]): ParameterMetaData[ContractIdString] =
    subStringMetaParameter(strParamMetaData)

}
