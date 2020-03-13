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
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref

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

  implicit def partyToStatement(implicit strToStm: ToStatement[String]): ToStatement[Ref.Party] =
    subStringToStatement(strToStm)

  def party(columnName: String)(implicit c: Column[String]): RowParser[Ref.Party] =
    SqlParser.get[Ref.Party](columnName)(columnToParty(c))

  // PackageIds

  implicit def columnToPackageId(implicit c: Column[String]): Column[Ref.PackageId] =
    stringColumnToX(Ref.PackageId.fromString)(c)

  implicit def packageIdToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[Ref.PackageId] =
    subStringToStatement(strToStm)

  def packageId(columnName: String)(implicit c: Column[String]): RowParser[Ref.PackageId] =
    SqlParser.get[Ref.PackageId](columnName)(columnToPackageId(c))

  // LedgerStrings

  implicit def columnToLedgerString(implicit c: Column[String]): Column[Ref.LedgerString] =
    stringColumnToX(Ref.LedgerString.fromString)(c)

  implicit def ledgerStringToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[Ref.LedgerString] =
    subStringToStatement(strToStm)

  implicit def columnToParticipantId(implicit c: Column[String]): Column[Ref.ParticipantId] =
    stringColumnToX(Ref.ParticipantId.fromString)(c)

  implicit def participantToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[Ref.ParticipantId] =
    subStringToStatement(strToStm)

  implicit def columnToContractId(implicit c: Column[String]): Column[Ref.ContractIdString] =
    stringColumnToX(Ref.ContractIdString.fromString)(c)

  implicit def contractIdToStatement(
      implicit strToStm: ToStatement[String]): ToStatement[Ref.ContractIdString] =
    subStringToStatement(strToStm)

  def emptyStringToNullColumn(implicit c: Column[String]): Column[String] = new Column[String] {
    override def apply(value: Any, meta: MetaDataItem) = value match {
      case "" => c(null, meta)
      case x => c(x, meta)
    }
  }

  def ledgerString(columnName: String)(implicit c: Column[String]): RowParser[Ref.LedgerString] =
    SqlParser.get[Ref.LedgerString](columnName)(columnToLedgerString(c))

  implicit def ledgerStringMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String]): ParameterMetaData[Ref.LedgerString] =
    subStringMetaParameter(strParamMetaData)

  def participantId(columnName: String)(implicit c: Column[String]): RowParser[Ref.ParticipantId] =
    SqlParser.get[Ref.ParticipantId](columnName)(columnToParticipantId(c))

  implicit def participantIdMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String]): ParameterMetaData[Ref.ParticipantId] =
    subStringMetaParameter(strParamMetaData)

  def contractIdString(columnName: String)(
      implicit c: Column[String]): RowParser[Ref.ContractIdString] =
    SqlParser.get[Ref.ContractIdString](columnName)(columnToContractId(c))

  implicit def contractIdStringMetaParameter(implicit strParamMetaData: ParameterMetaData[String])
    : ParameterMetaData[Ref.ContractIdString] =
    subStringMetaParameter(strParamMetaData)

  implicit def offsetToStatement: ToStatement[Offset] = new ToStatement[Offset] {
    override def set(s: PreparedStatement, index: Int, v: Offset): Unit =
      s.setBytes(index, v.toByteArray)
  }
  def offset(name: String): RowParser[Offset] =
    SqlParser.get[Array[Byte]](name).map(Offset.fromBytes)

  implicit def columnToOffset(implicit c: Column[Array[Byte]]): Column[Offset] =
    Column.nonNull((value: Any, meta) => c(value, meta).toEither.map(Offset.fromBytes))
}
