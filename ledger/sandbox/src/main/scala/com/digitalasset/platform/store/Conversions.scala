// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.io.InputStream
import java.sql.PreparedStatement
import java.time.Instant
import java.util.Date

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
import com.daml.lf.data.Ref
import com.daml.lf.value.Value

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

  implicit def partyMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String],
  ): ParameterMetaData[Ref.Party] =
    subStringMetaParameter(strParamMetaData)

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

  implicit def columnToContractId(implicit c: Column[String]): Column[Value.AbsoluteContractId] =
    stringColumnToX(Value.AbsoluteContractId.fromString)(c)

  implicit def contractIdToStatement(
      implicit strToStm: ToStatement[String]
  ): ToStatement[Value.AbsoluteContractId] =
    (s: PreparedStatement, index: Int, v: Value.AbsoluteContractId) =>
      strToStm.set(s, index, v.coid)

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

  def contractId(columnName: String)(
      implicit c: Column[String]): RowParser[Value.AbsoluteContractId] =
    SqlParser.get[Value.AbsoluteContractId](columnName)(columnToContractId(c))

  implicit def contractIdStringMetaParameter(implicit strParamMetaData: ParameterMetaData[String])
    : ParameterMetaData[Ref.ContractIdString] =
    subStringMetaParameter(strParamMetaData)

  // ChoiceNames

  implicit def columnToChoiceName(implicit c: Column[String]): Column[Ref.ChoiceName] =
    stringColumnToX(Ref.ChoiceName.fromString)(c)

  implicit def choiceNameToStatement(
      implicit strToStm: ToStatement[String],
  ): ToStatement[Ref.ChoiceName] =
    subStringToStatement(strToStm)

  implicit def choiceNameMetaParameter(
      implicit strParamMetaData: ParameterMetaData[String],
  ): ParameterMetaData[Ref.ChoiceName] =
    subStringMetaParameter(strParamMetaData)

  // QualifiedNames

  implicit def qualifiedNameToStatement(
      implicit strToStm: ToStatement[String],
  ): ToStatement[Ref.QualifiedName] =
    (s: PreparedStatement, index: Int, v: Ref.QualifiedName) => strToStm.set(s, index, v.toString)

  // Identifiers

  implicit def identifierToStatement(
      implicit strToStm: ToStatement[String],
  ): ToStatement[Ref.Identifier] =
    (s: PreparedStatement, index: Int, v: Ref.Identifier) => strToStm.set(s, index, v.toString)

  // Offsets

  implicit def offsetToStatement: ToStatement[Offset] = new ToStatement[Offset] {
    override def set(s: PreparedStatement, index: Int, v: Offset): Unit =
      s.setBinaryStream(index, v.toInputStream)
  }

  def offset(name: String): RowParser[Offset] =
    SqlParser.get[InputStream](name).map(Offset.fromInputStream)

  implicit def columnToOffset(implicit c: Column[InputStream]): Column[Offset] =
    Column.nonNull((value: Any, meta) => c(value, meta).toEither.map(Offset.fromInputStream))

  // Instant

  def instant(name: String): RowParser[Instant] =
    SqlParser.get[Date](name).map(_.toInstant)

}
