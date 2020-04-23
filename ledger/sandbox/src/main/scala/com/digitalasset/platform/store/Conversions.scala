// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.sql.PreparedStatement
import java.time.Instant
import java.util.Date

import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.value.Value

object Conversions {

  private def stringColumnToX[X](f: String => Either[String, X]): Column[X] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToString(value, meta).toEither.flatMap(x => f(x).left.map(SqlMappingError)))

  private final class SubTypeOfStringToStatement[S <: String] extends ToStatement[S] {
    override def set(s: PreparedStatement, i: Int, v: S): Unit =
      ToStatement.stringToStatement.set(s, i, v)
  }

  private final class ToStringToStatement[A] extends ToStatement[A] {
    override def set(s: PreparedStatement, i: Int, v: A): Unit =
      ToStatement.stringToStatement.set(s, i, v.toString)
  }

  private final class SubTypeOfStringMetaParameter[S <: String] extends ParameterMetaData[S] {
    override val sqlType: String = ParameterMetaData.StringParameterMetaData.sqlType
    override val jdbcType: Int = ParameterMetaData.StringParameterMetaData.jdbcType
  }

  // Party

  implicit val columnToParty: Column[Ref.Party] =
    stringColumnToX(Ref.Party.fromString)

  implicit val partyToStatement: ToStatement[Ref.Party] =
    new SubTypeOfStringToStatement[Ref.Party]

  implicit val partyMetaParameter: ParameterMetaData[Ref.Party] =
    new SubTypeOfStringMetaParameter[Ref.Party]

  def party(columnName: String): RowParser[Ref.Party] =
    SqlParser.get[Ref.Party](columnName)(columnToParty)

  // PackageId

  implicit val columnToPackageId: Column[Ref.PackageId] =
    stringColumnToX(Ref.PackageId.fromString)

  implicit val packageIdToStatement: ToStatement[Ref.PackageId] =
    new SubTypeOfStringToStatement[Ref.PackageId]

  def packageId(columnName: String): RowParser[Ref.PackageId] =
    SqlParser.get[Ref.PackageId](columnName)(columnToPackageId)

  // LedgerString

  implicit val columnToLedgerString: Column[Ref.LedgerString] =
    stringColumnToX(Ref.LedgerString.fromString)

  implicit val ledgerStringToStatement: ToStatement[Ref.LedgerString] =
    new SubTypeOfStringToStatement[Ref.LedgerString]

  def ledgerString(columnName: String): RowParser[Ref.LedgerString] =
    SqlParser.get[Ref.LedgerString](columnName)(columnToLedgerString)

  implicit val ledgerStringMetaParameter: ParameterMetaData[Ref.LedgerString] =
    new SubTypeOfStringMetaParameter[Ref.LedgerString]

  // ParticipantId

  implicit val columnToParticipantId: Column[Ref.ParticipantId] =
    stringColumnToX(Ref.ParticipantId.fromString)

  implicit val participantToStatement: ToStatement[Ref.ParticipantId] =
    new SubTypeOfStringToStatement[Ref.ParticipantId]

  implicit val participantIdMetaParameter: ParameterMetaData[Ref.ParticipantId] =
    new SubTypeOfStringMetaParameter[Ref.ParticipantId]

  def participantId(columnName: String): RowParser[Ref.ParticipantId] =
    SqlParser.get[Ref.ParticipantId](columnName)(columnToParticipantId)

  // AbsoluteContractId

  implicit val columnToContractId: Column[Value.AbsoluteContractId] =
    stringColumnToX(Value.AbsoluteContractId.fromString)

  implicit object ContractIdToStatement extends ToStatement[Value.AbsoluteContractId] {
    override def set(s: PreparedStatement, index: Int, v: Value.AbsoluteContractId): Unit =
      ToStatement.stringToStatement.set(s, index, v.coid)
  }

  def contractId(columnName: String): RowParser[Value.AbsoluteContractId] =
    SqlParser.get[Value.AbsoluteContractId](columnName)(columnToContractId)

  // ContractIdString

  implicit val contractIdStringMetaParameter: ParameterMetaData[Ref.ContractIdString] =
    new SubTypeOfStringMetaParameter[Ref.ContractIdString]

  // ChoiceName

  implicit val columnToChoiceName: Column[Ref.ChoiceName] =
    stringColumnToX(Ref.ChoiceName.fromString)

  implicit val choiceNameToStatement: ToStatement[Ref.ChoiceName] =
    new SubTypeOfStringToStatement[Ref.ChoiceName]

  implicit val choiceNameMetaParameter: ParameterMetaData[Ref.ChoiceName] =
    new SubTypeOfStringMetaParameter[Ref.ChoiceName]

  // QualifiedName

  implicit val qualifiedNameToStatement: ToStatement[Ref.QualifiedName] =
    new ToStringToStatement[Ref.QualifiedName]

  // Identifier

  implicit val IdentifierToStatement: ToStatement[Ref.Identifier] =
    new ToStringToStatement[Ref.Identifier]

  implicit val columnToIdentifier: Column[Ref.Identifier] =
    stringColumnToX(Ref.Identifier.fromString)

  def identifier(columnName: String): RowParser[Ref.Identifier] =
    SqlParser.get[Ref.Identifier](columnName)(columnToIdentifier)

  // Offset

  implicit object OffsetToStatement extends ToStatement[Offset] {
    override def set(s: PreparedStatement, index: Int, v: Offset): Unit =
      s.setBytes(index, v.toByteArray)
  }

  def offset(name: String): RowParser[Offset] =
    SqlParser.get[Array[Byte]](name).map(Offset.fromByteArray)

  implicit val columnToOffset: Column[Offset] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToByteArray(value, meta).toEither.map(Offset.fromByteArray))

  // Instant

  def instant(name: String): RowParser[Instant] =
    SqlParser.get[Date](name).map(_.toInstant)

  // Hash

  implicit object HashToStatement extends ToStatement[Hash] {
    override def set(s: PreparedStatement, i: Int, v: Hash): Unit =
      s.setBytes(i, v.bytes.toByteArray)
  }

  implicit val columnToHash: Column[Hash] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToByteArray(value, meta).toEither.map(Hash.assertFromByteArray))

  implicit object HashMetaParameter extends ParameterMetaData[Hash] {
    override val sqlType: String = ParameterMetaData.ByteArrayParameterMetaData.sqlType
    override val jdbcType: Int = ParameterMetaData.ByteArrayParameterMetaData.jdbcType
  }

}
