// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.sql.{PreparedStatement, Timestamp}
import java.time.Instant
import java.util.Date

import anorm._
import com.daml.ledger.EventId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.value.Value

private[platform] object Conversions {

  private def stringColumnToX[X](f: String => Either[String, X]): Column[X] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToString(value, meta).flatMap(x => f(x).left.map(SqlMappingError))
    )

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

  // EventId

  implicit val columnToEventId: Column[EventId] =
    stringColumnToX(EventId.fromString)

  implicit val eventIdToStatement: ToStatement[EventId] =
    (s: PreparedStatement, i: Int, v: EventId) =>
      ToStatement.stringToStatement.set(s, i, v.toLedgerString)

  def eventId(columnName: String): RowParser[EventId] =
    SqlParser.get[EventId](columnName)

  implicit val eventIdMetaParameter: ParameterMetaData[EventId] = new ParameterMetaData[EventId] {
    override val sqlType: String = ParameterMetaData.StringParameterMetaData.sqlType
    override val jdbcType: Int = ParameterMetaData.StringParameterMetaData.jdbcType
  }

  // ParticipantId

  implicit val columnToParticipantId: Column[Ref.ParticipantId] =
    stringColumnToX(Ref.ParticipantId.fromString)

  implicit val participantToStatement: ToStatement[Ref.ParticipantId] =
    new SubTypeOfStringToStatement[Ref.ParticipantId]

  implicit val participantIdMetaParameter: ParameterMetaData[Ref.ParticipantId] =
    new SubTypeOfStringMetaParameter[Ref.ParticipantId]

  def participantId(columnName: String): RowParser[Ref.ParticipantId] =
    SqlParser.get[Ref.ParticipantId](columnName)(columnToParticipantId)

  implicit val columnToContractId: Column[Value.ContractId] =
    stringColumnToX(Value.ContractId.fromString)

  implicit object ContractIdToStatement extends ToStatement[Value.ContractId] {
    override def set(s: PreparedStatement, index: Int, v: Value.ContractId): Unit =
      ToStatement.stringToStatement.set(s, index, v.coid)
  }

  def contractId(columnName: String): RowParser[Value.ContractId] =
    SqlParser.get[Value.ContractId](columnName)(columnToContractId)

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
      Column.columnToByteArray(value, meta).map(Offset.fromByteArray)
    )

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
      Column.columnToByteArray(value, meta).map(Hash.assertFromByteArray)
    )

  implicit object HashMetaParameter extends ParameterMetaData[Hash] {
    override val sqlType: String = ParameterMetaData.ByteArrayParameterMetaData.sqlType
    override val jdbcType: Int = ParameterMetaData.ByteArrayParameterMetaData.jdbcType
  }

  // Array[String]

  implicit object StringArrayParameterMetadata extends ParameterMetaData[Array[String]] {
    override def sqlType: String = "ARRAY"
    override def jdbcType: Int = java.sql.Types.ARRAY
  }

  abstract sealed class ArrayToStatement[T](postgresTypeName: String)
      extends ToStatement[Array[T]] {
    override def set(s: PreparedStatement, index: Int, v: Array[T]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf(postgresTypeName, v.asInstanceOf[Array[AnyRef]])
      s.setArray(index, ts)
    }
  }

  object IntToSmallIntConversions {
    implicit object IntOptionArrayArrayToStatement extends ToStatement[Array[Option[Int]]] {
      override def set(s: PreparedStatement, index: Int, intOpts: Array[Option[Int]]): Unit = {
        val conn = s.getConnection
        val intOrNullsArray = intOpts.map(_.map(new Integer(_)).orNull)
        val ts = conn.createArrayOf("SMALLINT", intOrNullsArray.asInstanceOf[Array[AnyRef]])
        s.setArray(index, ts)
      }
    }
  }

  implicit object ByteArrayArrayToStatement extends ArrayToStatement[Array[Byte]]("BYTEA")

  implicit object CharArrayToStatement extends ArrayToStatement[String]("VARCHAR")

  implicit object IntArrayToStatement extends ToStatement[Array[Int]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Int]): Unit = {
      s.setObject(index, v)
    }
  }

  implicit object TimestampArrayToStatement extends ArrayToStatement[Timestamp]("TIMESTAMP")

  implicit object InstantArrayToStatement extends ToStatement[Array[Instant]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Instant]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("TIMESTAMP", v.map(i => if (i == null) null else java.sql.Timestamp.from(i)))
      s.setArray(index, ts)
    }
  }

}
