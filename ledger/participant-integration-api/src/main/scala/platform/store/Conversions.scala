// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.sql.{Connection, JDBCType, PreparedStatement, Timestamp, Types}
import java.time.Instant
import java.util.Date

import anorm.Column.nonNull
import anorm._
import com.daml.ledger.EventId
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.v1.RejectionReasonV0._
import com.daml.ledger.participant.state.v1.{Offset, RejectionReasonV0}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.value.Value
import com.zaxxer.hikari.pool.HikariProxyConnection
import io.grpc.Status.Code

import scala.language.implicitConversions

private[platform] object OracleArrayConversions {
  import oracle.jdbc.OracleConnection

  implicit object StringArrayParameterMetadata extends ParameterMetaData[Array[String]] {
    override def sqlType: String = "ARRAY"
    override def jdbcType: Int = java.sql.Types.ARRAY
  }

  // Oracle does not support the boolean SQL type, so we need to treat it as integer
  // when setting nulls
  implicit object BooleanParameterMetaData extends ParameterMetaData[Boolean] {
    val sqlType = "INTEGER"
    val jdbcType = Types.INTEGER
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  abstract sealed class ArrayToStatement[T](oracleTypeName: String)
      extends ToStatement[Array[T]]
      with NotNullGuard {
    override def set(s: PreparedStatement, index: Int, v: Array[T]): Unit = {
      if (v == (null: AnyRef)) {
        s.setNull(index, JDBCType.ARRAY.getVendorTypeNumber, oracleTypeName)
      } else {
        s.setObject(
          index,
          unwrapConnection(s).createARRAY(oracleTypeName, v.asInstanceOf[Array[AnyRef]]),
          JDBCType.ARRAY.getVendorTypeNumber,
        )
      }
    }
  }

  implicit object ByteArrayArrayToStatement
      extends ArrayToStatement[Array[Byte]]("BYTE_ARRAY_ARRAY")

  implicit object TimestampArrayToStatement extends ArrayToStatement[Timestamp]("TIMESTAMP_ARRAY")

  implicit object RefPartyArrayToStatement extends ArrayToStatement[Ref.Party]("VARCHAR_ARRAY")

  implicit object CharArrayToStatement extends ArrayToStatement[String]("VARCHAR_ARRAY")

  implicit object IntegerArrayToStatement extends ArrayToStatement[Integer]("SMALLINT_ARRAY")

  implicit object BooleanArrayToStatement
      extends ArrayToStatement[java.lang.Boolean]("BOOLEAN_ARRAY")

  implicit object InstantArrayToStatement extends ToStatement[Array[Instant]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Instant]): Unit = {
      s.setObject(
        index,
        unwrapConnection(s).createARRAY("TIMESTAMP_ARRAY", v.map(java.sql.Timestamp.from)),
        JDBCType.ARRAY.getVendorTypeNumber,
      )
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
  implicit object StringOptionArrayArrayToStatement extends ToStatement[Option[Array[String]]] {
    override def set(s: PreparedStatement, index: Int, stringOpts: Option[Array[String]]): Unit = {
      stringOpts match {
        case None => s.setNull(index, JDBCType.ARRAY.getVendorTypeNumber, "VARCHAR_ARRAY")
        case Some(arr) =>
          s.setObject(
            index,
            unwrapConnection(s)
              .createARRAY("VARCHAR_ARRAY", arr.asInstanceOf[Array[AnyRef]]),
            JDBCType.ARRAY.getVendorTypeNumber,
          )
      }
    }
  }

  object IntToSmallIntConversions {

    implicit object IntOptionArrayArrayToStatement extends ToStatement[Array[Option[Int]]] {
      override def set(s: PreparedStatement, index: Int, intOpts: Array[Option[Int]]): Unit = {
        val intOrNullsArray = intOpts.map(_.map(new Integer(_)).orNull)
        s.setObject(
          index,
          unwrapConnection(s)
            .createARRAY("SMALLINT_ARRAY", intOrNullsArray.asInstanceOf[Array[AnyRef]]),
          JDBCType.ARRAY.getVendorTypeNumber,
        )
      }
    }
  }

  private def unwrapConnection[T](s: PreparedStatement): OracleConnection = {
    s.getConnection match {
      case hikari: HikariProxyConnection =>
        hikari.unwrap(classOf[OracleConnection])
      case oracle: OracleConnection =>
        oracle
      case c: Connection =>
        sys.error(
          s"Unsupported connection type for creating Oracle integer array: ${c.getClass.getSimpleName}"
        )
    }
  }
}

private[platform] object JdbcArrayConversions {

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

  implicit object TimestampArrayToStatement extends ArrayToStatement[Timestamp]("TIMESTAMP")

  implicit object InstantArrayToStatement extends ToStatement[Array[Instant]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Instant]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("TIMESTAMP", v.map(java.sql.Timestamp.from))
      s.setArray(index, ts)
    }
  }

}

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

  // booleans are stored as BigDecimal 0/1 in oracle, need to do implicit conversion when reading from db
  implicit val bigDecimalColumnToBoolean: Column[Boolean] = nonNull { (value, meta) =>
    val MetaDataItem(qualified, _, _) = meta
    value match {
      case bd: java.math.BigDecimal => Right(bd.equals(new java.math.BigDecimal(1)))
      case bool: Boolean => Right(bool)
      case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: to Boolean for column $qualified"))
    }
  }

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

  def flatEventWitnessesColumn(columnName: String): RowParser[Set[Party]] =
    SqlParser
      .get[Array[String]](columnName)(Column.columnToArray)
      .map(_.iterator.map(Party.assertFromString).toSet)

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
      s.setString(index, v.toHexString)
  }

  def offset(name: String): RowParser[Offset] =
    SqlParser.get[String](name).map(v => Offset.fromHexString(Ref.HexString.assertFromString(v)))

  implicit val columnToOffset: Column[Offset] =
    Column.nonNull((value: Any, meta) =>
      Column
        .columnToString(value, meta)
        .map(v => Offset.fromHexString(Ref.HexString.assertFromString(v)))
    )

  // Instant

  def instant(name: String): RowParser[Instant] =
    SqlParser.get[Date](name).map(_.toInstant)

  // Hash

  implicit object HashToStatement extends ToStatement[Hash] {
    override def set(s: PreparedStatement, i: Int, v: Hash): Unit =
      s.setString(i, v.bytes.toHexString)
  }

  implicit val columnToHash: Column[Hash] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToString(value, meta).map(Hash.assertFromString)
    )

  implicit object HashMetaParameter extends ParameterMetaData[Hash] {
    override val sqlType: String = ParameterMetaData.StringParameterMetaData.sqlType
    override val jdbcType: Int = ParameterMetaData.StringParameterMetaData.jdbcType
  }

  // RejectionReason
  implicit def domainRejectionReasonToErrorCode(reason: domain.RejectionReason): Code =
    domainRejectionReasonToParticipantRejectionReason(
      reason
    ).code

  implicit def domainRejectionReasonToParticipantRejectionReason(
      reason: domain.RejectionReason
  ): RejectionReasonV0 =
    reason match {
      case r: domain.RejectionReason.Inconsistent => Inconsistent(r.description)
      case r: domain.RejectionReason.Disputed => Disputed(r.description)
      case r: domain.RejectionReason.OutOfQuota => ResourcesExhausted(r.description)
      case r: domain.RejectionReason.PartyNotKnownOnLedger => PartyNotKnownOnLedger(r.description)
      case r: domain.RejectionReason.SubmitterCannotActViaParticipant =>
        SubmitterCannotActViaParticipant(r.description)
      case r: domain.RejectionReason.InvalidLedgerTime => InvalidLedgerTime(r.description)
    }
}
