// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import anorm.Column.nonNull
import anorm._
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.CommandRejected
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value
import com.daml.platform.server.api.validation.ErrorFactories
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.BufferedReader
import java.sql.{PreparedStatement, Types}
import java.util.stream.Collectors
import scala.annotation.nowarn

// TODO append-only: split this file on cleanup, and move anorm/db conversion related stuff to the right place

private[platform] object OracleArrayConversions {
  implicit object PartyJsonFormat extends RootJsonFormat[Ref.Party] {
    def write(c: Ref.Party) =
      JsString(c)

    def read(value: JsValue) = value match {
      case JsString(s) => s.asInstanceOf[Ref.Party]
      case _ => deserializationError("Party expected")
    }
  }

  implicit object LedgerStringJsonFormat extends RootJsonFormat[Ref.LedgerString] {
    def write(c: Ref.LedgerString) =
      JsString(c)

    def read(value: JsValue) = value match {
      case JsString(s) => s.asInstanceOf[Ref.LedgerString]
      case _ => deserializationError("Ledger string expected")
    }
  }
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

  object DefaultImplicitArrayColumn {
    val default = Column.of[Array[String]]
  }

  object ArrayColumnToStringArray {
    // This is used to allow us to convert oracle CLOB fields storing JSON text into Array[String].
    // We first summon the default Anorm column for an Array[String], and run that - this preserves
    // the behavior PostgreSQL is expecting. If that fails, we then try our Oracle specific deserialization
    // strategies

    implicit val arrayColumnToStringArray: Column[Array[String]] = nonNull { (value, meta) =>
      DefaultImplicitArrayColumn.default(value, meta) match {
        case Right(value) => Right(value)
        case Left(_) =>
          val MetaDataItem(qualified, _, _) = meta
          value match {
            case jsonArrayString: String =>
              Right(jsonArrayString.parseJson.convertTo[Array[String]])
            case clob: java.sql.Clob =>
              try {
                val reader = clob.getCharacterStream
                val br = new BufferedReader(reader)
                val jsonArrayString = br.lines.collect(Collectors.joining)
                reader.close
                Right(jsonArrayString.parseJson.convertTo[Array[String]])
              } catch {
                case e: Throwable =>
                  Left(
                    TypeDoesNotMatch(
                      s"Cannot convert $value: received CLOB but failed to deserialize to " +
                        s"string array for column $qualified. Error message: ${e.getMessage}"
                    )
                  )
              }
            case _ =>
              Left(
                TypeDoesNotMatch(s"Cannot convert $value: to string array for column $qualified")
              )
          }
      }
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

  def flatEventWitnessesColumn(columnName: String): RowParser[Set[Ref.Party]] =
    SqlParser
      .get[Array[String]](columnName)(ArrayColumnToStringArray.arrayColumnToStringArray)
      .map(_.iterator.map(Ref.Party.assertFromString).toSet)

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

  // Timestamp

  def timestampFromMicros(name: String): RowParser[com.daml.lf.data.Time.Timestamp] =
    SqlParser.get[Long](name).map(com.daml.lf.data.Time.Timestamp.assertFromLong)

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

  implicit class RejectionReasonOps(rejectionReason: domain.RejectionReason) {
    @nowarn("msg=deprecated")
    def toParticipantStateRejectionReason(
        errorFactories: ErrorFactories
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): state.Update.CommandRejected.RejectionReasonTemplate =
      rejectionReason match {
        case domain.RejectionReason.ContractsNotFound(missingContractIds) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.contractsNotFound(missingContractIds)
          )
        case domain.RejectionReason.Inconsistent(reason) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.Deprecated.inconsistent(reason)
          )
        case domain.RejectionReason.InconsistentContractKeys(lookupResult, currentResult) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections
              .inconsistentContractKeys(lookupResult, currentResult)
          )
        case rejection @ domain.RejectionReason.DuplicateContractKey(key) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections
              .duplicateContractKey(rejection.description, key)
          )
        case domain.RejectionReason.Disputed(reason) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.Deprecated.disputed(reason)
          )
        case domain.RejectionReason.OutOfQuota(reason) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.Deprecated.outOfQuota(reason)
          )
        case domain.RejectionReason.PartiesNotKnownOnLedger(parties) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.partiesKnownToLedger(parties)
          )
        case domain.RejectionReason.PartyNotKnownOnLedger(description) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.partyNotKnownOnLedger(description)
          )
        case domain.RejectionReason.SubmitterCannotActViaParticipant(reason) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.submitterCannotActViaParticipant(reason)
          )
        case domain.RejectionReason.InvalidLedgerTime(reason) =>
          CommandRejected.FinalReason(
            errorFactories.CommandRejections.invalidLedgerTime(reason)
          )
      }
  }
}
