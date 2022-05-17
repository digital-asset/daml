// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import anorm.Column.nonNull
import anorm._
import com.daml.ledger.offset.Offset
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.ledger.EventId
import com.daml.lf.value.Value
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.io.BufferedReader
import java.sql.{PreparedStatement, SQLNonTransientException}
import java.util.stream.Collectors
import scala.util.Try

private[backend] object Conversions {

  private def stringColumnToX[X](f: String => Either[String, X]): Column[X] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToString(value, meta).flatMap(x => f(x).left.map(SqlMappingError))
    )

  private final class SubTypeOfStringToStatement[S <: String] extends ToStatement[S] {
    override def set(s: PreparedStatement, i: Int, v: S): Unit =
      ToStatement.stringToStatement.set(s, i, v)
  }

  // Party

  private implicit val columnToParty: Column[Ref.Party] =
    stringColumnToX(Ref.Party.fromString)

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
    val defaultString: Column[Array[String]] = Column.of[Array[String]]
    val defaultInt: Column[Array[Int]] = Column.of[Array[Int]]
  }

  object ArrayColumnToIntArray {
    implicit val arrayColumnToIntArray: Column[Array[Int]] = nonNull { (value, meta) =>
      DefaultImplicitArrayColumn.defaultInt(value, meta) match {
        case Right(value) => Right(value)
        case Left(_) =>
          val MetaDataItem(qualified, _, _) = meta
          value match {
            case someArray: Array[_] =>
              Try(
                someArray.view.map {
                  case i: Int => i
                  case null =>
                    throw new SQLNonTransientException(
                      s"Cannot convert object array element null to Int"
                    )
                  case invalid =>
                    throw new SQLNonTransientException(
                      s"Cannot convert object array element (of type ${invalid.getClass.getName}) to Int"
                    )
                }.toArray
              ).toEither.left.map(t => TypeDoesNotMatch(t.getMessage))

            case jsonArrayString: String =>
              Right(jsonArrayString.parseJson.convertTo[Array[Int]])

            case clob: java.sql.Clob =>
              try {
                val reader = clob.getCharacterStream
                val br = new BufferedReader(reader)
                val jsonArrayString = br.lines.collect(Collectors.joining)
                reader.close
                Right(jsonArrayString.parseJson.convertTo[Array[Int]])
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

  object ArrayColumnToStringArray {
    // This is used to allow us to convert oracle CLOB fields storing JSON text into Array[String].
    // We first summon the default Anorm column for an Array[String], and run that - this preserves
    // the behavior PostgreSQL is expecting. If that fails, we then try our Oracle specific deserialization
    // strategies

    implicit val arrayColumnToStringArray: Column[Array[String]] = nonNull { (value, meta) =>
      DefaultImplicitArrayColumn.defaultString(value, meta) match {
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

  implicit val packageIdToStatement: ToStatement[Ref.PackageId] =
    new SubTypeOfStringToStatement[Ref.PackageId]

  // LedgerString

  private implicit val columnToLedgerString: Column[Ref.LedgerString] =
    stringColumnToX(Ref.LedgerString.fromString)

  implicit val ledgerStringToStatement: ToStatement[Ref.LedgerString] =
    new SubTypeOfStringToStatement[Ref.LedgerString]

  def ledgerString(columnName: String): RowParser[Ref.LedgerString] =
    SqlParser.get[Ref.LedgerString](columnName)(columnToLedgerString)

  // ApplicationId

  private implicit val columnToApplicationId: Column[Ref.ApplicationId] =
    stringColumnToX(Ref.ApplicationId.fromString)

  implicit val applicationIdToStatement: ToStatement[Ref.ApplicationId] =
    new SubTypeOfStringToStatement[Ref.ApplicationId]

  def applicationId(columnName: String): RowParser[Ref.ApplicationId] =
    SqlParser.get[Ref.ApplicationId](columnName)(columnToApplicationId)

  // EventId

  private implicit val columnToEventId: Column[EventId] =
    stringColumnToX(EventId.fromString)

  def eventId(columnName: String): RowParser[EventId] =
    SqlParser.get[EventId](columnName)(columnToEventId)

  // ParticipantId

  private implicit val columnToParticipantId: Column[Ref.ParticipantId] =
    stringColumnToX(Ref.ParticipantId.fromString)

  def participantId(columnName: String): RowParser[Ref.ParticipantId] =
    SqlParser.get[Ref.ParticipantId](columnName)(columnToParticipantId)

  // ContractIdString

  private implicit val columnToContractId: Column[Value.ContractId] =
    stringColumnToX(Value.ContractId.fromString)

  implicit object ContractIdToStatement extends ToStatement[Value.ContractId] {
    override def set(s: PreparedStatement, index: Int, v: Value.ContractId): Unit =
      ToStatement.stringToStatement.set(s, index, v.coid)
  }

  def contractId(columnName: String): RowParser[Value.ContractId] =
    SqlParser.get[Value.ContractId](columnName)(columnToContractId)

  // Offset

  implicit object OffsetToStatement extends ToStatement[Offset] {
    override def set(s: PreparedStatement, index: Int, v: Offset): Unit =
      s.setString(index, v.toHexString)
  }

  def offset(name: String): RowParser[Offset] =
    SqlParser.get[String](name).map(v => Offset.fromHexString(Ref.HexString.assertFromString(v)))

  def offset(position: Int): RowParser[Offset] =
    SqlParser
      .get[String](position)
      .map(v => Offset.fromHexString(Ref.HexString.assertFromString(v)))

  // Timestamp

  implicit def TimestampParamMeta: ParameterMetaData[Timestamp] = new ParameterMetaData[Timestamp] {
    val sqlType = "BIGINT"
    def jdbcType: Int = java.sql.Types.BIGINT
  }

  implicit object TimestampToStatement extends ToStatement[Timestamp] {
    override def set(s: PreparedStatement, index: Int, v: Timestamp): Unit =
      s.setLong(index, v.micros)
  }

  def timestampFromMicros(name: String): RowParser[com.daml.lf.data.Time.Timestamp] =
    SqlParser.get[Long](name).map(com.daml.lf.data.Time.Timestamp.assertFromLong)

  // Hash

  implicit object HashToStatement extends ToStatement[Hash] {
    override def set(s: PreparedStatement, i: Int, v: Hash): Unit =
      s.setString(i, v.bytes.toHexString)
  }

  def hashFromHexString(name: String): RowParser[Hash] =
    SqlParser.get[String](name).map(Hash.assertFromString)
}
