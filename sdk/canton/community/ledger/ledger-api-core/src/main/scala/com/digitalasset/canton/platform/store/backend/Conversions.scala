// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.*
import anorm.Column.nonNull
import com.daml.ledger.api.v2.trace_context.TraceContext as ProtoTraceContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.tracing.{SerializableTraceContextConverter, TraceContext}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.Logger

import java.nio.ByteBuffer
import java.sql.PreparedStatement

private[backend] object Conversions {

  private def stringColumnToX[X](f: String => Either[String, X]): Column[X] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToString(value, meta).flatMap(x => f(x).left.map(SqlMappingError.apply))
    )

  private def binaryColumnToX[X](f: Array[Byte] => Either[String, X]): Column[X] =
    Column.nonNull((value: Any, meta) =>
      Column.columnToByteArray(value, meta).flatMap(x => f(x).left.map(SqlMappingError.apply))
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

  implicit val bigDecimalColumnToBoolean: Column[Boolean] = nonNull { (value, meta) =>
    val MetaDataItem(qualified, _, _) = meta
    value match {
      case bd: java.math.BigDecimal => Right(bd.equals(new java.math.BigDecimal(1)))
      case bool: Boolean => Right(bool)
      case _ => Left(TypeDoesNotMatch(s"Cannot convert $value: to Boolean for column $qualified"))
    }
  }

  def parties(stringInterning: StringInterning)(columName: String): RowParser[Seq[Ref.Party]] =
    SqlParser
      .byteArray(columName)
      .map(IntArrayDBSerialization.decodeFromByteArray)
      .map(_.map(stringInterning.party.externalize))

  // PackageId

  implicit val packageIdToStatement: ToStatement[Ref.PackageId] =
    new SubTypeOfStringToStatement[Ref.PackageId]

  // ParticipantId

  private implicit val columnToParticipantId: Column[Ref.ParticipantId] =
    stringColumnToX(Ref.ParticipantId.fromString)

  def participantId(columnName: String): RowParser[Ref.ParticipantId] =
    SqlParser.get[Ref.ParticipantId](columnName)(columnToParticipantId)

  // ContractId

  private implicit val columnToContractId: Column[Value.ContractId] =
    binaryColumnToX(byteArray => Value.ContractId.fromBytes(Bytes.fromByteArray(byteArray)))

  def contractId(columnName: String): RowParser[Value.ContractId] =
    SqlParser.get[Value.ContractId](columnName)(columnToContractId)

  // Offset

  implicit object OffsetToStatement extends ToStatement[Offset] {
    override def set(s: PreparedStatement, index: Int, v: Offset): Unit =
      s.setLong(index, v.unwrap)
  }

  def offset(name: String): RowParser[Offset] =
    SqlParser
      .get[Long](name)
      .map(Offset.tryFromLong)

  def offset(position: Int): RowParser[Offset] =
    SqlParser
      .get[Long](position)
      .map(Offset.tryFromLong)

  // Timestamp

  def timestampFromMicros(name: String): RowParser[com.digitalasset.daml.lf.data.Time.Timestamp] =
    SqlParser.get[Long](name).map(com.digitalasset.daml.lf.data.Time.Timestamp.assertFromLong)

  // Hash

  implicit object HashToStatement extends ToStatement[Hash] {
    override def set(s: PreparedStatement, i: Int, v: Hash): Unit =
      s.setString(i, v.bytes.toHexString)
  }

  def hashFromHexString(name: String): RowParser[Hash] =
    SqlParser.get[String](name).map(Hash.assertFromString)

  def traceContextOption(bytes: Option[Array[Byte]])(implicit logger: Logger): TraceContext =
    bytes
      .map(b =>
        SerializableTraceContextConverter
          .fromDamlProtoSafeOpt(logger)(
            Some(ProtoTraceContext.parseFrom(b))
          )
          .traceContext
      )
      .getOrElse(TraceContext.empty)

  // UpdateId

  implicit object UpdateIdToStatement extends ToStatement[UpdateId] {
    override def set(s: PreparedStatement, i: Int, v: UpdateId): Unit =
      s.setBytes(i, v.getCryptographicEvidence.toByteArray)
  }

  private implicit val columnToUpdateId: Column[UpdateId] =
    binaryColumnToX(byteArray =>
      UpdateId.fromProtoPrimitive(ByteString.copyFrom(byteArray)).left.map(_.message)
    )

  def updateId(columnName: String): RowParser[UpdateId] =
    SqlParser.get[UpdateId](columnName)(columnToUpdateId)

  // AuthorizationEvent

  private lazy val authorizationLevelToIntMapping: Map[AuthorizationLevel, Int] = Map(
    Submission -> 0,
    Confirmation -> 1,
    Observation -> 2,
  )

  private def authorizationLevel(n: Int): AuthorizationLevel =
    authorizationLevelToIntMapping
      .map(_.swap)
      .getOrElse(
        n,
        throw new RuntimeException(
          s"Integer $n was not expected as an authorization level serialized value."
        ),
      )

  def participantPermissionInt(authorizationEvent: AuthorizationEvent): Int =
    authorizationEvent match {
      case active: AuthorizationEvent.ActiveAuthorization =>
        authorizationLevelToIntMapping.getOrElse(
          active.level,
          throw new RuntimeException(
            s"Unexpectedly level ${active.level} could not be serialized."
          ),
        )
      case Revoked => 0 // we do not care about the permission level if the mapping is revoked
    }

  def authorizationEventInt(state: AuthorizationEvent): Int = state match {
    case Added(_) => 0
    case ChangedTo(_) => 1
    case Revoked => 2
  }

  def authorizationEvent(t: Int, l: Int): AuthorizationEvent = t match {
    case 0 => Added(authorizationLevel(l))
    case 1 => ChangedTo(authorizationLevel(l))
    case 2 => Revoked
    case other =>
      throw new RuntimeException(
        s"Integer $other was not expected as an authorization event serialized value."
      )
  }

  object IntArrayDBSerialization {
    // Ints to Byte Array (with version byte prefix)
    def encodeToByteArray(ints: Set[Int]): Array[Byte] =
      if (ints.nonEmpty) {
        val buffer = ByteBuffer.allocate(1 + ints.size * 4)
        buffer.put(1.toByte) // version byte
        ints.foreach(buffer.putInt(_).discard)
        buffer.array()
      } else Array.emptyByteArray

    // Ints from Byte Array (with prefix version byte)
    def decodeFromByteArray(bytes: Array[Byte]): Seq[Int] =
      if (bytes.sizeIs > 1) {
        val buf = ByteBuffer.wrap(bytes)
        // first byte = version
        val version = buf.get().toInt
        if (version != 1) {
          throw new IllegalArgumentException(
            s"Decoding the bytes to integers failed. Unknown version: $version. The first byte is used as the version byte and should be set to 1."
          )
        }

        // remaining are 4-byte ints
        val ints = Iterator
          .continually(if (buf.remaining() >= 4) Some(buf.getInt()) else None)
          .takeWhile(_.isDefined)
          .flatten
          .toSeq

        ints
      } else Seq.empty

  }
}
