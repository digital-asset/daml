// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.ProtoDeserializationError.{
  BufferException,
  FieldNotSet,
  StringConversionError,
  TimestampConversionError,
}
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedString,
  LengthLimitedStringCompanion,
}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeLong,
  PositiveDouble,
  PositiveInt,
  PositiveLong,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.{
  LedgerApplicationId,
  LedgerParticipantId,
  LedgerSubmissionId,
  LfPartyId,
  LfWorkflowId,
  ProtoDeserializationError,
}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.timestamp.Timestamp
import com.google.protobuf.{ByteString, CodedInputStream, InvalidProtocolBufferException}

import java.time.{DateTimeException, Duration, Instant}
import java.util.UUID

/** Can convert messages to and from proto objects
  * @tparam A type of the message to be serialized
  * @tparam Proto type of the proto message
  * @tparam Err type of deserialization errors
  */
trait ProtoConverter[A, Proto, Err] {

  /** Convert an instance to a protobuf structure
    * @param value to be serialized
    * @return serialized proto
    */
  def toProtoPrimitive(value: A): Proto

  /** Convert proto value to its native type
    * @param value to be deserialized
    * @return deserialized value
    */
  def fromProtoPrimitive(value: Proto): Either[Err, A]
}

object ProtoConverter {
  type ParsingResult[+T] = Either[ProtoDeserializationError, T]

  /** Helper to convert protobuf exceptions into ProtoDeserializationErrors
    *
    * i.e. usage: ProtoConverter.protoParser(v0.MessageContent.parseFrom)
    */
  def protoParser[A](parseFrom: CodedInputStream => A): ByteString => Either[BufferException, A] =
    bytes =>
      Either
        .catchOnly[InvalidProtocolBufferException](parseFrom(bytes.newCodedInput))
        .leftMap(BufferException.apply)

  def protoParserArray[A](parseFrom: Array[Byte] => A): Array[Byte] => Either[BufferException, A] =
    bytes =>
      Either
        .catchOnly[InvalidProtocolBufferException](parseFrom(bytes))
        .leftMap(BufferException.apply)

  /** Helper for extracting an optional field where the value is required
    * @param field the field name
    * @param optValue the optional value
    * @return a [[scala.Right$]] of the value if set or
    *         a [[scala.Left$]] of [[com.digitalasset.canton.ProtoDeserializationError.FieldNotSet]] error
    */
  def required[B](field: String, optValue: Option[B]): Either[FieldNotSet, B] =
    optValue.toRight(FieldNotSet(field))

  def parseRequired[A, P](
      fromProto: P => ParsingResult[A],
      field: String,
      optValue: Option[P],
  ): ParsingResult[A] =
    required(field, optValue).flatMap(fromProto)

  def parse[A, P](
      parseFrom: CodedInputStream => P,
      fromProto: P => ParsingResult[A],
      value: ByteString,
  ): ParsingResult[A] =
    protoParser(parseFrom)(value).flatMap(fromProto)

  def parseEnum[A, P](
      fromProto: P => ParsingResult[Option[A]],
      field: String,
      value: P,
  ): ParsingResult[A] = fromProto(value).sequence.getOrElse(Left(FieldNotSet(field)))

  def parseRequiredNonEmpty[A, P](
      fromProto: P => ParsingResult[A],
      field: String,
      content: Seq[P],
  ): ParsingResult[NonEmpty[Seq[A]]] =
    for {
      contentNE <- NonEmpty
        .from(content)
        .toRight(ProtoDeserializationError.FieldNotSet(s"Sequence $field not set or empty"))
      parsed <- contentNE.toNEF.traverse(fromProto)
    } yield parsed

  def parsePositiveInt(field: String, i: Int): ParsingResult[PositiveInt] =
    PositiveInt.create(i).leftMap(ProtoDeserializationError.InvariantViolation(field, _))

  def parsePositiveLong(field: String, l: Long): ParsingResult[PositiveLong] =
    PositiveLong
      .create(l)
      .leftMap(ProtoDeserializationError.InvariantViolation(field, _))

  def parsePositiveDouble(field: String, i: Double): ParsingResult[PositiveDouble] =
    PositiveDouble.create(i).leftMap(ProtoDeserializationError.InvariantViolation(field, _))

  def parseNonNegativeInt(field: String, i: Int): ParsingResult[NonNegativeInt] =
    NonNegativeInt
      .create(i)
      .leftMap(ProtoDeserializationError.InvariantViolation(field, _))

  def parseNonNegativeLong(field: String, l: Long): ParsingResult[NonNegativeLong] =
    NonNegativeLong
      .create(l)
      .leftMap(ProtoDeserializationError.InvariantViolation(field, _))

  def parseLfPartyId(party: String, field: String): ParsingResult[LfPartyId] =
    parseString(party, field = Some(field))(LfPartyId.fromString)

  def parseLfParticipantId(party: String, field: String): ParsingResult[LedgerParticipantId] =
    parseString(party, field = Some(field))(LedgerParticipantId.fromString)

  def parseLFApplicationId(applicationId: String): ParsingResult[LedgerApplicationId] =
    parseString(applicationId, field = None)(LedgerApplicationId.fromString)

  def parseLFSubmissionIdO(submissionId: String): ParsingResult[Option[LedgerSubmissionId]] =
    Option
      .when(submissionId.nonEmpty)(parseLFSubmissionId(submissionId))
      .sequence

  def parseLFSubmissionId(submissionId: String): ParsingResult[LedgerSubmissionId] =
    parseString(submissionId, field = None)(LedgerSubmissionId.fromString)

  def parseLFWorkflowIdO(workflowId: String): ParsingResult[Option[LfWorkflowId]] =
    Option
      .when(workflowId.nonEmpty)(parseString(workflowId, field = None)(LfWorkflowId.fromString))
      .sequence

  def parseLfContractId(id: String): ParsingResult[LfContractId] =
    parseString(id, field = None)(LfContractId.fromString)

  def parseCommandId(id: String): ParsingResult[Ref.CommandId] =
    parseString(id, field = None)(Ref.CommandId.fromString)

  def parsePackageId(id: String): ParsingResult[Ref.PackageId] =
    parseString(id, field = None)(Ref.PackageId.fromString)

  private def parseString[T](from: String, field: Option[String])(
      to: String => Either[String, T]
  ): ParsingResult[T] =
    to(from).leftMap(StringConversionError.apply(_, field))

  def parseLengthLimitedString[LLS <: LengthLimitedString](
      companion: LengthLimitedStringCompanion[LLS],
      s: String,
  ): ParsingResult[LLS] = companion.create(s).leftMap(StringConversionError.apply(_, None))

  object InstantConverter extends ProtoConverter[Instant, Timestamp, ProtoDeserializationError] {
    override def toProtoPrimitive(value: Instant): Timestamp =
      Timestamp(value.getEpochSecond, value.getNano)

    override def fromProtoPrimitive(proto: Timestamp): ParsingResult[Instant] =
      try {
        Right(Instant.ofEpochSecond(proto.seconds, proto.nanos.toLong))
      } catch {
        case _: DateTimeException =>
          Left(TimestampConversionError("timestamp exceeds min or max of Instant"))
        case _: ArithmeticException => Left(TimestampConversionError("numeric overflow"))
      }

  }

  object DurationConverter
      extends ProtoConverter[
        java.time.Duration,
        com.google.protobuf.duration.Duration,
        ProtoDeserializationError,
      ] {
    override def toProtoPrimitive(duration: Duration): com.google.protobuf.duration.Duration =
      com.google.protobuf.duration.Duration(duration.getSeconds, duration.getNano)
    override def fromProtoPrimitive(
        duration: com.google.protobuf.duration.Duration
    ): ParsingResult[java.time.Duration] =
      Right(java.time.Duration.ofSeconds(duration.seconds, duration.nanos.toLong))
  }

  object UuidConverter extends ProtoConverter[UUID, String, StringConversionError] {
    override def toProtoPrimitive(uuid: UUID): String = uuid.toString

    override def fromProtoPrimitive(uuidP: String): Either[StringConversionError, UUID] =
      Either
        .catchOnly[IllegalArgumentException](UUID.fromString(uuidP))
        .leftMap(err => StringConversionError(err.getMessage))
  }
}
