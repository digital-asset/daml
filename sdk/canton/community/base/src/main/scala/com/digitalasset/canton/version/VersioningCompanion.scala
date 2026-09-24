// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.OtherError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.{BinaryFileUtil, ReassignmentTag}
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import slick.jdbc.{GetResult, SetParameter}

import java.io.InputStream
import scala.annotation.nowarn
import scala.util.control.NonFatal

/** This trait has the logic to store proto (de)serializers and retrieve them by protocol version.
  *
  * Parameters and concepts are explained in
  * [[https://github.com/DACH-NY/canton/blob/main/contributing/how-to-choose-BaseVersioningCompanion.md contributing guide]]
  */
trait BaseVersioningCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    Context,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Dependency,
] {

  /** The name of the class as used for pretty-printing and error reporting */
  def name: String

  type Codec =
    ProtoCodec[ValueClass, Context, DeserializedValueClass, this.type, Dependency]

  type Deserializer =
    (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  protected type ThisRepresentativeProtocolVersion = RepresentativeProtocolVersion[this.type]

  type VersioningTable = SupportedProtoVersions[
    ValueClass,
    Context,
    DeserializedValueClass,
    this.type,
    Dependency,
  ]

  protected type Invariants = Seq[Invariant[ValueClass, this.type]]

  object VersioningTable {
    def apply(
        head: (ProtoVersion, Codec),
        tail: (ProtoVersion, Codec)*
    ): VersioningTable = SupportedProtoVersions.fromNonEmpty(name)(
      NonEmpty.mk(Seq, head, tail*)
    )
  }

  /** Will check that default value rules defined in [[invariants]] hold.
    */
  def validateInstance(
      instance: ValueClass,
      representativeProtocolVersion: ThisRepresentativeProtocolVersion,
  ): Either[String, Unit] =
    invariants.traverse_(_.validateInstance(instance, representativeProtocolVersion))

  def invariants: Seq[Invariant[ValueClass, this.type]] = Nil

  def protocolVersionRepresentativeFor(
      protocolVersion: ProtocolVersion
  ): RepresentativeProtocolVersion[this.type] =
    versioningTable.protocolVersionRepresentativeFor(protocolVersion)

  def protocolVersionRepresentativeFor(
      protoVersion: ProtoVersion
  ): ParsingResult[RepresentativeProtocolVersion[this.type]] =
    versioningTable.protocolVersionRepresentativeFor(protoVersion)

  def converterFor(
      protocolVersion: RepresentativeProtocolVersion[BaseVersioningCompanion.this.type]
  ): ParsingResult[Codec] = versioningTable.converterFor(protocolVersion)

  /** Return the Proto version corresponding to the representative protocol version
    */
  def protoVersionFor(
      protocolVersion: RepresentativeProtocolVersion[this.type]
  ): ProtoVersion = versioningTable.protoVersionFor(protocolVersion)

  /** Return the Proto version corresponding to the protocol version
    */
  def protoVersionFor(protocolVersion: ProtocolVersion): ProtoVersion =
    versioningTable.protoVersionFor(protocolVersionRepresentativeFor(protocolVersion))

  /** Proto versions that are supported by `fromByteString` See the helper `supportedProtoVersion`
    * below to define a `Parser`.
    */
  def versioningTable: VersioningTable

  /** Main deserialization method to parse a byte string
    * @param expectedProtocolVersion
    *   Protocol version used by the synchronizer
    * @param context
    *   Context for the deserialization (() if there is no context)
    * @param bytes
    *   Byte string to be deserialized
    * @return
    *   Deserialized value class (as a Right) or a [[ProtoDeserializationError]] (as a left)
    *
    * Variants of this method (e.g., when Context=unit) are provided below for convenience.
    */
  def fromByteString(
      expectedProtocolVersion: ProtocolVersionValidation,
      context: Context,
      bytes: OriginalByteString,
  ): ParsingResult[DeserializedValueClass] = for {
    valueClass <- fromTrustedByteString(context)(bytes)
    _ <- validateDeserialization(
      expectedProtocolVersion,
      valueClass.representativeProtocolVersion.representative,
    )
  } yield valueClass

  def fromByteString(expectedProtocolVersion: ProtocolVersion, context: Context)(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion), context, bytes)

  /** Alias of fromByteString that can be used when there is no context.
    */
  def fromByteString(expectedProtocolVersion: ProtocolVersion, bytes: OriginalByteString)(implicit
      ev: Unit =:= Context
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion), ev.apply(()), bytes)

  /** Alias of fromByteString that can be used when there is no context.
    */
  def fromByteString(expectedProtocolVersion: ProtocolVersionValidation, bytes: OriginalByteString)(
      implicit ev: Unit =:= Context
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(expectedProtocolVersion, ev.apply(()), bytes)

  /** Alias of fromByteString that can be used when the Context is a
    * [[com.digitalasset.canton.version.ProtocolVersion]].
    */
  def fromByteStringPV(expectedProtocolVersion: ProtocolVersion, bytes: OriginalByteString)(implicit
      ev: ProtocolVersion =:= Context
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(
      ProtocolVersionValidation(expectedProtocolVersion),
      ev.apply(expectedProtocolVersion),
      bytes,
    )

  /** Alias of fromByteString that can be used when the Context is a
    * [[com.digitalasset.canton.version.ProtocolVersionValidation]].
    */
  def fromByteStringPVV(
      expectedProtocolVersion: ProtocolVersionValidation,
      bytes: OriginalByteString,
  )(implicit
      ev: ProtocolVersionValidation =:= Context
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(expectedProtocolVersion, ev.apply(expectedProtocolVersion), bytes)

  /** Deserializes the given bytes without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the given bytes originate
    * from a trusted source. For example, this should be the case for deserialization of data that
    * originates from a database.
    *
    * @param bytes
    *   trusted bytes with an embedded proto version
    */
  def fromTrustedByteString(
      context: Context
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- versioningTable
      .deserializerFor(ProtoVersion(proto.version))(context, bytes, data)
  } yield valueClass

  /** Alias of fromTrustedByteString that can be used when there is no context.
    */
  def fromTrustedByteString(
      bytes: OriginalByteString
  )(implicit ev: Unit =:= Context): ParsingResult[DeserializedValueClass] =
    fromTrustedByteString(ev.apply(()))(bytes)

  /** Deserializes the given bytes without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the given bytes originate
    * from a trusted source. For example, this should be the case for deserialization of data that
    * originates from a database.
    *
    * @param bytes
    *   trusted bytes with an embedded proto version
    */
  def fromTrustedByteArray(
      context: Context,
      bytes: Array[Byte],
  ): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParserArray(v1.UntypedVersionedMessage.parseFrom)(bytes)
      data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
      valueClass <- versioningTable
        .deserializerFor(ProtoVersion(proto.version))(context, ByteString.copyFrom(bytes), data)
    } yield valueClass

  def fromTrustedByteArray(bytes: Array[Byte])(implicit
      ev: Unit =:= Context
  ): ParsingResult[DeserializedValueClass] = fromTrustedByteArray(ev.apply(()), bytes)

  /** Deserializes the data from the given file without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the data originates from a
    * trusted source.
    */
  def readFromTrustedFile(
      context: Context,
      inputFile: String,
  ): Either[String, DeserializedValueClass] =
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromTrustedByteString(context)(bs).leftMap(_.toString)
    } yield value

  def readFromTrustedFile(inputFile: String)(implicit
      ev: Unit =:= Context
  ): Either[String, DeserializedValueClass] =
    readFromTrustedFile(ev.apply(()), inputFile)

  def tryReadFromTrustedFile(
      context: Context,
      inputFile: String,
  ): DeserializedValueClass = readFromTrustedFile(context, inputFile).valueOr(err =>
    throw new RuntimeException(s"Unable to create a $name from file $inputFile: $err")
  )

  def tryReadFromTrustedFile(
      inputFile: String
  )(implicit
      ev: Unit =:= Context
  ): DeserializedValueClass = tryReadFromTrustedFile(ev.apply(()), inputFile)

  def readFromTrustedFilePVV(
      inputFile: String
  )(implicit ev: ProtocolVersionValidation =:= Context): Either[String, DeserializedValueClass] =
    readFromTrustedFile(ev.apply(ProtocolVersionValidation.NoValidation), inputFile)

  /** Since dependency on the ProtocolVersionValidation is encoded in the context, one still has to
    * provide `ProtocolVersionValidation.NoValidation` even when calling `fromTrustedByteString`,
    * which is counterintuitive. This method allows a simpler call if the Context is a
    * [[com.digitalasset.canton.version.ProtocolVersionValidation]]
    */
  def fromTrustedByteStringPVV(
      bytes: OriginalByteString
  )(implicit ev: ProtocolVersionValidation =:= Context): ParsingResult[DeserializedValueClass] =
    fromTrustedByteString(ev.apply(ProtocolVersionValidation.NoValidation))(bytes)

  /** Deserializes a message using a delimiter (the message length) from the given input stream.
    *
    * '''Unsafe!''' No deserialization validation is performed.
    *
    * Do NOT use this method unless you can justify that the given bytes originate from a trusted
    * source.
    *
    * This method works in conjunction with
    * [[com.digitalasset.canton.version.HasProtocolVersionedWrapper.writeDelimitedTo]] which should
    * have been used to serialize the message. It is useful for deserializing multiple messages from
    * a single input stream through repeated invocations.
    *
    * Deserialization is only supported for [[com.digitalasset.canton.version.VersionedMessage]].
    *
    * @param input
    *   the source from which a message is deserialized
    * @return
    *   an Option that is None when there are no messages left anymore, otherwise it wraps an Either
    *   where left represents a deserialization error (exception) and right represents the
    *   successfully deserialized message
    */
  def parseDelimitedFromTrusted(
      input: InputStream,
      context: Context,
  ): Option[ParsingResult[DeserializedValueClass]] = {
    def fromTrustedProtoVersioned(
        proto: VersionedMessage[DeserializedValueClass]
    ): ParsingResult[DeserializedValueClass] =
      proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
        bytes =>
          versioningTable.deserializerFor(ProtoVersion(proto.version))(context, bytes, bytes)
      }

    try {
      v1.UntypedVersionedMessage
        .parseDelimitedFrom(input)
        .map(VersionedMessage[DeserializedValueClass])
        .map(fromTrustedProtoVersioned)
    } catch {
      case protoBuffException: InvalidProtocolBufferException =>
        Some(Left(ProtoDeserializationError.BufferException(protoBuffException)))
      case NonFatal(e) =>
        Some(Left(ProtoDeserializationError.OtherError(e.getMessage)))
    }
  }

  def parseDelimitedFromTrusted(
      input: InputStream
  )(implicit ev: Unit =:= Context): Option[ParsingResult[DeserializedValueClass]] =
    parseDelimitedFromTrusted(input, ev.apply(()))

  /** Checks whether the representative protocol version originating from a deserialized proto
    * message version field value is compatible with the passed in expected protocol version.
    *
    * To skip this validation use [[ProtocolVersionValidation.NoValidation]].
    *
    * @param expectedProtocolVersion
    *   the protocol version the synchronizer is running on
    * @param deserializedRepresentativeProtocolVersion
    *   the representative protocol version which originates from a proto message version field
    * @return
    *   Unit when the validation succeeds, parsing error otherwise
    */
  private[version] def validateDeserialization(
      expectedProtocolVersion: ProtocolVersionValidation,
      deserializedRepresentativeProtocolVersion: ProtocolVersion,
  ): ParsingResult[Unit] =
    expectedProtocolVersion match {
      case ProtocolVersionValidation.PV(pv) =>
        val expected = protocolVersionRepresentativeFor(pv).representative
        Either.cond(
          expected == deserializedRepresentativeProtocolVersion,
          (),
          unexpectedProtoVersionError(expected, deserializedRepresentativeProtocolVersion),
        )
      case ProtocolVersionValidation.NoValidation =>
        Either.unit
    }

  private[version] def unexpectedProtoVersionError(
      expected: ProtocolVersion,
      found: ProtocolVersion,
  ) =
    OtherError(
      s"Error while deserializing a $name; expected representative protocol version $expected but found $found"
    )
}

trait VersioningCompanionMemoization2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
] extends BaseVersioningCompanion[
      ValueClass,
      Unit, // Context
      DeserializedValueClass,
      Unit, // Dependency
    ] {

  @nowarn("msg=parameter _ctx in anonymous function is never used")
  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    (_ctx: Unit, original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(_)(original))
}

trait VersioningCompanionContextMemoization2[
    ValueClass <: HasRepresentativeProtocolVersion,
    Context,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Dependency,
] extends BaseVersioningCompanion[
      ValueClass,
      Context,
      DeserializedValueClass,
      Dependency,
    ] {

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    (ctx: Context, original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _)(original))
}

trait VersioningCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
] extends BaseVersioningCompanion[
      ValueClass,
      Unit, // Context
      DeserializedValueClass,
      Unit, // Dependency
    ] {

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[DeserializedValueClass]
  ): Deserializer = { case (_, _, data: DataByteString) =>
    ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto)
  }

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[DeserializedValueClass] = GetResult { r =>
    fromTrustedByteArray(r.<<[Array[Byte]]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArray: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[DeserializedValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromTrustedByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }
}

trait VersioningCompanionContext2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Context,
] extends BaseVersioningCompanion[
      ValueClass,
      Context,
      DeserializedValueClass,
      Unit,
    ] {

  @nowarn("msg=parameter _original in anonymous function is never used")
  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => ParsingResult[DeserializedValueClass]
  ): Deserializer =
    (ctx: Context, _original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _))
}

/** For readability, replaces the deserialization methods for value classes that require the
  * protocol version for the deserialization validation to be passed in as part of the
  * deserialization context.
  *
  * Replaces `.fromByteString(protocolVersion)((context, protocolVersion))(bytes)` with
  * `.fromByteString(context, protocolVersion)(bytes)`.
  */
trait VersioningCompanionContextPVValidation2[
    ValueClass <: HasRepresentativeProtocolVersion,
    RawContext,
] extends VersioningCompanionContext2[
      ValueClass,
      ValueClass,
      (RawContext, ProtocolVersion),
    ] {
  def fromByteString(context: RawContext, expectedProtocolVersion: ProtocolVersion)(
      bytes: OriginalByteString
  ): ParsingResult[ValueClass] =
    super.fromByteString(expectedProtocolVersion, (context, expectedProtocolVersion))(bytes)
}

/** Similar to [[VersioningCompanionContextPVValidation2]] but the deserialization context contains
  * a Source or Target of [[com.digitalasset.canton.version.ProtocolVersion]] for validation.
  */
trait VersioningCompanionContextTaggedPVValidation2[
    ValueClass <: HasRepresentativeProtocolVersion,
    T[X] <: ReassignmentTag[X],
    RawContext,
] extends VersioningCompanionContext2[
      ValueClass,
      ValueClass,
      (RawContext, T[ProtocolVersion]),
    ] {
  def fromByteString(context: RawContext, expectedProtocolVersion: T[ProtocolVersion])(
      bytes: OriginalByteString
  ): ParsingResult[ValueClass] =
    super.fromByteString(expectedProtocolVersion.unwrap, (context, expectedProtocolVersion))(bytes)
}

/** Trait to be mixed in to have the set parameters defined in terms of the `.toByteString`
  */
trait ProtocolVersionedCompanionDbHelpers[ValueClass <: HasProtocolVersionedWrapper[ValueClass]] {
  def getVersionedSetParameter(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[ValueClass] = { (value, pp) =>
    pp >> value.toByteArray
  }

  def getVersionedSetParameterO(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[ValueClass]] = (valueO, pp) => pp >> valueO.map(_.toByteArray)
}
