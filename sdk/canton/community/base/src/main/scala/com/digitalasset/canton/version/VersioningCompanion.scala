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
import scala.util.control.NonFatal

/** This trait has the logic to store proto (de)serializers and retrieve them by protocol version.
  *
  * Parameters and concepts are explained in [[https://github.com/DACH-NY/canton/blob/main/contributing/how-to-choose-BaseVersioningCompanion.md contributing guide]]
  */
trait BaseVersioningCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializationDomain,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Dependency,
] {

  /** The name of the class as used for pretty-printing and error reporting */
  def name: String

  type Deserializer = DeserializationDomain => ParsingResult[DeserializedValueClass]

  type Codec =
    ProtoCodec[ValueClass, DeserializationDomain, DeserializedValueClass, this.type, Dependency]

  protected type ThisRepresentativeProtocolVersion = RepresentativeProtocolVersion[this.type]

  type VersioningTable = SupportedProtoVersions[
    ValueClass,
    DeserializationDomain,
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

  /** Proto versions that are supported by `fromByteString`
    * See the helper `supportedProtoVersion` below to define a `Parser`.
    */
  def versioningTable: VersioningTable

  /** Checks whether the representative protocol version originating from a deserialized proto
    * message version field value is compatible with the passed in expected protocol version.
    *
    * To skip this validation use [[ProtocolVersionValidation.NoValidation]].
    *
    * @param expectedProtocolVersion the protocol version the synchronizer is running on
    * @param deserializedRepresentativeProtocolVersion the representative protocol version which originates from a proto message version field
    * @return Unit when the validation succeeds, parsing error otherwise
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

private[version] trait VersioningCompanionNoContext2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializationDomain,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
] extends BaseVersioningCompanion[
      ValueClass,
      DeserializationDomain,
      DeserializedValueClass,
      Unit,
    ] {

  /** Deserializes the given bytes and checks that the therein embedded proto version matches
    * the `expectedProtocolVersion`.
    *
    * Use this method whenever the origin of the given bytes cannot be trusted, and for example the
    * proto version message field may be set maliciously. This should be your default choice for
    * deserialization.
    *
    * @param expectedProtocolVersion the protocol version on which the synchronizer is running on
    * @param bytes                 an untrusted byte string with an embedded proto version
    */
  def fromByteString(
      expectedProtocolVersion: ProtocolVersion
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass]

  /** Variation of `fromByteString` that takes a `ProtocolVersionValidation` instead of a `ProtocolVersion`.
    *
    * Use this method when sometimes no protocol version can be passed for the `expectedProtocolVersion`.
    * For these cases use `ProtocolVersionValidation.NoValidation`.
    */
  def fromByteString(
      expectedProtocolVersion: ProtocolVersionValidation
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass]

  /** Deserializes the given bytes without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the given bytes originate from a trusted
    * source.
    *
    * @param bytes                 a trusted byte string with an embedded proto version
    */
  def fromTrustedByteString(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass]

  /** Deserializes the data from the given file without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the data originates from a trusted
    * source.
    */
  def readFromTrustedFile(
      inputFile: String
  ): Either[String, DeserializedValueClass] =
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromTrustedByteString(bs).leftMap(_.toString)
    } yield value

  /** Deserializes the data from the given file without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the data originates from a trusted
    * source.
    */
  def tryReadFromTrustedFile(inputFile: String): DeserializedValueClass =
    readFromTrustedFile(inputFile).valueOr(err =>
      throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
    )

}

trait VersioningCompanionNoContextMemoization2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
] extends VersioningCompanionNoContext2[
      ValueClass,
      (OriginalByteString, DataByteString),
      DeserializedValueClass,
    ] {

  override type Codec =
    ProtoCodec[
      ValueClass,
      (OriginalByteString, DataByteString),
      DeserializedValueClass,
      this.type,
      Unit,
    ]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    input => {
      val (original, data) = input
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(_)(original))
    }

  def fromTrustedByteArray(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParserArray(v1.UntypedVersionedMessage.parseFrom)(bytes)
      data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
      valueClass <- versioningTable
        .deserializerFor(ProtoVersion(proto.version))((ByteString.copyFrom(bytes), data))
    } yield valueClass

  override def fromByteString(
      expectedProtocolVersion: ProtocolVersion
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion))(bytes)

  override def fromByteString(
      expectedProtocolVersion: ProtocolVersionValidation
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    for {
      valueClass <- fromTrustedByteString(bytes)
      _ <- validateDeserialization(
        expectedProtocolVersion,
        valueClass.representativeProtocolVersion.representative,
      )
    } yield valueClass

  override def fromTrustedByteString(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
      data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
      valueClass <- versioningTable
        .deserializerFor(ProtoVersion(proto.version))((bytes, data))
    } yield valueClass
}

private[version] trait VersioningCompanionContext2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializationDomain,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Context,
    Dependency,
] extends BaseVersioningCompanion[
      ValueClass,
      DeserializationDomain,
      DeserializedValueClass,
      Dependency,
    ] {

  /** Deserializes the given bytes and checks that the therein embedded proto version matches the
    * `expectedProtocolVersion`.
    *
    * Use this method whenever the origin of the given bytes cannot be trusted, and for example the
    * proto version message field may be set maliciously. This should be your default choice for
    * deserialization.
    *
    * Hint: If the `ValueClass` requires the synchronizer protocol version for its implementation, pass it
    * as part of the deserialization context and consider using one of the traits suffixed with
    * `ValidationCompanion` to avoid possibly confusing argument duplication of the synchronizer protocol version.
    *
    * @param expectedProtocolVersion the protocol version on which the synchronizer is running on
    * @param context               additional information which is required for the deserialization
    * @param bytes                 an untrusted byte string with an embedded proto version
    */
  def fromByteString(expectedProtocolVersion: ProtocolVersion)(
      context: Context
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass]

  /** Variation of `fromByteString` that takes a `ProtocolVersionValidation` instead of a `ProtocolVersion`.
    *
    * Use this method when sometimes no protocol version can be passed for the `expectedProtocolVersion`.
    * For these cases use `ProtocolVersionValidation.NoValidation`.
    */
  def fromByteString(
      expectedProtocolVersion: ProtocolVersionValidation
  )(
      context: Context
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass]

  /** Deserializes the given bytes without validation!
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the given bytes originate from a trusted
    * source.
    *
    * Hint: If the `ValueClass` requires the synchronizer protocol version for its implementation, pass it
    * as part of the deserialization context and consider using one of the traits suffixed with
    * `ValidationCompanion` to avoid possibly confusing argument duplication of the synchronizer protocol version.
    *
    * @param context additional information which required for the deserialization
    * @param bytes   a trusted byte string with an embedded proto version
    */
  def fromTrustedByteString(
      context: Context
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass]

  /** Deserializes the data from the given file without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the data originates from a trusted
    * source.
    */
  private[version] def readFromTrustedFile(context: Context)(
      inputFile: String
  ): Either[String, DeserializedValueClass] =
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromTrustedByteString(context)(bs).leftMap(_.toString)
    } yield value

  /** Deserializes the data from the given file without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the data originates from a trusted
    * source.
    */
  def tryReadFromTrustedFile(
      inputFile: String
  )(implicit ev: ProtocolVersionValidation =:= Context): DeserializedValueClass =
    readFromTrustedFile(ev.apply(ProtocolVersionValidation.NoValidation))(inputFile).valueOr(err =>
      throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
    )
}

trait VersioningCompanionContextMemoization2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Context,
    Dependency,
] extends VersioningCompanionContext2[
      ValueClass,
      (Context, OriginalByteString, DataByteString),
      DeserializedValueClass,
      Context,
      Dependency,
    ] {

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer = input => {
    val (ctx, original, data) = input
    ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _)(original))
  }

  override def fromByteString(expectedProtocolVersion: ProtocolVersion)(
      context: Context
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion))(context)(bytes)

  def fromByteString(expectedProtocolVersion: ProtocolVersion, bytes: OriginalByteString)(implicit
      ev: ProtocolVersion =:= Context
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion))(
      ev.apply(expectedProtocolVersion)
    )(bytes)

  override def fromByteString(
      expectedProtocolVersion: ProtocolVersionValidation
  )(
      context: Context
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] = for {
    valueClass <- fromTrustedByteString(context)(bytes)
    _ <- validateDeserialization(
      expectedProtocolVersion,
      valueClass.representativeProtocolVersion.representative,
    )
  } yield valueClass

  override def fromTrustedByteString(
      context: Context
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- versioningTable
      .deserializerFor(ProtoVersion(proto.version))((context, bytes, data))
  } yield valueClass
}

trait VersioningCompanionNoContextNoMemoization2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
] extends VersioningCompanionNoContext2[
      ValueClass,
      ByteString,
      DeserializedValueClass,
    ] {

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[DeserializedValueClass]
  ): Deserializer =
    (data: DataByteString) => ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto)

  /** Deserializes the given bytes without validation.
    *
    * '''Unsafe!''' Do NOT use this method unless you can justify that the given bytes originate from a trusted
    * source. For example, this should be the case for deserialization of data that originates a
    * database.
    *
    * @param bytes trusted bytes with an embedded proto version
    */
  def fromTrustedByteArray(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParserArray(v1.UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromTrustedProtoVersioned(VersionedMessage(proto))
    } yield valueClass

  /** '''Unsafe!''' No deserialization validation is performed. Use `fromByteString` instead. */
  private def fromTrustedProtoVersioned(
      proto: VersionedMessage[DeserializedValueClass]
  ): ParsingResult[DeserializedValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      versioningTable.deserializerFor(ProtoVersion(proto.version))
    }

  override def fromByteString(
      expectedProtocolVersion: ProtocolVersion
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion))(bytes)

  override def fromByteString(
      expectedProtocolVersion: ProtocolVersionValidation
  )(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    for {
      valueClass <- fromTrustedByteString(bytes)
      _ <- validateDeserialization(
        expectedProtocolVersion,
        valueClass.representativeProtocolVersion.representative,
      )
    } yield valueClass

  override def fromTrustedByteString(
      bytes: OriginalByteString
  ): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromTrustedProtoVersioned(VersionedMessage(proto))
    } yield valueClass

  /** Deserializes a message using a delimiter (the message length) from the given input stream.
    *
    * '''Unsafe!''' No deserialization validation is performed.
    *
    * Do NOT use this method unless you can justify that the given bytes originate from a trusted
    * source.
    *
    * This method works in conjunction with
    *  [[com.digitalasset.canton.version.HasProtocolVersionedWrapper.writeDelimitedTo]] which should have been used to
    *  serialize the message. It is useful for deserializing multiple messages from a single input stream through
    *  repeated invocations.
    *
    * Deserialization is only supported for [[com.digitalasset.canton.version.VersionedMessage]].
    *
    * @param input the source from which a message is deserialized
    * @return an Option that is None when there are no messages left anymore, otherwise it wraps an Either
    *         where left represents a deserialization error (exception) and right represents the successfully
    *         deserialized message
    */
  def parseDelimitedFromTrusted(
      input: InputStream
  ): Option[ParsingResult[DeserializedValueClass]] =
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

trait VersioningCompanionContextNoMemoization2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass <: HasRepresentativeProtocolVersion,
    Context,
] extends VersioningCompanionContext2[
      ValueClass,
      (Context, ByteString),
      DeserializedValueClass,
      Context,
      Unit,
    ] {

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => ParsingResult[DeserializedValueClass]
  ): Deserializer =
    input => {
      val (ctx, data) = input
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _))
    }

  /** '''Unsafe!''' No deserialization validation is performed. Use `fromByteString` instead.
    */
  private def fromTrustedProtoVersioned(
      context: Context
  )(proto: VersionedMessage[DeserializedValueClass]): ParsingResult[DeserializedValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      byteString =>
        versioningTable.deserializerFor(ProtoVersion(proto.version))((context, byteString))
    }

  override def fromByteString(expectedProtocolVersion: ProtocolVersion)(
      context: Context
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] =
    fromByteString(ProtocolVersionValidation(expectedProtocolVersion))(context)(bytes)

  override def fromByteString(expectedProtocolVersion: ProtocolVersionValidation)(
      context: Context
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = for {
    valueClass <- fromTrustedByteString(context)(bytes)
    _ <- validateDeserialization(
      expectedProtocolVersion,
      valueClass.representativeProtocolVersion.representative,
    )
  } yield valueClass

  /** When the Context is a [[com.digitalasset.canton.version.ProtocolVersionValidation]], this method allows to write
    * `fromByteString(pv, bs)` instead of `fromByteString(pv)(pv)(bs)`
    */
  def fromByteString(expectedProtocolVersion: ProtocolVersion, bytes: OriginalByteString)(implicit
      ev: ProtocolVersionValidation =:= Context
  ): ParsingResult[DeserializedValueClass] = fromByteString(expectedProtocolVersion)(
    ProtocolVersionValidation.PV(expectedProtocolVersion)
  )(bytes)

  override def fromTrustedByteString(
      context: Context
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromTrustedProtoVersioned(context)(VersionedMessage(proto))
  } yield valueClass

  /** Since dependency on the ProtocolVersionValidation is encoded in the context, one
    * still has to provide `ProtocolVersionValidation.NoValidation` even when calling
    * `fromTrustedByteString`, which is counterintuitive.
    * This method allows a simpler call if the Context is a [[com.digitalasset.canton.version.ProtocolVersionValidation]]
    */
  def fromTrustedByteString(
      bytes: OriginalByteString
  )(implicit ev: ProtocolVersionValidation =:= Context): ParsingResult[DeserializedValueClass] =
    fromTrustedByteString(ev.apply(ProtocolVersionValidation.NoValidation))(bytes)
}

/** For readability, replaces the deserialization methods for value classes that require
  * the protocol version for the deserialization validation to be passed in as part of
  * the deserialization context.
  *
  * Replaces `.fromByteString(protocolVersion)((context, protocolVersion))(bytes)` with
  * `.fromByteString(context, protocolVersion)(bytes)`.
  */
trait VersioningCompanionContextNoMemoizationPVValidation2[
    ValueClass <: HasRepresentativeProtocolVersion,
    RawContext,
] extends VersioningCompanionContextNoMemoization2[
      ValueClass,
      ValueClass,
      (RawContext, ProtocolVersion),
    ] {
  def fromByteString(context: RawContext, expectedProtocolVersion: ProtocolVersion)(
      bytes: OriginalByteString
  ): ParsingResult[ValueClass] =
    super.fromByteString(expectedProtocolVersion)((context, expectedProtocolVersion))(bytes)
}

/** Similar to [[VersioningCompanionContextNoMemoizationPVValidation2]] but the deserialization
  * context contains a Source or Target of [[com.digitalasset.canton.version.ProtocolVersion]] for validation.
  */
trait VersioningCompanionContextNoMemoizationTaggedPVValidation2[
    ValueClass <: HasRepresentativeProtocolVersion,
    T[X] <: ReassignmentTag[X],
    RawContext,
] extends VersioningCompanionContextNoMemoization2[
      ValueClass,
      ValueClass,
      (RawContext, T[ProtocolVersion]),
    ] {
  def fromByteString(context: RawContext, expectedProtocolVersion: T[ProtocolVersion])(
      bytes: OriginalByteString
  ): ParsingResult[ValueClass] =
    super.fromByteString(expectedProtocolVersion.unwrap)((context, expectedProtocolVersion))(bytes)
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
