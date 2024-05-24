// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{ProtoDeserializationError, checked}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import scala.collection.immutable

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  *
  * This wrapper is to be used if a single instance needs to be serialized to different proto versions.
  *
  * The underlying ProtoClass is [[com.digitalasset.canton.version.v1.UntypedVersionedMessage]]
  * but we often specify the typed alias [[com.digitalasset.canton.version.VersionedMessage]]
  * instead.
  */
trait HasVersionedWrapper[ValueClass] extends HasVersionedToByteString {
  self: ValueClass =>

  protected def companionObj: HasVersionedMessageCompanionCommon[ValueClass]

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    */
  def toProtoVersioned(version: ProtocolVersion): VersionedMessage[ValueClass] =
    companionObj.supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion) if version >= supportedVersion.fromInclusive =>
          VersionedMessage(supportedVersion.serializer(self), protoVersion.v)
      }
      .getOrElse(serializeToHighestVersion)

  private def serializeToHighestVersion: VersionedMessage[ValueClass] = {
    VersionedMessage(
      companionObj.supportedProtoVersions.higherConverter.serializer(self),
      companionObj.supportedProtoVersions.higherProtoVersion.v,
    )
  }

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  override def toByteString(version: ProtocolVersion): ByteString = toProtoVersioned(
    version
  ).toByteString

  /** Yields a byte array representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteArray(version: ProtocolVersion): Array[Byte] = toByteString(version).toByteArray

  /** Writes the byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance to a file. */
  def writeToFile(outputFile: String, version: ProtocolVersion = ProtocolVersion.latest): Unit = {
    val bytes = toByteString(version)
    BinaryFileUtil.writeByteStringToFile(outputFile, bytes)
  }
}

// Implements shared behavior of [[HasVersionedMessageCompanion]] and [[HasVersionedMessageWithContextCompanion]]
trait HasVersionedMessageCompanionCommon[ValueClass] {

  /** The name of the class as used for pretty-printing and error reporting */
  def name: String

  type Serializer = ValueClass => ByteString
  type Deserializer

  /** Proto versions that are supported by `fromProtoVersioned`, `fromByteString`,
    * `toProtoVersioned` and `toByteString`.
    * See the helpers `supportedProtoVersion` and `supportedProtoVersionMemoized`
    * below to define a `ProtoCodec`.
    */
  def supportedProtoVersions: SupportedProtoVersions

  case class ProtoCodec(
      fromInclusive: ProtocolVersion,
      deserializer: Deserializer,
      serializer: Serializer,
  )

  case class SupportedProtoVersions private (
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[ProtoVersion, ProtoCodec]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def converterFor(protocolVersion: ProtocolVersion): ProtoCodec =
      converters
        .collectFirst {
          case (_, converter) if protocolVersion >= converter.fromInclusive =>
            converter
        }
        .getOrElse(higherConverter)

    def deserializerFor(protoVersion: ProtoVersion): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)
  }

  object SupportedProtoVersions {
    def apply(
        head: (ProtoVersion, ProtoCodec),
        tail: (ProtoVersion, ProtoCodec)*
    ): SupportedProtoVersions = SupportedProtoVersions.fromNonEmpty(
      NonEmpty.mk(Seq, head, tail*)
    )

    /*
     Throws an error if a protocol version is used twice.
     This indicates an error in the converters list since one protocol version
     cannot correspond to two proto versions.
     */
    private def ensureNoDuplicates(converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec)]]): Unit =
      NonEmpty
        .from {
          converters.forgetNE
            .groupMap { case (_, codec) => codec.fromInclusive } { case (protoVersion, _) =>
              protoVersion
            }
            .filter { case (_, protoVersions) => protoVersions.lengthCompare(1) > 0 }
            .toList
        }
        .foreach { duplicates =>
          throw new IllegalArgumentException(
            s"Some protocol versions appear several times in `$name`: $duplicates "
          )
        }
        .discard

    private def fromNonEmpty(
        converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec)]]
    ): SupportedProtoVersions = {

      ensureNoDuplicates(converters)

      val sortedConverters = checked(
        NonEmptyUtil.fromUnsafe(
          immutable.SortedMap.from(converters)(implicitly[Ordering[ProtoVersion]].reverse)
        )
      )
      val (_, lowestProtocolVersion) = sortedConverters.last1

      require(
        lowestProtocolVersion.fromInclusive == ProtocolVersion.minimum,
        s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum}, found ${lowestProtocolVersion.fromInclusive}",
      )

      SupportedProtoVersions(sortedConverters)
    }
  }
}

/** Traits for the companion objects of classes that implement [[HasVersionedWrapper]].
  * Provide default methods.
  */
trait HasVersionedMessageCompanion[ValueClass]
    extends HasVersionedMessageCompanionCommon[ValueClass] {
  type Deserializer = ByteString => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[ValueClass]
  ): ByteString => ParsingResult[ValueClass] =
    ProtoConverter.protoParser(p.parseFrom)(_).flatMap(fromProto)

  def fromProtoVersioned(
      proto: VersionedMessage[ValueClass]
  ): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      data => supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))(data)
    }

  /** The embedded version is not validated */
  def fromTrustedByteString(bytes: ByteString): ParsingResult[ValueClass] =
    for { // no input validation for proto version
      proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromProtoVersioned(VersionedMessage(proto))
    } yield valueClass

  def fromTrustedByteArray(bytes: Array[Byte]): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParserArray(v1.UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def readFromTrustedFile(
      inputFile: String
  ): Either[String, ValueClass] = {
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromTrustedByteString(bs).leftMap(_.toString)
    } yield value
  }

  def tryReadFromTrustedFile(inputFile: String): ValueClass =
    readFromTrustedFile(inputFile).valueOr(err =>
      throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
    )

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[ValueClass] = GetResult { r =>
    fromTrustedByteArray(r.<<[Array[Byte]]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[ValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromTrustedByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }
}

trait HasVersionedMessageCompanionDbHelpers[ValueClass <: HasVersionedWrapper[ValueClass]] {
  def getVersionedSetParameter(protocolVersion: ProtocolVersion)(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[ValueClass] = { (value, pp) =>
    pp >> value.toByteArray(protocolVersion)
  }

  def getVersionedSetParameterO(protocolVersion: ProtocolVersion)(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[ValueClass]] =
    (valueO, pp) => pp >> valueO.map(_.toByteArray(protocolVersion))
}

/** Traits for the companion objects of classes that implement [[HasVersionedWrapper]].
  * They provide default methods.
  * Unlike [[HasVersionedMessageCompanion]] these traits allow to pass additional
  * context to the conversion methods.
  */
trait HasVersionedMessageWithContextCompanion[ValueClass, Ctx]
    extends HasVersionedMessageCompanionCommon[ValueClass] {
  type Deserializer = (Ctx, ByteString) => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Ctx, Proto) => ParsingResult[ValueClass]
  ): (Ctx, ByteString) => ParsingResult[ValueClass] =
    (ctx: Ctx, data: ByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _))

  def fromProtoVersioned(
      ctx: Ctx
  )(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      data => supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))(ctx, data)
    }

  /** The embedded version is not validated */
  def fromTrustedByteString(ctx: Ctx)(bytes: ByteString): ParsingResult[ValueClass] =
    for { // no input validation for proto version
      proto <- ProtoConverter.protoParser(v1.UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromProtoVersioned(ctx)(VersionedMessage(proto))
    } yield valueClass
}
