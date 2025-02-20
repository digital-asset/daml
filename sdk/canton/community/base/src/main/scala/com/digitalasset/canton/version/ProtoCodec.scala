// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.{PositionedParameters, SetParameter}

import scala.reflect.ClassTag

/** See
  * [[com.digitalasset.canton.version.HasProtocolVersionedWrapper.representativeProtocolVersion]]
  * for more context
  */
sealed abstract case class RepresentativeProtocolVersion[ValueCompanion](
    private val v: ProtocolVersion
) extends PrettyPrinting {

  /** When using this method, keep in mind that for a given companion object `C` that implements
    * `HasProtocolVersionedWrapperCompanion` and for a protocol version `pv`, then
    * `C.protocolVersionRepresentativeFor(pv).representative` is different than `pv`. In particular,
    * do not use a representative for a given class to construct a representative for another class.
    */
  def representative: ProtocolVersion = v

  override protected def pretty: Pretty[this.type] = prettyOfParam(_.v)
}

object RepresentativeProtocolVersion {

  implicit val setParameterRepresentativeProtocolVersion
      : SetParameter[RepresentativeProtocolVersion[_]] =
    (rpv: RepresentativeProtocolVersion[_], pp: PositionedParameters) => pp >> rpv.v

  // As `ValueCompanion` is a phantom type on `RepresentativeProtocolVersion`,
  // we can have a single Ordering object for all of them here.
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  implicit def orderRepresentativeProtocolVersion[ValueClass]
      : Ordering[RepresentativeProtocolVersion[ValueClass]] =
    orderingRepresentativeProtocolVersionInternal
      .asInstanceOf[Ordering[RepresentativeProtocolVersion[ValueClass]]]

  private[this] val orderingRepresentativeProtocolVersionInternal
      : Ordering[RepresentativeProtocolVersion[Any]] =
    Ordering.by(_.representative)

}

/** Base class for (de)serializing from/to protobuf of ValueClass from a specific PV
  */
sealed trait ProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Dependency]
    extends PrettyPrinting {

  type Deserializer =
    (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  def fromInclusive: RepresentativeProtocolVersion[Comp]
  def deserializer: Deserializer
  def serializer: ValueClass => ByteString
  def dependencySerializer: Dependency => ByteString
  // Can't always rely on the subtype to differentiate between instances of ProtoCodec, because the type is erased
  // at compile time when it is a dependent type of ValueClass (e.g in HasProtocolVersionedWrapper).
  // Instead use this method to differentiate between versioned and un-versioned serialization
  def isVersioned: Boolean
  def isSupported: Boolean
}

/** Supported Proto version
  * @param fromInclusive
  *   The protocol version when this Proto version was introduced
  * @param deserializer
  *   Deserialization method
  * @param serializer
  *   Serialization method
  * @param dependencySerializer
  *   Serialization method for the dependency
  */
class VersionedProtoCodec[
    ValueClass,
    Context,
    DeserializedValueClass,
    Comp: ClassTag,
    Dependency,
] private[version] (
    val fromInclusive: RepresentativeProtocolVersion[Comp],
    val deserializer: (
        Context,
        OriginalByteString,
        DataByteString,
    ) => ParsingResult[DeserializedValueClass],
    val serializer: ValueClass => ByteString,
    val dependencySerializer: Dependency => ByteString,
) extends ProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Dependency] {
  override val isVersioned: Boolean = true
  override val isSupported: Boolean = true

  override protected def pretty: Pretty[this.type] =
    prettyOfClass(
      param("instance", _ => implicitly[ClassTag[Comp]].getClass.getSimpleName.singleQuoted),
      param("fromInclusive", _.fromInclusive),
    )
}

final case class UnsupportedProtoCodec[
    ValueClass: ClassTag,
    Context,
    DeserializedValueClass,
    Comp,
] private (
    fromInclusive: RepresentativeProtocolVersion[Comp]
) extends ProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Unit]
    with PrettyPrinting {
  override val isVersioned: Boolean = false
  override val isSupported: Boolean = false

  private def valueClassName: String = implicitly[ClassTag[ValueClass]].getClass.getSimpleName

  def deserializationError: ProtoDeserializationError = ProtoDeserializationError.OtherError(
    s"Cannot deserialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
  )
  override def deserializer: Deserializer = (_, _, _) => Left(deserializationError)
  override def serializer: ValueClass => ByteString = throw new UnsupportedOperationException(
    s"Cannot serialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
  )

  override def dependencySerializer: Unit => ByteString = throw new UnsupportedOperationException(
    s"Cannot serialize dependency of $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
  )

  override protected def pretty: Pretty[this.type] = prettyOfClass(
    unnamedParam(_.valueClassName.unquoted),
    param("fromInclusive", _.fromInclusive),
  )
}

// In the versioning framework, such calls are legitimate
@SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
object VersionedProtoCodec {
  def withDependency[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp: ClassTag,
      Dependency,
      ProtoClass <: scalapb.GeneratedMessage,
      Status <: ProtocolVersionAnnotation.Status,
  ](
      fromInclusive: ProtocolVersion.ProtocolVersionWithStatus[Status]
  )(
      protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & Status
  )(
      deserializer: scalapb.GeneratedMessageCompanion[
        ProtoClass
      ] => (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass],
      serializer: ValueClass => scalapb.GeneratedMessage,
      dependencySerializer: Dependency => scalapb.GeneratedMessage,
  ): VersionedProtoCodec[
    ValueClass,
    Context,
    DeserializedValueClass,
    Comp,
    Dependency,
  ] = new VersionedProtoCodec(
    new RepresentativeProtocolVersion[Comp](fromInclusive) {},
    deserializer(protoCompanion),
    serializer(_).toByteString,
    dependencySerializer(_).toByteString,
  )

  def apply[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp: ClassTag,
      ProtoClass <: scalapb.GeneratedMessage,
      Status <: ProtocolVersionAnnotation.Status,
  ](
      fromInclusive: ProtocolVersion.ProtocolVersionWithStatus[Status]
  )(
      protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & Status
  )(
      parser: scalapb.GeneratedMessageCompanion[ProtoClass] => (
          (
              Context,
              OriginalByteString,
              DataByteString,
          ) => ParsingResult[DeserializedValueClass],
      ),
      serializer: ValueClass => scalapb.GeneratedMessage,
  ): VersionedProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Unit] =
    raw(fromInclusive, parser(protoCompanion), serializer(_).toByteString)

  def apply[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp: ClassTag,
  ](
      fromInclusive: RepresentativeProtocolVersion[Comp],
      deserializer: (
          Context,
          OriginalByteString,
          DataByteString,
      ) => ParsingResult[DeserializedValueClass],
      serializer: ValueClass => ByteString,
  ) =
    new VersionedProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Unit](
      fromInclusive,
      deserializer,
      serializer,
      _ => ByteString.EMPTY,
    )

  def storage[
      ValueClass,
      Context,
      DeserializedValueClass,
      Comp: ClassTag,
      ProtoClass <: scalapb.GeneratedMessage,
  ](
      fromInclusive: ReleaseProtocolVersion,
      protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & StorageProtoVersion,
  )(
      parser: scalapb.GeneratedMessageCompanion[ProtoClass] => (
          (
              Context,
              OriginalByteString,
              DataByteString,
          ) => ParsingResult[DeserializedValueClass],
      ),
      serializer: ValueClass => scalapb.GeneratedMessage,
  ): VersionedProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Unit] =
    raw(fromInclusive.v, parser(protoCompanion), serializer(_).toByteString)

  @VisibleForTesting
  def raw[ValueClass, Context, DeserializedValueClass, Comp: ClassTag](
      fromInclusive: ProtocolVersion,
      deserializer: (
          Context,
          OriginalByteString,
          DataByteString,
      ) => ParsingResult[DeserializedValueClass],
      serializer: ValueClass => ByteString,
  ): VersionedProtoCodec[ValueClass, Context, DeserializedValueClass, Comp, Unit] =
    VersionedProtoCodec(
      new RepresentativeProtocolVersion[Comp](fromInclusive) {},
      deserializer,
      serializer,
    )

}

object UnsupportedProtoCodec {
  def apply[ValueClass: ClassTag, Context, DeserializedValueClass, Comp](
      fromInclusive: ProtocolVersion = ProtocolVersion.minimum
  ): UnsupportedProtoCodec[ValueClass, Context, DeserializedValueClass, Comp] =
    new UnsupportedProtoCodec(
      new RepresentativeProtocolVersion[Comp](fromInclusive) {}
    )
}
