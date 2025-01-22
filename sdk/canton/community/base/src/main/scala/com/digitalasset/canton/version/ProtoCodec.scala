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

/** See [[com.digitalasset.canton.version.HasProtocolVersionedWrapper.representativeProtocolVersion]] for more context */
sealed abstract case class RepresentativeProtocolVersion[ValueCompanion](
    private val v: ProtocolVersion
) extends PrettyPrinting {

  /** When using this method, keep in mind that for a given companion object `C` that implements
    * `HasProtocolVersionedWrapperCompanion` and for a protocol version `pv`, then
    * `C.protocolVersionRepresentativeFor(pv).representative` is different than `pv`.
    * In particular, do not use a representative for a given class to construct a representative
    * for another class.
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
sealed trait ProtoCodec[ValueClass, DeserializationDomain, DeserializedValueClass, Comp]
    extends PrettyPrinting {
  type Deserializer = DeserializationDomain => ParsingResult[DeserializedValueClass]

  def fromInclusive: RepresentativeProtocolVersion[Comp]
  def deserializer: Deserializer
  def serializer: ValueClass => ByteString
  // Can't always rely on the subtype to differentiate between instances of ProtoCodec, because the type is erased
  // at compile time when it is a dependent type of ValueClass (e.g in HasProtocolVersionedWrapper).
  // Instead use this method to differentiate between versioned and un-versioned serialization
  def isVersioned: Boolean
  def isSupported: Boolean
}

/** Supported Proto version
  * @param fromInclusive The protocol version when this Proto version was introduced
  * @param deserializer Deserialization method
  * @param serializer Serialization method
  */
final case class VersionedProtoConverter[
    ValueClass,
    DeserializationDomain,
    DeserializedValueClass,
    Comp: ClassTag,
] private (
    fromInclusive: RepresentativeProtocolVersion[Comp],
    deserializer: DeserializationDomain => ParsingResult[DeserializedValueClass],
    serializer: ValueClass => ByteString,
) extends ProtoCodec[ValueClass, DeserializationDomain, DeserializedValueClass, Comp] {
  override val isVersioned: Boolean = true
  override val isSupported: Boolean = true

  override protected def pretty: Pretty[this.type] =
    prettyOfClass(
      param("instance", _ => implicitly[ClassTag[Comp]].getClass.getSimpleName.singleQuoted),
      param("fromInclusive", _.fromInclusive),
    )
}

/** Supported Proto version
  * @param fromInclusive The protocol version when this Proto version was introduced
  * @param deserializer Deserialization method
  * @param serializer Serialization method
  * @param dependencySerializer Serialization method for the dependency
  */
final case class VersionedProtoConverterWithDependency[
    ValueClass,
    DeserializationDomain,
    DeserializedValueClass,
    Comp: ClassTag,
    Dependency,
] private (
    fromInclusive: RepresentativeProtocolVersion[Comp],
    deserializer: DeserializationDomain => ParsingResult[DeserializedValueClass],
    serializer: ValueClass => ByteString,
    dependencySerializer: Dependency => ByteString,
) extends ProtoCodec[ValueClass, DeserializationDomain, DeserializedValueClass, Comp] {
  override val isVersioned: Boolean = true
  override val isSupported: Boolean = true

  override protected def pretty: Pretty[this.type] =
    prettyOfClass(
      param("instance", _ => implicitly[ClassTag[Comp]].getClass.getSimpleName.singleQuoted),
      param("fromInclusive", _.fromInclusive),
    )
}

object VersionedProtoConverter {
  def apply[
      ValueClass,
      DeserializationDomain,
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
          DeserializationDomain => ParsingResult[DeserializedValueClass]
      ),
      serializer: ValueClass => scalapb.GeneratedMessage,
  ): VersionedProtoConverter[ValueClass, DeserializationDomain, DeserializedValueClass, Comp] =
    raw(fromInclusive, parser(protoCompanion), serializer(_).toByteString)

  def storage[
      ValueClass,
      DeserializationDomain,
      DeserializedValueClass,
      Comp: ClassTag,
      ProtoClass <: scalapb.GeneratedMessage,
  ](
      fromInclusive: ReleaseProtocolVersion,
      protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & StorageProtoVersion,
  )(
      parser: scalapb.GeneratedMessageCompanion[ProtoClass] => (
          DeserializationDomain => ParsingResult[DeserializedValueClass]
      ),
      serializer: ValueClass => scalapb.GeneratedMessage,
  ): VersionedProtoConverter[ValueClass, DeserializationDomain, DeserializedValueClass, Comp] =
    raw(fromInclusive.v, parser(protoCompanion), serializer(_).toByteString)

  @VisibleForTesting
  def raw[ValueClass, DeserializationDomain, DeserializedValueClass, Comp: ClassTag](
      fromInclusive: ProtocolVersion,
      deserializer: DeserializationDomain => ParsingResult[DeserializedValueClass],
      serializer: ValueClass => ByteString,
  ): VersionedProtoConverter[ValueClass, DeserializationDomain, DeserializedValueClass, Comp] =
    VersionedProtoConverter(
      new RepresentativeProtocolVersion[Comp](fromInclusive) {},
      deserializer,
      serializer,
    )
}

final case class UnsupportedProtoCodec[
    ValueClass: ClassTag,
    DeserializationDomain,
    DeserializedValueClass,
    Comp,
] private (
    fromInclusive: RepresentativeProtocolVersion[Comp]
) extends ProtoCodec[ValueClass, DeserializationDomain, DeserializedValueClass, Comp]
    with PrettyPrinting {
  override val isVersioned: Boolean = false
  override val isSupported: Boolean = false

  private def valueClassName: String = implicitly[ClassTag[ValueClass]].getClass.getSimpleName

  def deserializationError: ProtoDeserializationError = ProtoDeserializationError.OtherError(
    s"Cannot deserialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
  )
  override def deserializer: Deserializer = _ => Left(deserializationError)
  override def serializer: ValueClass => ByteString = throw new UnsupportedOperationException(
    s"Cannot serialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
  )

  override protected def pretty: Pretty[this.type] = prettyOfClass(
    unnamedParam(_.valueClassName.unquoted),
    param("fromInclusive", _.fromInclusive),
  )
}

object VersionedProtoConverterWithDependency {
  def apply[
      ValueClass,
      DeserializationDomain,
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
      ] => DeserializationDomain => ParsingResult[DeserializedValueClass],
      serializer: ValueClass => scalapb.GeneratedMessage,
      dependencySerializer: Dependency => scalapb.GeneratedMessage,
  ): VersionedProtoConverterWithDependency[
    ValueClass,
    DeserializationDomain,
    DeserializedValueClass,
    Comp,
    Dependency,
  ] =
    raw[
      ValueClass,
      DeserializationDomain,
      DeserializedValueClass,
      Comp,
      Dependency,
      ProtoClass,
      Status,
    ](
      fromInclusive,
      deserializer(protoCompanion),
      serializer(_).toByteString,
      dependencySerializer(_).toByteString,
    )

  def raw[
      ValueClass,
      DeserializationDomain,
      DeserializedValueClass,
      Comp: ClassTag,
      Dependency,
      ProtoClass <: scalapb.GeneratedMessage,
      Status <: ProtocolVersionAnnotation.Status,
  ](
      fromInclusive: ProtocolVersion,
      deserializer: DeserializationDomain => ParsingResult[DeserializedValueClass],
      serializer: ValueClass => ByteString,
      dependencySerializer: Dependency => ByteString,
  ): VersionedProtoConverterWithDependency[
    ValueClass,
    DeserializationDomain,
    DeserializedValueClass,
    Comp,
    Dependency,
  ] = VersionedProtoConverterWithDependency(
    new RepresentativeProtocolVersion[Comp](fromInclusive) {},
    deserializer,
    serializer,
    dependencySerializer,
  )
}

object UnsupportedProtoCodec {
  def apply[ValueClass: ClassTag, DeserializationDomain, DeserializedValueClass, Comp](
      fromInclusive: ProtocolVersion = ProtocolVersion.minimum
  ): UnsupportedProtoCodec[ValueClass, DeserializationDomain, DeserializedValueClass, Comp] =
    new UnsupportedProtoCodec(
      new RepresentativeProtocolVersion[Comp](fromInclusive) {}
    )
}
