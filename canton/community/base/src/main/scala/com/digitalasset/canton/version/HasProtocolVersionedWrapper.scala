// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion.ProtocolVersionWithStatus
import com.digitalasset.canton.{DiscardOps, ProtoDeserializationError, checked}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.{ByteString, InvalidProtocolBufferException}
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import java.io.{InputStream, OutputStream}
import scala.collection.immutable
import scala.math.Ordered.orderingToOrdered
import scala.util.Try
import scala.util.control.NonFatal

trait HasRepresentativeProtocolVersion {
  // Needs to be a `val` because we need a stable ref.
  // @transient because there is no point in serializing it.
  // Actual implementations should make this a `lazy val` so that it gets re-initialized after deserialization
  @transient protected val companionObj: AnyRef

  /** We have a correspondence {Proto version} <-> {[protocol version]}: each proto version
    * correspond to a list of consecutive protocol versions. The representative is one instance
    * of this list, usually the smallest value. In other words, the Proto versions induce an
    * equivalence relation on the list of protocol version, thus use of `representative`.
    *
    * The method `protocolVersionRepresentativeFor` below
    * allows to query the representative for an equivalence class.
    */
  def representativeProtocolVersion: RepresentativeProtocolVersion[companionObj.type]
}

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

  override def pretty: Pretty[this.type] = prettyOfParam(_.v)
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

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  *
  * This wrapper is to be used when every instance can be tied to a single protocol version.
  * Consequently, some attributes of the class may depend on the protocol version (e.g., the signature).
  * The protocol version is then bundled with the instance and does not need to
  * be passed to the toProtoVersioned, toByteString and getCryptographicEvidence
  * methods.
  *
  * The underlying ProtoClass is [[com.digitalasset.canton.version.UntypedVersionedMessage]]
  * but we often specify the typed alias [[com.digitalasset.canton.version.VersionedMessage]]
  * instead.
  */
trait HasProtocolVersionedWrapper[ValueClass <: HasRepresentativeProtocolVersion]
    extends HasRepresentativeProtocolVersion {
  self: ValueClass =>

  @transient
  override protected val companionObj: HasProtocolVersionedWrapperCompanion[ValueClass, _]

  def isEquivalentTo(protocolVersion: ProtocolVersion): Boolean =
    companionObj.protocolVersionRepresentativeFor(protocolVersion) == representativeProtocolVersion

  private def serializeToHighestVersion: VersionedMessage[ValueClass] = {
    VersionedMessage(
      companionObj.supportedProtoVersions.higherConverter.serializer(self),
      companionObj.supportedProtoVersions.higherProtoVersion.v,
    )
  }

  /** Will check that default value rules defined in `companionObj.defaultValues` hold.
    */
  def validateInstance(): Either[String, Unit] =
    companionObj.invariants.traverse_(_.validateInstance(this, representativeProtocolVersion))

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    *
    * Be aware that if calling on a class that defines a LegacyProtoConverter, this method will still
    * return a VersionedMessage. If the current protocol version maps to the
    * legacy converter, deserialization will then fail (as it will try to deserialize to the raw protobuf instead of the
    * VersionedMessage wrapper this was serialized to.
    * Prefer using toByteString which handles this use case correctly.
    */
  def toProtoVersioned: VersionedMessage[ValueClass] =
    companionObj.supportedProtoVersions.converters
      .collectFirst {
        case (protoVersion, supportedVersion)
            if representativeProtocolVersion >= supportedVersion.fromInclusive =>
          VersionedMessage(supportedVersion.serializer(self), protoVersion.v)
      }
      .getOrElse(serializeToHighestVersion)

  /** Yields the Proto version that this class will be serialized to
    */
  def protoVersion: ProtoVersion =
    companionObj.protoVersionFor(representativeProtocolVersion)

  /** Yields a byte string representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteString: ByteString = companionObj.supportedProtoVersions.converters
    .collectFirst {
      case (protoVersion, supportedVersion)
          if representativeProtocolVersion >= supportedVersion.fromInclusive =>
        supportedVersion match {
          case versioned if versioned.isVersioned =>
            VersionedMessage(supportedVersion.serializer(self), protoVersion.v).toByteString
          case legacy =>
            legacy.serializer(self)
        }
    }
    .getOrElse(serializeToHighestVersion.toByteString)

  /** Serializes this instance to a message together with a delimiter (the message length) to the given output stream.
    *
    * This method works in conjunction with
    *  [[com.digitalasset.canton.version.HasProtocolVersionedCompanion2.parseDelimitedFrom]] which deserializes the
    *  message again. It is useful for serializing multiple messages to a single output stream through multiple
    *  invocations.
    *
    * Serialization is only supported for
    *  [[com.digitalasset.canton.version.HasSupportedProtoVersions.VersionedProtoConverter]], an error message is
    *  returned otherwise.
    *
    * @param output the sink to which this message is serialized to
    * @return an Either where left represents an error message, and right represents a successful message
    *         serialization
    */
  def writeDelimitedTo(output: OutputStream): Either[String, Unit] = {
    val converter: Either[String, VersionedMessage[ValueClass]] =
      companionObj.supportedProtoVersions.converters
        .collectFirst {
          case (protoVersion, supportedVersion)
              if representativeProtocolVersion >= supportedVersion.fromInclusive =>
            supportedVersion match {
              case companionObj.VersionedProtoConverter(_, _, serializer) =>
                Right(VersionedMessage(serializer(self), protoVersion.v))
              case other =>
                Left(
                  s"Cannot call writeDelimitedTo on ${companionObj.name} in protocol version equivalent to ${other.fromInclusive.representative}"
                )
            }
        }
        .getOrElse(Right(serializeToHighestVersion))

    converter.flatMap(actual =>
      Try(actual.writeDelimitedTo(output)).toEither.leftMap(e =>
        s"Cannot serialize ${companionObj.name} into the given output stream due to: ${e.getMessage}"
      )
    )
  }

  /** Yields a byte array representation of the corresponding `UntypedVersionedMessage` wrapper of this instance.
    */
  def toByteArray: Array[Byte] = toByteString.toByteArray

  def writeToFile(outputFile: String): Unit =
    BinaryFileUtil.writeByteStringToFile(outputFile, toByteString)

  /** Casts this instance's representative protocol version to one for the target type.
    * This only succeeds if the versioning schemes are the same.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def castRepresentativeProtocolVersion[T <: HasSupportedProtoVersions[_]](
      target: T
  ): Either[String, RepresentativeProtocolVersion[T]] = {
    val sourceTable = companionObj.supportedProtoVersions.table
    val targetTable = target.supportedProtoVersions.table

    Either.cond(
      sourceTable == targetTable,
      representativeProtocolVersion.asInstanceOf[RepresentativeProtocolVersion[T]],
      "Source and target versioning schemes should be the same",
    )
  }
}

/** This trait has the logic to store proto (de)serializers and retrieve them by protocol version.
  * @tparam ValueClass
  */
trait HasSupportedProtoVersions[ValueClass] {

  /** The name of the class as used for pretty-printing and error reporting */
  def name: String

  // Deserializer: (Proto => ValueClass)
  type Deserializer
  // Serializer: (ValueClass => Proto)
  type Serializer = ValueClass => ByteString

  private type ThisRepresentativeProtocolVersion = RepresentativeProtocolVersion[this.type]

  trait Invariant {
    def validateInstance(
        v: ValueClass,
        rpv: ThisRepresentativeProtocolVersion,
    ): Either[String, Unit]
  }

  private[version] sealed trait InvariantImpl[T] extends Invariant with Product with Serializable {
    def attribute: ValueClass => T
    def validate(v: T, pv: ProtocolVersion): Either[String, Unit]
    def validate(v: T, rpv: ThisRepresentativeProtocolVersion): Either[String, Unit]
    def validateInstance(
        v: ValueClass,
        rpv: ThisRepresentativeProtocolVersion,
    ): Either[String, Unit] =
      validate(attribute(v), rpv)
  }

  /*
    This trait encodes a default value starting (or ending) at a specific protocol version.
   */
  private[version] sealed trait DefaultValue[T] extends InvariantImpl[T] {

    def defaultValue: T

    /** Returns `v` or the default value, depending on the `protocolVersion`.
      */
    def orValue(v: T, protocolVersion: ProtocolVersion): T

    /** Returns `v` or the default value, depending on the `protocolVersion`.
      */
    def orValue(v: T, protocolVersion: ThisRepresentativeProtocolVersion): T

    override def validate(v: T, rpv: ThisRepresentativeProtocolVersion): Either[String, Unit] =
      validate(v, rpv.representative)
  }

  case class DefaultValueFromInclusive[T](
      attribute: ValueClass => T,
      attributeName: String,
      startInclusive: ThisRepresentativeProtocolVersion,
      defaultValue: T,
  ) extends DefaultValue[T] {
    def orValue(v: T, protocolVersion: ProtocolVersion): T =
      if (protocolVersion >= startInclusive.representative) defaultValue else v

    def orValue(v: T, protocolVersion: ThisRepresentativeProtocolVersion): T =
      if (protocolVersion >= startInclusive) defaultValue else v

    override def validate(
        v: T,
        pv: ProtocolVersion,
    ): Either[String, Unit] = {
      val shouldHaveDefaultValue = pv >= startInclusive.representative

      Either.cond(
        !shouldHaveDefaultValue || v == defaultValue,
        (),
        s"expected default value for $attributeName in $name but found $v",
      )
    }
  }

  case class DefaultValueUntilExclusive[T](
      attribute: ValueClass => T,
      attributeName: String,
      untilExclusive: ThisRepresentativeProtocolVersion,
      defaultValue: T,
  ) extends DefaultValue[T] {
    def orValue(v: T, protocolVersion: ProtocolVersion): T =
      if (protocolVersion < untilExclusive.representative) defaultValue else v

    def orValue(v: T, protocolVersion: ThisRepresentativeProtocolVersion): T =
      if (protocolVersion < untilExclusive) defaultValue else v

    override def validate(
        v: T,
        pv: ProtocolVersion,
    ): Either[String, Unit] = {
      val shouldHaveDefaultValue = pv < untilExclusive.representative

      Either.cond(
        !shouldHaveDefaultValue || v == defaultValue,
        (),
        s"expected default value for $attributeName in $name but found $v",
      )
    }
  }

  case class EmptyOptionExactlyUntilExclusive[T](
      attribute: ValueClass => Option[T],
      attributeName: String,
      untilExclusive: ThisRepresentativeProtocolVersion,
  ) extends DefaultValue[Option[T]] {
    val defaultValue: Option[T] = None

    def orValue(v: Option[T], protocolVersion: ProtocolVersion): Option[T] =
      if (protocolVersion < untilExclusive.representative) defaultValue else v

    def orValue(v: Option[T], protocolVersion: ThisRepresentativeProtocolVersion): Option[T] =
      if (protocolVersion < untilExclusive) defaultValue else v

    override def validate(
        v: Option[T],
        pv: ProtocolVersion,
    ): Either[String, Unit] =
      Either.cond(
        v.isEmpty == pv < untilExclusive.representative,
        (),
        s"expecting None if and only if $pv < ${untilExclusive.representative}; found: $v",
      )
  }

  def invariants: Seq[Invariant] = Nil

  def protocolVersionRepresentativeFor(
      protocolVersion: ProtocolVersion
  ): RepresentativeProtocolVersion[this.type] =
    supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)

  def protocolVersionRepresentativeFor(
      protoVersion: ProtoVersion
  ): RepresentativeProtocolVersion[this.type] =
    supportedProtoVersions.protocolVersionRepresentativeFor(protoVersion)

  /** Return the Proto version corresponding to the representative protocol version
    */
  def protoVersionFor(
      protocolVersion: RepresentativeProtocolVersion[this.type]
  ): ProtoVersion = supportedProtoVersions.protoVersionFor(protocolVersion)

  /** Return the Proto version corresponding to the protocol version
    */
  def protoVersionFor(protocolVersion: ProtocolVersion): ProtoVersion =
    supportedProtoVersions.protoVersionFor(protocolVersionRepresentativeFor(protocolVersion))

  /** Base class for (de)serializating from/to protobuf of ValueClass from a specific PV
    */
  sealed trait ProtoCodec {
    def fromInclusive: RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type]
    def deserializer: Deserializer
    def serializer: Serializer
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
  protected[this] case class VersionedProtoConverter private (
      fromInclusive: RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type],
      deserializer: Deserializer,
      serializer: Serializer,
  ) extends ProtoCodec
      with PrettyPrinting {
    override val isVersioned: Boolean = true
    override val isSupported: Boolean = true

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        unnamedParam(_ => HasSupportedProtoVersions.this.getClass.getSimpleName.unquoted),
        param("fromInclusive", _.fromInclusive),
      )
  }

  object VersionedProtoConverter {
    def apply[ProtoClass <: scalapb.GeneratedMessage, Status <: ProtocolVersion.Status](
        fromInclusive: ProtocolVersion.ProtocolVersionWithStatus[Status]
    )(
        protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & Status
    )(
        parser: scalapb.GeneratedMessageCompanion[ProtoClass] => Deserializer,
        serializer: Serializer,
    ): VersionedProtoConverter =
      raw(fromInclusive, parser(protoCompanion), serializer)

    def storage[ProtoClass <: scalapb.GeneratedMessage](
        fromInclusive: ReleaseProtocolVersion,
        protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & StorageProtoVersion,
    )(
        parser: scalapb.GeneratedMessageCompanion[ProtoClass] => Deserializer,
        serializer: Serializer,
    ): VersionedProtoConverter = raw(fromInclusive.v, parser(protoCompanion), serializer)

    @VisibleForTesting
    def raw(
        fromInclusive: ProtocolVersion,
        deserializer: Deserializer,
        serializer: Serializer,
    ): VersionedProtoConverter = VersionedProtoConverter(
      new RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type](fromInclusive) {},
      deserializer,
      serializer,
    )
  }

  /** Used to (de)serialize classes which for legacy reasons where not wrapped in VersionedMessage
    * Chances are this is NOT the class you want to use, use VersionedProtoConverter instead when adding serialization
    * to a new class
    */
  protected[this] case class LegacyProtoConverter private (
      fromInclusive: RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type],
      deserializer: Deserializer,
      serializer: Serializer,
  ) extends ProtoCodec
      with PrettyPrinting {
    override val isVersioned: Boolean = false
    override val isSupported: Boolean = true

    override def pretty: Pretty[this.type] = prettyOfClass(
      unnamedParam(_ => HasSupportedProtoVersions.this.getClass.getSimpleName.unquoted),
      param("fromInclusive", _.fromInclusive),
    )
  }

  object LegacyProtoConverter {
    def apply[ProtoClass <: scalapb.GeneratedMessage](
        fromInclusive: ProtocolVersion.ProtocolVersionWithStatus[ProtocolVersion.Stable]
    )(
        // A legacy converter should not be used for an unstable Protobuf message.
        protoCompanion: scalapb.GeneratedMessageCompanion[ProtoClass] & ProtocolVersion.Stable
    )(
        parser: scalapb.GeneratedMessageCompanion[ProtoClass] => Deserializer,
        serializer: Serializer,
    ): LegacyProtoConverter =
      LegacyProtoConverter.raw(fromInclusive, parser(protoCompanion), serializer)

    @VisibleForTesting
    def raw(
        fromInclusive: ProtocolVersionWithStatus[ProtocolVersion.Stable],
        deserializer: Deserializer,
        serializer: Serializer,
    ): LegacyProtoConverter = LegacyProtoConverter(
      new RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type](fromInclusive) {},
      deserializer,
      serializer,
    )
  }

  protected def deserializationErrorK(error: ProtoDeserializationError): Deserializer

  protected[this] case class UnsupportedProtoCodec(
      fromInclusive: RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type]
  ) extends ProtoCodec
      with PrettyPrinting {
    override val isVersioned: Boolean = false
    override val isSupported: Boolean = false

    private def valueClassName: String = HasSupportedProtoVersions.this.getClass.getSimpleName

    def deserializationError: ProtoDeserializationError = ProtoDeserializationError.OtherError(
      s"Cannot deserialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
    )
    override def deserializer: Deserializer = deserializationErrorK(deserializationError)
    override def serializer: Serializer = throw new UnsupportedOperationException(
      s"Cannot serialize $valueClassName in protocol version equivalent to ${fromInclusive.representative}"
    )
    override def pretty: Pretty[this.type] = prettyOfClass(
      unnamedParam(_.valueClassName.unquoted),
      param("fromInclusive", _.fromInclusive),
    )
  }

  object UnsupportedProtoCodec {
    def apply(fromInclusive: ProtocolVersion): UnsupportedProtoCodec =
      new UnsupportedProtoCodec(
        new RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type](fromInclusive) {}
      )
  }

  case class SupportedProtoVersions private (
      // Sorted with descending order
      converters: NonEmpty[immutable.SortedMap[ProtoVersion, ProtoCodec]]
  ) {
    val (higherProtoVersion, higherConverter) = converters.head1

    def converterFor(protocolVersion: ProtocolVersion): ProtoCodec = {
      converters
        .collectFirst {
          case (_, converter) if protocolVersion >= converter.fromInclusive.representative =>
            converter
        }
        .getOrElse(higherConverter)
    }

    def deserializerFor(protoVersion: ProtoVersion): Deserializer =
      converters.get(protoVersion).map(_.deserializer).getOrElse(higherConverter.deserializer)

    def protoVersionFor(
        protocolVersion: RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type]
    ): ProtoVersion = converters
      .collectFirst {
        case (protoVersion, converter) if protocolVersion >= converter.fromInclusive =>
          protoVersion
      }
      .getOrElse(higherProtoVersion)

    def protocolVersionRepresentativeFor(
        protoVersion: ProtoVersion
    ): RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type] =
      table.getOrElse(protoVersion, higherConverter.fromInclusive)

    def protocolVersionRepresentativeFor(
        protocolVersion: ProtocolVersion
    ): RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type] = converterFor(
      protocolVersion
    ).fromInclusive

    lazy val table
        : Map[ProtoVersion, RepresentativeProtocolVersion[HasSupportedProtoVersions.this.type]] =
      converters.forgetNE.fmap(_.fromInclusive)
  }

  object SupportedProtoVersions {
    def apply(
        head: (ProtoVersion, ProtoCodec),
        tail: (ProtoVersion, ProtoCodec)*
    ): SupportedProtoVersions = SupportedProtoVersions.fromNonEmpty(
      NonEmpty.mk(Seq, head, tail: _*)
    )

    /*
     Throws an error if a protocol version or a protobuf version is used twice.
     This indicates an error in the converters list:
     - Each protobuf version should appear only once.
     - Each protobuf version should use a different minimum protocol version.
     */
    private def ensureNoDuplicates(converters: NonEmpty[Seq[(ProtoVersion, ProtoCodec)]]): Unit = {

      val versions: Seq[(ProtoVersion, ProtocolVersion)] = converters.forgetNE.map {
        case (protoVersion, codec) =>
          (protoVersion, codec.fromInclusive.representative)
      }

      def getDuplicates[T](
          proj: ((ProtoVersion, ProtocolVersion)) => T
      ): Option[NonEmpty[List[T]]] = {
        val duplicates = versions
          .groupBy(proj)
          .toList
          .collect {
            case (_, versions) if versions.lengthCompare(1) > 0 =>
              versions.map(proj)
          }
          .flatten

        NonEmpty.from(duplicates)
      }

      val duplicatedProtoVersion = getDuplicates(_._1)
      val duplicatedProtocolVersion = getDuplicates(_._2)

      duplicatedProtoVersion.foreach { duplicates =>
        throw new IllegalArgumentException(
          s"Some protobuf versions appear several times in `$name`: $duplicates"
        )
      }.discard

      duplicatedProtocolVersion.foreach { duplicates =>
        throw new IllegalArgumentException(
          s"Some protocol versions appear several times in `$name`: $duplicates"
        )
      }.discard
    }

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

      // If you are hitting this require failing when your message doesn't exist in PV.minimum,
      // remember to specify that explicitly by adding to the SupportedProtoVersions:
      // ProtoVersion(-1) -> UnsupportedProtoCodec(ProtocolVersion.minimum),
      require(
        lowestProtocolVersion.fromInclusive.representative == ProtocolVersion.minimum,
        s"ProtocolVersion corresponding to lowest proto version should be ${ProtocolVersion.minimum}, found $lowestProtocolVersion",
      )

      SupportedProtoVersions(sortedConverters)
    }
  }

  /** Proto versions that are supported by `fromProtoVersioned` and `fromByteString`
    * See the helper `supportedProtoVersion` below to define a `Parser`.
    */
  def supportedProtoVersions: SupportedProtoVersions
}

trait HasProtocolVersionedWrapperCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
] extends HasSupportedProtoVersions[ValueClass]
    with Serializable {

  /** The name of the class as used for pretty-printing and error reporting */
  def name: String

  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  protected def deserializeForVersion(
      rpv: RepresentativeProtocolVersion[this.type],
      deserializeLegacyProto: Deserializer => ParsingResult[DeserializedValueClass],
      deserializeVersionedProto: => ParsingResult[DeserializedValueClass],
  ): ParsingResult[DeserializedValueClass] = {
    val converter =
      supportedProtoVersions.converterFor(rpv.representative)

    converter match {
      case LegacyProtoConverter(_, deserializer, _) => deserializeLegacyProto(deserializer)
      case _: VersionedProtoConverter => deserializeVersionedProto
      case unsupported: UnsupportedProtoCodec =>
        Left(unsupported.deserializationError)
    }
  }
}

trait HasProtocolVersionedWrapperWithoutContextCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, DeserializedValueClass] {
  def fromByteString(bytes: OriginalByteString): ParsingResult[DeserializedValueClass]
}

/** Trait for companion objects of serializable classes with memoization.
  * Use this class if deserialization produces a different type than where serialization starts.
  * For example, if a container can serialize its elements, but the container's deserializer
  * does not deserialize the elements and instead leaves them as Bytestring.
  *
  * Use [[HasMemoizedProtocolVersionedWrapperCompanion]] if the type distinction between serialization and deseserialization is not needed.
  */
trait HasMemoizedProtocolVersionedWrapperCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
] extends HasProtocolVersionedWrapperWithoutContextCompanion[ValueClass, DeserializedValueClass] {
  // Deserializer: (Proto => DeserializedValueClass)
  override type Deserializer =
    (OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    (original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(_)(original))

  def fromByteArray(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] = fromByteString(
    ByteString.copyFrom(bytes)
  )

  override def fromByteString(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
      data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
      valueClass <- supportedProtoVersions
        .deserializerFor(ProtoVersion(proto.version))(bytes, data)
    } yield valueClass

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the version to use for the deserialization.
    * @param protoVersion Proto version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protoVersion: ProtoVersion
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protoVersion),
      _(bytes, bytes),
      fromByteString(bytes),
    )
  }

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): (OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass] =
    (_, _) => Left(error)
}

/** Trait for companion objects of serializable classes with memoization and a (de)serialization context.
  * Use this class if deserialization produces a different type than where serialization starts.
  * For example, if a container can serialize its elements, but the container's deserializer
  * does not deserialize the elements and instead leaves them as Bytestring.
  *
  * Use [[HasMemoizedProtocolVersionedWithContextCompanion]] if the type distinction between serialization and deseserialization is not needed.
  */
trait HasMemoizedProtocolVersionedWithContextCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
    Context,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, DeserializedValueClass] {
  override type Deserializer =
    (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass]

  protected def supportedProtoVersionMemoized[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => (OriginalByteString => ParsingResult[DeserializedValueClass])
  ): Deserializer =
    (ctx: Context, original: OriginalByteString, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _)(original))

  def fromByteString(
      context: Context
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    data <- proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data"))
    valueClass <- supportedProtoVersions
      .deserializerFor(ProtoVersion(proto.version))(context, bytes, data)
  } yield valueClass

  def fromByteArray(context: Context)(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] =
    fromByteString(context)(ByteString.copyFrom(bytes))

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): (Context, OriginalByteString, DataByteString) => ParsingResult[DeserializedValueClass] =
    (_, _, _) => Left(error)
}

/** Trait for companion objects of serializable classes without memoization.
  * Use this class if deserialization produces a different type than where serialization starts.
  * For example, if a container can serialize its elements, but the container's deserializer
  * does not deserialize the elements and instead leaves them as Bytestring.
  *
  * Use [[HasProtocolVersionedCompanion]] if the type distinction between serialization and deseserialization is not needed.
  */
trait HasProtocolVersionedCompanion2[
    ValueClass <: HasRepresentativeProtocolVersion,
    DeserializedValueClass,
] extends HasProtocolVersionedWrapperWithoutContextCompanion[ValueClass, DeserializedValueClass] {
  override type Deserializer = DataByteString => ParsingResult[DeserializedValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: Proto => ParsingResult[DeserializedValueClass]
  ): Deserializer =
    (data: DataByteString) => ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto)

  def fromByteArray(bytes: Array[Byte]): ParsingResult[DeserializedValueClass] = for {
    proto <- ProtoConverter.protoParserArray(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(VersionedMessage(proto))
  } yield valueClass

  def fromProtoVersioned(
      proto: VersionedMessage[DeserializedValueClass]
  ): ParsingResult[DeserializedValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))
    }

  override def fromByteString(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] =
    for {
      proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
      valueClass <- fromProtoVersioned(VersionedMessage(proto))
    } yield valueClass

  /** Deserializes a message using a delimiter (the message length) from the given input stream.
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
  def parseDelimitedFrom(input: InputStream): Option[ParsingResult[DeserializedValueClass]] = {
    try {
      UntypedVersionedMessage
        .parseDelimitedFrom(input)
        .map(VersionedMessage[DeserializedValueClass])
        .map(fromProtoVersioned)
    } catch {
      case protoBuffException: InvalidProtocolBufferException =>
        Some(Left(ProtoDeserializationError.BufferException(protoBuffException)))
      case NonFatal(e) =>
        Some(Left(ProtoDeserializationError.OtherError(e.getMessage)))
    }
  }

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the version to use for the deserialization.
    * @param protocolVersion protocol version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protocolVersion: ProtocolVersion
  )(bytes: OriginalByteString): ParsingResult[DeserializedValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protocolVersion),
      _(bytes),
      fromByteString(bytes),
    )
  }

  def readFromFile(
      inputFile: String
  ): Either[String, DeserializedValueClass] = {
    for {
      bs <- BinaryFileUtil.readByteStringFromFile(inputFile)
      value <- fromByteString(bs).leftMap(_.toString)
    } yield value
  }

  def tryReadFromFile(inputFile: String): DeserializedValueClass =
    readFromFile(inputFile).valueOr(err =>
      throw new IllegalArgumentException(s"Reading $name from file $inputFile failed: $err")
    )

  implicit def hasVersionedWrapperGetResult(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[DeserializedValueClass] = GetResult { r =>
    fromByteArray(r.<<[Array[Byte]]).valueOr(err =>
      throw new DbDeserializationException(s"Failed to deserialize $name: $err")
    )
  }

  implicit def hasVersionedWrapperGetResultO(implicit
      getResultByteArray: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[DeserializedValueClass]] = GetResult { r =>
    r.<<[Option[Array[Byte]]]
      .map(
        fromByteArray(_).valueOr(err =>
          throw new DbDeserializationException(s"Failed to deserialize $name: $err")
        )
      )
  }

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): DataByteString => ParsingResult[DeserializedValueClass] = _ => Left(error)
}

trait HasProtocolVersionedWithContextCompanion[
    ValueClass <: HasRepresentativeProtocolVersion,
    Context,
] extends HasProtocolVersionedWrapperCompanion[ValueClass, ValueClass] {
  override type Deserializer = (Context, DataByteString) => ParsingResult[ValueClass]

  protected def supportedProtoVersion[Proto <: scalapb.GeneratedMessage](
      p: scalapb.GeneratedMessageCompanion[Proto]
  )(
      fromProto: (Context, Proto) => ParsingResult[ValueClass]
  ): Deserializer =
    (ctx: Context, data: DataByteString) =>
      ProtoConverter.protoParser(p.parseFrom)(data).flatMap(fromProto(ctx, _))

  def fromProtoVersioned(
      context: Context
  )(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass] =
    proto.wrapper.data.toRight(ProtoDeserializationError.FieldNotSet(s"$name: data")).flatMap {
      supportedProtoVersions.deserializerFor(ProtoVersion(proto.version))(context, _)
    }

  def fromByteString(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = for {
    proto <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(bytes)
    valueClass <- fromProtoVersioned(context)(VersionedMessage(proto))
  } yield valueClass

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the Proto version to use for the deserialization.
    * @param protoVersion Proto version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protoVersion: ProtoVersion
  )(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protoVersion),
      _(context, bytes),
      fromByteString(context)(bytes),
    )
  }

  /** Use this method when deserializing bytes for classes that have a legacy proto converter to explicitly
    * set the protocol version to use for the deserialization.
    * @param protocolVersion protocol version of the bytes to be deserialized
    * @param bytes data
    */
  def fromByteString(
      protocolVersion: ProtocolVersion
  )(context: Context)(bytes: OriginalByteString): ParsingResult[ValueClass] = {
    deserializeForVersion(
      protocolVersionRepresentativeFor(protocolVersion),
      _(context, bytes),
      fromByteString(context)(bytes),
    )
  }

  override protected def deserializationErrorK(
      error: ProtoDeserializationError
  ): (Context, DataByteString) => ParsingResult[ValueClass] = (_, _) => Left(error)
}

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
