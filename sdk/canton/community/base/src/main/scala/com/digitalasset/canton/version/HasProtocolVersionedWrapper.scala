// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import cats.syntax.either.*
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.ByteString

import java.io.OutputStream
import scala.math.Ordered.orderingToOrdered
import scala.util.Try

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

/** Trait for classes that can be serialized by using ProtoBuf.
  * See "CONTRIBUTING.md" for our guidelines on serialization.
  *
  * This wrapper is to be used when every instance can be tied to a single protocol version.
  * Consequently, some attributes of the class may depend on the protocol version (e.g., the signature).
  * The protocol version is then bundled with the instance and does not need to
  * be passed to the toProtoVersioned, toByteString and getCryptographicEvidence
  * methods.
  *
  * The underlying ProtoClass is [[com.digitalasset.canton.version.v1.UntypedVersionedMessage]]
  * but we often specify the typed alias [[com.digitalasset.canton.version.VersionedMessage]]
  * instead.
  */
trait HasProtocolVersionedWrapper[ValueClass <: HasRepresentativeProtocolVersion]
    extends HasRepresentativeProtocolVersion
    with HasToByteString {
  self: ValueClass =>

  @transient
  override protected val companionObj: BaseVersioningCompanion[ValueClass, ?, ?, ?]

  def isEquivalentTo(protocolVersion: ProtocolVersion): Boolean =
    companionObj.protocolVersionRepresentativeFor(protocolVersion) == representativeProtocolVersion

  private def serializeToHighestVersion: VersionedMessage[ValueClass] =
    VersionedMessage(
      companionObj.versioningTable.higherConverter.serializer(self),
      companionObj.versioningTable.higherProtoVersion.v,
    )

  /** Will check that default value rules defined in `companionObj.defaultValues` hold.
    */
  def validateInstance(): Either[String, Unit] =
    companionObj.validateInstance(this, representativeProtocolVersion)

  /** Yields the proto representation of the class inside an `UntypedVersionedMessage` wrapper.
    *
    * Subclasses should make this method public by default, as this supports composing proto serializations.
    * Keep it protected, if there are good reasons for it
    * (e.g. [[com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence]]).
    */
  def toProtoVersioned: VersionedMessage[ValueClass] =
    companionObj.versioningTable.converters
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
  def toByteString: ByteString = companionObj.versioningTable.converters
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
    *  parseDelimitedFromTrusted which deserializes the
    *  message again. It is useful for serializing multiple messages to a single output stream through multiple
    *  invocations.
    *
    * Serialization is only supported for
    *  [[com.digitalasset.canton.version.VersionedProtoCodec]], an error message is
    *  returned otherwise.
    *
    * @param output the sink to which this message is serialized to
    * @return an Either where left represents an error message, and right represents a successful message
    *         serialization
    */
  def writeDelimitedTo(output: OutputStream): Either[String, Unit] = {
    val converter: Either[String, VersionedMessage[ValueClass]] =
      companionObj.versioningTable.converters
        .collectFirst {
          case (protoVersion, supportedVersion)
              if representativeProtocolVersion >= supportedVersion.fromInclusive =>
            Try(VersionedMessage(supportedVersion.serializer(self), protoVersion.v)).toEither
              .leftMap(_.getMessage)
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
  def castRepresentativeProtocolVersion[T <: BaseVersioningCompanion[?, ?, ?, ?]](
      target: T
  ): Either[String, RepresentativeProtocolVersion[T]] = {
    val sourceTable = companionObj.versioningTable.table
    val targetTable = target.versioningTable.table

    Either.cond(
      sourceTable == targetTable,
      representativeProtocolVersion.asInstanceOf[RepresentativeProtocolVersion[T]],
      "Source and target versioning schemes should be the same",
    )
  }
}
