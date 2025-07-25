// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import com.digitalasset.canton.ProtoDeserializationError.ValueDeserializationError
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.RootHashMessage.RootHashMessagePayloadCast
import com.digitalasset.canton.protocol.{RootHash, v30}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContext,
}
import com.google.protobuf.ByteString

/** One root hash message is sent for each participant involved in a mediator confirmation request
  * that requires root hash messages. The root hash message is delivered to the participant and the
  * mediator. The mediator checks that it receives the right root hash messages and that they all
  * contain the root hash that the mediator confirmation request message specifies. The mediator
  * also checks that all payloads have the same serialization and, if it can parse the mediator
  * confirmation request envelope, that the payload fits to the mediator confirmation request.
  */
final case class RootHashMessage[+Payload <: RootHashMessagePayload](
    rootHash: RootHash,
    override val synchronizerId: PhysicalSynchronizerId,
    viewType: ViewType,
    submissionTopologyTimestamp: CantonTimestamp,
    payload: Payload,
) extends UnsignedProtocolMessage
    with PrettyPrinting
    with HasProtocolVersionedWrapper[RootHashMessage[RootHashMessagePayload]] {

  override val representativeProtocolVersion: RepresentativeProtocolVersion[RootHashMessage.type] =
    RootHashMessage.protocolVersionRepresentativeFor(synchronizerId.protocolVersion)

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.RootHashMessage(toProtoV30)

  def toProtoV30: v30.RootHashMessage = v30.RootHashMessage(
    rootHash = rootHash.toProtoPrimitive,
    physicalSynchronizerId = synchronizerId.toProtoPrimitive,
    viewType = viewType.toProtoEnum,
    submissionTopologyTime = submissionTopologyTimestamp.toProtoPrimitive,
    payload = payload.getCryptographicEvidence,
  )

  override def pretty: Pretty[RootHashMessage.this.type] =
    prettyOfClass(
      param("root hash", _.rootHash),
      param("payload size", _.payload.getCryptographicEvidence.size()),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def map[Payload2 <: RootHashMessagePayload](f: Payload => Payload2): RootHashMessage[Payload2] = {
    val payload2 = f(payload)
    if (payload eq payload2) this.asInstanceOf[RootHashMessage[Payload2]]
    else this.copy(payload = payload2)
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], Payload2 <: RootHashMessagePayload](
      f: Payload => F[Payload2]
  )(implicit F: Functor[F]): F[RootHashMessage[Payload2]] =
    F.map(f(payload)) { payload2 =>
      if (payload eq payload2) this.asInstanceOf[RootHashMessage[Payload2]]
      else this.copy(payload = payload2)
    }

  def copy[Payload2 <: RootHashMessagePayload](
      rootHash: RootHash = rootHash,
      payload: Payload2 = payload,
      viewType: ViewType = viewType,
      submissionTopologyTime: CantonTimestamp = submissionTopologyTimestamp,
  ): RootHashMessage[Payload2] =
    RootHashMessage(
      rootHash,
      synchronizerId,
      viewType,
      submissionTopologyTime,
      payload,
    )

  @transient override protected lazy val companionObj: RootHashMessage.type = RootHashMessage
}

object RootHashMessage
    extends VersioningCompanionContext[RootHashMessage[
      RootHashMessagePayload
    ], ByteString => ParsingResult[RootHashMessagePayload]] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.RootHashMessage)(
      supportedProtoVersion(_)((deserializer, proto) => fromProtoV30(deserializer)(proto)),
      _.toProtoV30,
    )
  )

  def fromProtoV30[Payload <: RootHashMessagePayload](
      payloadDeserializer: ByteString => ParsingResult[Payload]
  )(
      rootHashMessageP: v30.RootHashMessage
  ): ParsingResult[RootHashMessage[Payload]] = {
    val v30.RootHashMessage(
      rootHashP,
      synchronizerIdP,
      viewTypeP,
      submissionTopologyTimeP,
      payloadP,
    ) =
      rootHashMessageP
    for {
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        synchronizerIdP,
        "physical_synchronizer_id",
      )
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      submissionTopologyTime <- CantonTimestamp.fromProtoPrimitive(submissionTopologyTimeP)
      payloadO <- payloadDeserializer(payloadP)
    } yield RootHashMessage(
      rootHash,
      synchronizerId,
      viewType,
      submissionTopologyTime,
      payloadO,
    )
  }

  implicit def rootHashMessageProtocolMessageContentCast[Payload <: RootHashMessagePayload](implicit
      cast: RootHashMessagePayloadCast[Payload]
  ): ProtocolMessageContentCast[RootHashMessage[Payload]] =
    ProtocolMessageContentCast.create[RootHashMessage[Payload]]("RootHashMessage") {
      case rhm: RootHashMessage[?] => rhm.traverse(cast.toKind)
      case _ => None
    }

  trait RootHashMessagePayloadCast[+Payload <: RootHashMessagePayload] {
    def toKind(payload: RootHashMessagePayload): Option[Payload]
  }

  def toKind[Payload <: RootHashMessagePayload](payload: RootHashMessagePayload)(implicit
      cast: RootHashMessagePayloadCast[Payload]
  ): Option[Payload] = cast.toKind(payload)

  def select[Payload <: RootHashMessagePayload](message: RootHashMessage[RootHashMessagePayload])(
      implicit cast: RootHashMessagePayloadCast[Payload]
  ): Option[RootHashMessage[Payload]] =
    message.traverse(toKind(_))

  override def name: String = "RootHashMessage"
}

/** Payloads of [[RootHashMessage]] */
trait RootHashMessagePayload extends PrettyPrinting with HasCryptographicEvidence

case object EmptyRootHashMessagePayload extends RootHashMessagePayload {
  override protected def pretty: Pretty[EmptyRootHashMessagePayload.type] =
    prettyOfString(_ => "\"\"")
  def fromByteString(
      bytes: ByteString
  ): ParsingResult[EmptyRootHashMessagePayload.type] =
    Either.cond(
      bytes.isEmpty,
      EmptyRootHashMessagePayload,
      ValueDeserializationError("payload", s"expected no payload, but found ${bytes.size} bytes"),
    )

  implicit val emptyRootHashMessagePayloadCast
      : RootHashMessagePayloadCast[EmptyRootHashMessagePayload.type] = {
    case payload: EmptyRootHashMessagePayload.type => Some(payload)
    case _ => None
  }

  override def getCryptographicEvidence: ByteString = ByteString.EMPTY
}

final case class SerializedRootHashMessagePayload(bytes: ByteString)
    extends RootHashMessagePayload {

  override protected def pretty: Pretty[SerializedRootHashMessagePayload] = prettyOfClass(
    param("payload size", _.bytes.size)
  )

  override def getCryptographicEvidence: ByteString = bytes
}

object SerializedRootHashMessagePayload {
  def fromByteString(
      bytes: ByteString
  ): ParsingResult[SerializedRootHashMessagePayload] =
    Right(
      if (bytes.isEmpty) SerializedRootHashMessagePayload.empty
      else SerializedRootHashMessagePayload(bytes)
    )

  val empty: SerializedRootHashMessagePayload = SerializedRootHashMessagePayload(ByteString.EMPTY)

  implicit val serializedRootHashMessagePayloadCast
      : RootHashMessagePayloadCast[SerializedRootHashMessagePayload] = {
    case serialized: SerializedRootHashMessagePayload => Some(serialized)
    case _ => None
  }
}
