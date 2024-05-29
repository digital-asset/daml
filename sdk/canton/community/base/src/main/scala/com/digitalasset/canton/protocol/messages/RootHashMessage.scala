// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

/** One root hash message is sent for each participant involved in a mediator confirmation request that requires root hash messages.
  * The root hash message is delivered to the participant and the mediator.
  * The mediator checks that it receives the right root hash messages
  * and that they all contain the root hash that the mediator confirmation request message specifies.
  * The mediator also checks that all payloads have the same serialization and,
  * if it can parse the mediator confirmation request envelope, that the payload fits to the mediator confirmation request.
  */
final case class RootHashMessage[+Payload <: RootHashMessagePayload](
    rootHash: RootHash,
    override val domainId: DomainId,
    viewType: ViewType,
    submissionTopologyTimestamp: CantonTimestamp,
    payload: Payload,
)(override val representativeProtocolVersion: RepresentativeProtocolVersion[RootHashMessage.type])
    extends UnsignedProtocolMessage
    with PrettyPrinting {

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.RootHashMessage(toProtoV30)

  def toProtoV30: v30.RootHashMessage = v30.RootHashMessage(
    rootHash = rootHash.toProtoPrimitive,
    domainId = domainId.toProtoPrimitive,
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
      domainId,
      viewType,
      submissionTopologyTime,
      payload,
    )(representativeProtocolVersion)

  @transient override protected lazy val companionObj: RootHashMessage.type = RootHashMessage
}

object RootHashMessage
    extends HasProtocolVersionedWithContextCompanion[RootHashMessage[
      RootHashMessagePayload
    ], ByteString => ParsingResult[RootHashMessagePayload]] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.RootHashMessage)(
      supportedProtoVersion(_)((deserializer, proto) => fromProtoV30(deserializer)(proto)),
      _.toProtoV30.toByteString,
    )
  )

  def apply[Payload <: RootHashMessagePayload](
      rootHash: RootHash,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      viewType: ViewType,
      submissionTopologyTime: CantonTimestamp,
      payload: Payload,
  ): RootHashMessage[Payload] = RootHashMessage(
    rootHash,
    domainId,
    viewType,
    submissionTopologyTime,
    payload,
  )(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV30[Payload <: RootHashMessagePayload](
      payloadDeserializer: ByteString => ParsingResult[Payload]
  )(
      rootHashMessageP: v30.RootHashMessage
  ): ParsingResult[RootHashMessage[Payload]] = {
    val v30.RootHashMessage(rootHashP, domainIdP, viewTypeP, submissionTopologyTimeP, payloadP) =
      rootHashMessageP
    for {
      rootHash <- RootHash.fromProtoPrimitive(rootHashP)
      domainId <- DomainId.fromProtoPrimitive(domainIdP, "domain_id")
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      submissionTopologyTime <- CantonTimestamp.fromProtoPrimitive(submissionTopologyTimeP)
      payloadO <- payloadDeserializer(payloadP)
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield RootHashMessage(
      rootHash,
      domainId,
      viewType,
      submissionTopologyTime,
      payloadO,
    )(rpv)
  }

  implicit def rootHashMessageProtocolMessageContentCast[Payload <: RootHashMessagePayload](implicit
      cast: RootHashMessagePayloadCast[Payload]
  ): ProtocolMessageContentCast[RootHashMessage[Payload]] =
    ProtocolMessageContentCast.create[RootHashMessage[Payload]]("RootHashMessage") {
      case rhm: RootHashMessage[_] => rhm.traverse(cast.toKind)
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
  override def pretty: Pretty[EmptyRootHashMessagePayload.type] = prettyOfString(_ => "\"\"")
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

  override def pretty: Pretty[SerializedRootHashMessagePayload] = prettyOfClass(
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
