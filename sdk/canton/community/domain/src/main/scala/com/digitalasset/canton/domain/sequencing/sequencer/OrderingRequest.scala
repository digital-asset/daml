// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.Functor
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  BytestringWithCryptographicEvidence,
  HasCryptographicEvidence,
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

/** Wrapper for requests sent by a sequencer to the ordering layer.
  * @param sequencerId sequencerId of the sequencer requesting ordering
  * @param content content to be ordered
  */
final case class OrderingRequest[+A <: HasCryptographicEvidence] private (
    sequencerId: SequencerId,
    content: A,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[OrderingRequest.type],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[OrderingRequest[HasCryptographicEvidence]]
    with ProtocolVersionedMemoizedEvidence {

  @transient override protected lazy val companionObj: OrderingRequest.type = OrderingRequest

  private def toProtoV30: v30.OrderingRequest =
    v30.OrderingRequest(
      sequencerId.toProtoPrimitive,
      Some(content.getCryptographicEvidence),
    )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], B <: HasCryptographicEvidence](
      f: A => F[B]
  )(implicit F: Functor[F]): F[OrderingRequest[B]] =
    F.map(f(content)) { newContent =>
      if (newContent eq content) this.asInstanceOf[OrderingRequest[B]]
      else this.copy(content = newContent)
    }

  def copy[B <: HasCryptographicEvidence](
      content: B = this.content,
      sequencerId: SequencerId = this.sequencerId,
  ): OrderingRequest[B] =
    OrderingRequest(
      sequencerId,
      content,
    )(representativeProtocolVersion, None)

  def deserializeContent[B <: HasCryptographicEvidence](
      contentDeserializer: ByteString => ParsingResult[B]
  ): ParsingResult[OrderingRequest[B]] =
    this.traverse(content => contentDeserializer(content.getCryptographicEvidence))

}

object OrderingRequest
    extends HasMemoizedProtocolVersionedWrapperCompanion2[
      OrderingRequest[HasCryptographicEvidence],
      OrderingRequest[BytestringWithCryptographicEvidence],
    ] {

  override def name: String = "OrderingRequest"

  override def supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(v30.OrderingRequest)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  def create[A <: HasCryptographicEvidence](
      sequencerId: SequencerId,
      content: A,
      protocolVersion: ProtocolVersion,
  ): OrderingRequest[A] = {
    OrderingRequest(sequencerId, content)(protocolVersionRepresentativeFor(protocolVersion), None)
  }

  def fromProtoV30(
      orderingRequestP: v30.OrderingRequest
  )(
      bytes: ByteString
  ): ParsingResult[OrderingRequest[BytestringWithCryptographicEvidence]] = {
    val v30.OrderingRequest(sequencerIdP, content) = orderingRequestP
    for {
      contentB <- ProtoConverter.required("content", content)
      sequencerId <- SequencerId.fromProtoPrimitive(sequencerIdP, "sequencer_id")
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield OrderingRequest(sequencerId, BytestringWithCryptographicEvidence(contentB))(
      rpv,
      Some(bytes),
    )
  }
}
