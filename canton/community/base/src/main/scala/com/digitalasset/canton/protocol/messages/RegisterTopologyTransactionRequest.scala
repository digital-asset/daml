// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, v1, v2, v3, v4}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, Member, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.{
  HasProtocolVersionedCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}

/** @param representativeProtocolVersion The representativeProtocolVersion must correspond to the protocol version of
  *                                      every transaction in the list (enforced by the factory method)
  */
final case class RegisterTopologyTransactionRequest private (
    requestedBy: Member,
    participant: ParticipantId,
    requestId: TopologyRequestId,
    transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
    override val domainId: DomainId,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      RegisterTopologyTransactionRequest.type
    ]
) extends UnsignedProtocolMessage
    with ProtocolMessageV0
    with ProtocolMessageV1
    with ProtocolMessageV2
    with ProtocolMessageV3
    with UnsignedProtocolMessageV4
    with PrettyPrinting {

  override def toProtoEnvelopeContentV0: v0.EnvelopeContent =
    v0.EnvelopeContent(
      v0.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoEnvelopeContentV1: v1.EnvelopeContent =
    v1.EnvelopeContent(
      v1.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoEnvelopeContentV2: v2.EnvelopeContent =
    v2.EnvelopeContent(
      v2.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoEnvelopeContentV3: v3.EnvelopeContent =
    v3.EnvelopeContent(
      v3.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)
    )

  override def toProtoSomeEnvelopeContentV4: v4.EnvelopeContent.SomeEnvelopeContent =
    v4.EnvelopeContent.SomeEnvelopeContent.RegisterTopologyTransactionRequest(toProtoV0)

  def toProtoV0: v0.RegisterTopologyTransactionRequest =
    v0.RegisterTopologyTransactionRequest(
      requestedBy = requestedBy.toProtoPrimitive,
      participant = participant.uid.toProtoPrimitive,
      requestId = requestId.toProtoPrimitive,
      signedTopologyTransactions = transactions.map(_.getCryptographicEvidence),
      domainId = domainId.unwrap.toProtoPrimitive,
    )

  @transient override protected lazy val companionObj: RegisterTopologyTransactionRequest.type =
    RegisterTopologyTransactionRequest

  override def pretty: Pretty[RegisterTopologyTransactionRequest] = prettyOfClass(
    param("requestBy", _.requestedBy),
    param("participant", _.participant),
    param("requestId", _.requestId.unwrap.doubleQuoted),
    param("numTx", _.transactions.length),
  )

}

object RegisterTopologyTransactionRequest
    extends HasProtocolVersionedCompanion[RegisterTopologyTransactionRequest] {

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v3)(
      v0.RegisterTopologyTransactionRequest
    )(
      supportedProtoVersion(_)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def create(
      requestedBy: Member,
      participant: ParticipantId,
      requestId: TopologyRequestId,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
  ): RegisterTopologyTransactionRequest =
    RegisterTopologyTransactionRequest(
      requestedBy = requestedBy,
      participant = participant,
      requestId = requestId,
      transactions = transactions,
      domainId = domainId,
    )(protocolVersionRepresentativeFor(protocolVersion))

  def fromProtoV0(
      message: v0.RegisterTopologyTransactionRequest
  ): ParsingResult[RegisterTopologyTransactionRequest] = {
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      transactions <- message.signedTopologyTransactions.toList.traverse(elem =>
        SignedTopologyTransaction.fromByteStringUnsafe(elem) // TODO(#12626) – try with context
      )
      domainUid <- UniqueIdentifier.fromProtoPrimitive(message.domainId, "domainId")
      requestId <- String255.fromProtoPrimitive(message.requestId, "requestId")
    } yield RegisterTopologyTransactionRequest(
      requestedBy,
      ParticipantId(participantUid),
      requestId,
      transactions,
      DomainId(domainUid),
    )(protocolVersionRepresentativeFor(ProtoVersion(0)))
  }

  override def name: String = "RegisterTopologyTransactionRequest"

  implicit val registerTopologyTransactionRequestCast
      : ProtocolMessageContentCast[RegisterTopologyTransactionRequest] =
    ProtocolMessageContentCast.create[RegisterTopologyTransactionRequest](
      "RegisterTopologyTransactionRequest"
    ) {
      case rttr: RegisterTopologyTransactionRequest => Some(rttr)
      case _ => None
    }
}
