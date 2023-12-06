// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v0, v4}
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
    with PrettyPrinting {

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
    ProtoVersion(0) -> VersionedProtoConverter(ProtocolVersion.v30)(
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
  ): Iterable[RegisterTopologyTransactionRequest] = Seq(
    RegisterTopologyTransactionRequest(
      requestedBy = requestedBy,
      participant = participant,
      requestId = requestId,
      transactions = transactions,
      domainId = domainId,
    )(protocolVersionRepresentativeFor(protocolVersion))
  )

  def fromProtoV0(
      message: v0.RegisterTopologyTransactionRequest
  ): ParsingResult[RegisterTopologyTransactionRequest] = {
    for {
      requestedBy <- Member.fromProtoPrimitive(message.requestedBy, "requestedBy")
      participantUid <- UniqueIdentifier.fromProtoPrimitive(message.participant, "participant")
      transactions <- message.signedTopologyTransactions.toList.traverse(elem =>
        SignedTopologyTransaction.fromByteString(elem)
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
