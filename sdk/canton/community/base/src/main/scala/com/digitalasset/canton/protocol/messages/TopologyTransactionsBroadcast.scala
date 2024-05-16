// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.messages.TopologyTransactionsBroadcast.Broadcast
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.{
  HasProtocolVersionedWithContextCompanion,
  ProtoVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
}

final case class TopologyTransactionsBroadcast private (
    override val domainId: DomainId,
    broadcasts: Seq[Broadcast],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      TopologyTransactionsBroadcast.type
    ]
) extends UnsignedProtocolMessage {

  @transient override protected lazy val companionObj: TopologyTransactionsBroadcast.type =
    TopologyTransactionsBroadcast

  override protected[messages] def toProtoSomeEnvelopeContentV30
      : v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.TopologyTransactionsBroadcast(toProtoV30)

  def toProtoV30: v30.TopologyTransactionsBroadcast = v30.TopologyTransactionsBroadcast(
    domainId.toProtoPrimitive,
    broadcasts = broadcasts.map(_.toProtoV30),
  )

}

object TopologyTransactionsBroadcast
    extends HasProtocolVersionedWithContextCompanion[
      TopologyTransactionsBroadcast,
      ProtocolVersion,
    ] {

  def create(
      domainId: DomainId,
      broadcasts: Seq[Broadcast],
      protocolVersion: ProtocolVersion,
  ): TopologyTransactionsBroadcast =
    TopologyTransactionsBroadcast(domainId = domainId, broadcasts = broadcasts)(
      supportedProtoVersions.protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "TopologyTransactionsBroadcast"

  implicit val acceptedTopologyTransactionMessageCast
      : ProtocolMessageContentCast[TopologyTransactionsBroadcast] =
    ProtocolMessageContentCast.create[TopologyTransactionsBroadcast](
      name
    ) {
      case att: TopologyTransactionsBroadcast => Some(att)
      case _ => None
    }

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.v31)(
      v30.TopologyTransactionsBroadcast
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[messages] def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.TopologyTransactionsBroadcast,
  ): ParsingResult[TopologyTransactionsBroadcast] = {
    val v30.TopologyTransactionsBroadcast(domain, broadcasts) = message
    for {
      domainId <- DomainId.fromProtoPrimitive(domain, "domain")
      broadcasts <- broadcasts.traverse(broadcastFromProtoV30(expectedProtocolVersion))
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield TopologyTransactionsBroadcast(domainId, broadcasts.toList)(rpv)
  }

  private def broadcastFromProtoV30(expectedProtocolVersion: ProtocolVersion)(
      message: v30.TopologyTransactionsBroadcast.Broadcast
  ): ParsingResult[Broadcast] = {
    val v30.TopologyTransactionsBroadcast.Broadcast(broadcastId, transactions) = message
    for {
      broadcastId <- String255.fromProtoPrimitive(broadcastId, "broadcast_id")
      transactions <- transactions.traverse(tx =>
        SignedTopologyTransaction.fromProtoV30(
          ProtocolVersionValidation(expectedProtocolVersion),
          tx,
        )
      )
    } yield Broadcast(broadcastId, transactions.toList)
  }

  final case class Broadcast(
      broadcastId: TopologyRequestId,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]],
  ) {
    def toProtoV30: v30.TopologyTransactionsBroadcast.Broadcast =
      v30.TopologyTransactionsBroadcast.Broadcast(
        broadcastId = broadcastId.toProtoPrimitive,
        transactions = transactions.map(_.toProtoV30),
      )
  }

  /** The state of the submission of a topology transaction broadcast. In combination with the sequencer client
    * send tracker capability, State reflects that either the sequencer Accepted the submission or that the submission
    * was Rejected due to an error or a timeout. See DomainTopologyService.
    */
  sealed trait State extends Product with Serializable

  object State {
    case object Failed extends State

    case object Accepted extends State
  }
}
