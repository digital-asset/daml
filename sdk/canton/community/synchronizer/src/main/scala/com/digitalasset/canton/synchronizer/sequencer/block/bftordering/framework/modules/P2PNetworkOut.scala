// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.PeerNetworkStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.topology.SequencerId

object P2PNetworkOut {

  sealed trait Message extends Product

  case object Start extends Message

  sealed trait Internal extends Message
  object Internal {
    final case class Connect(endpoint: Endpoint) extends Internal
    final case class Disconnect(endpoint: Endpoint) extends Internal
  }

  sealed trait Network extends Message
  object Network {
    final case class Authenticated(endpoint: Endpoint, sequencerId: SequencerId) extends Message
  }

  sealed trait Admin extends Message
  object Admin {
    final case class AddEndpoint(endpoint: Endpoint, callback: Boolean => Unit) extends Admin
    final case class RemoveEndpoint(endpoint: Endpoint, callback: Boolean => Unit) extends Admin
    final case class GetStatus(
        endpoints: Option[Iterable[Endpoint]],
        callback: PeerNetworkStatus => Unit,
    ) extends Admin
  }

  sealed trait BftOrderingNetworkMessage {
    def toProto: v1.BftOrderingMessageBody
  }

  object BftOrderingNetworkMessage {
    final case class AvailabilityMessage(
        signedMessage: SignedMessage[Availability.RemoteProtocolMessage]
    ) extends BftOrderingNetworkMessage {
      override def toProto: v1.BftOrderingMessageBody = v1.BftOrderingMessageBody.of(
        v1.BftOrderingMessageBody.Message.AvailabilityMessage(signedMessage.toProtoV1)
      )
    }

    final case class ConsensusMessage(
        signedMessage: SignedMessage[ConsensusSegment.ConsensusMessage.PbftNetworkMessage]
    ) extends BftOrderingNetworkMessage {
      override def toProto: v1.BftOrderingMessageBody = v1.BftOrderingMessageBody.of(
        v1.BftOrderingMessageBody.Message.ConsensusMessage(signedMessage.toProtoV1)
      )
    }

    final case class RetransmissionMessage(
        signedMessage: SignedMessage[Consensus.RetransmissionsMessage.RetransmissionsNetworkMessage]
    ) extends BftOrderingNetworkMessage {
      override def toProto: v1.BftOrderingMessageBody = v1.BftOrderingMessageBody.of(
        v1.BftOrderingMessageBody.Message.RetransmissionMessage(signedMessage.toProtoV1)
      )
    }

    final case class StateTransferMessage(
        signedMessage: SignedMessage[Consensus.StateTransferMessage.StateTransferNetworkMessage]
    ) extends BftOrderingNetworkMessage {
      override def toProto: v1.BftOrderingMessageBody = v1.BftOrderingMessageBody.of(
        v1.BftOrderingMessageBody.Message.StateTransferMessage(signedMessage.toProtoV1)
      )
    }

    final case object Empty extends BftOrderingNetworkMessage {
      override def toProto: v1.BftOrderingMessageBody =
        v1.BftOrderingMessageBody(v1.BftOrderingMessageBody.Message.Empty)
    }
  }

  final case class Multicast(
      message: BftOrderingNetworkMessage,
      to: Set[SequencerId],
  ) extends Message

  def send(
      message: BftOrderingNetworkMessage,
      to: SequencerId,
  ): Multicast =
    Multicast(message, Set(to))
}

trait P2PNetworkOut[E <: Env[E]] extends Module[E, P2PNetworkOut.Message] {
  val dependencies: P2PNetworkOutModuleDependencies[E]
}
