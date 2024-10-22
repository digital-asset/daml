// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.crypto.v30
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.BftOrderingMessageBody
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.admin.EnterpriseSequencerBftAdminData.PeerNetworkStatus
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.networking.Endpoint
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

  final case class Multicast(
      message: BftOrderingMessageBody,
      signature: Option[v30.Signature],
      to: Set[SequencerId],
  ) extends Message

  def send(
      message: BftOrderingMessageBody,
      signature: Option[v30.Signature],
      to: SequencerId,
  ): Multicast =
    Multicast(message, signature, Set(to))
}

trait P2PNetworkOut[E <: Env[E]] extends Module[E, P2PNetworkOut.Message] {
  val dependencies: P2PNetworkOutModuleDependencies[E]
}
