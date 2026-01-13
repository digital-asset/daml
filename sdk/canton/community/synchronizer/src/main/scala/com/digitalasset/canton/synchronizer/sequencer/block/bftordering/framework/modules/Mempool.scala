// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.WriteReadiness
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.Traced

object Mempool {

  sealed trait Message extends Product

  final case object Start extends Message

  // From clients
  final case class OrderRequest(
      tx: Traced[OrderingRequest],
      from: Option[ModuleRef[SequencerNode.Message]] = None,
      // Only used for metrics, not populated by unit and simulation tests
      sender: Option[Member] = None,
  ) extends Message

  // From local availability
  final case class CreateLocalBatches(atMost: Short) extends Message

  final case object MempoolBatchCreationClockTick extends Message

  // From local P2P output module
  final case class P2PConnectivityUpdate(
      membership: Membership,
      authenticatedCountIncludingSelf: Int,
  ) extends Message

  sealed trait Admin extends Message
  object Admin {
    final case class GetWriteReadiness(reply: WriteReadiness => Unit) extends Admin
  }
}

trait Mempool[E <: Env[E]] extends Module[E, Mempool.Message] {
  def availability: ModuleRef[Availability.Message[E]]
}
