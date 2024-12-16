// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction
import com.digitalasset.canton.tracing.TraceContext

trait OrderingTopologyProvider[E <: Env[E]] {

  /** Get the sequencer topology at a given activation time, i.e., one that incorporates all topology changes
    *  that have become part of a topology snapshot at or before that time.
    *
    * @param activationTime The timestamp with which to query the topology client for a topology snapshot.
    *                       See [[TopologyActivationTime]] for details.
    * @param traceContext The trace context.
    * @return A future that completes and yields the requested topology only if at least the immediate
    *         predecessor has been successfully sequenced and is visible to the sequencer's topology processor.
    */
  def getOrderingTopologyAt(
      activationTime: TopologyActivationTime
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[(OrderingTopology, CryptoProvider[E])]]
}

object OrderingTopologyProvider {

  val InitialOrderingTopologyActivationTime: TopologyActivationTime =
    TopologyActivationTime(
      SignedTopologyTransaction.InitialTopologySequencingTime.immediateSuccessor
    )
}
