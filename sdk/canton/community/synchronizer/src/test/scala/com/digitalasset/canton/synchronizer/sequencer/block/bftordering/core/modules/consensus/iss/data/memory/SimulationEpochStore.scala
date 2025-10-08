// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.ViewNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.tracing.TraceContext

import scala.util.Try

final class SimulationEpochStore(failOnViewChange: Boolean)
    extends GenericInMemoryEpochStore[SimulationEnv]
    with BaseTest {
  override protected def createFuture[T](action: String)(value: () => Try[T]): SimulationFuture[T] =
    SimulationFuture(action)(value)
  override def close(): Unit = ()

  override def addOrderedBlockAtomically(
      prePrepare: SignedMessage[ConsensusMessage.PrePrepare],
      commitMessages: Seq[SignedMessage[ConsensusMessage.Commit]],
  )(implicit traceContext: TraceContext): SimulationFuture[Unit] = {
    val viewNumber = prePrepare.message.viewNumber
    if (failOnViewChange && viewNumber > ViewNumber.First) {
      val blockNumber = prePrepare.message.blockMetadata.blockNumber
      clue(s"Expecting no view changes when checking block $blockNumber") {
        viewNumber shouldBe ViewNumber.First
      }
    }

    super.addOrderedBlockAtomically(prePrepare, commitMessages)
  }
}
