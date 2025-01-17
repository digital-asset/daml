// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  FutureId,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftEventFromFuture
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

final class SegmentClosingBehaviour[E <: Env[E]](
    private val waitingForFutureIds: mutable.Set[FutureId],
    actionName: String,
    parent: ModuleRef[Consensus.Message[E]],
    firstBlockNumber: BlockNumber,
    epochNumber: EpochNumber,
    messageToSendParent: Consensus.Message[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends Module[E, ConsensusSegment.Message] {

  override def ready(self: ModuleRef[ConsensusSegment.Message]): Unit =
    // If we are not waiting for any Future, have a message that will stop the module
    self.asyncSend(ConsensusSegment.StartModuleClosingBehaviour)

  override protected def receiveInternal(
      message: ConsensusSegment.Message
  )(implicit context: E#ActorContextT[ConsensusSegment.Message], traceContext: TraceContext): Unit =
    message match {
      case PbftEventFromFuture(_, futureId) =>
        waitingForFutureIds.remove(futureId).discard
        if (waitingForFutureIds.isEmpty) {
          stop()
        }

      case ConsensusSegment.StartModuleClosingBehaviour =>
        if (waitingForFutureIds.isEmpty) {
          stop()
        }

      case _ =>
        logger.error(
          s"Segment module $firstBlockNumber $actionName epoch $epochNumber but got unexpected message: $message"
        )
    }

  private def stop()(implicit
      context: E#ActorContextT[ConsensusSegment.Message],
      traceContext: TraceContext,
  ): Unit =
    context.stop { () =>
      logger.info(
        s"Segment module $firstBlockNumber $actionName epoch $epochNumber"
      )
      parent.asyncSend(messageToSendParent)
    }

}
