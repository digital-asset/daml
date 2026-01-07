// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.synchronizer.block.BlockFormat.Block.TickTopology
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString

object BlockFormat {

  val DefaultFirstBlockHeight: Long = 0

  /** @param tickTopology
    *   See [[RawLedgerBlock.tickTopologyAtMicrosFromEpoch]].
    */
  final case class Block(
      blockHeight: Long,
      baseSequencingTimeMicrosFromEpoch: Long,
      requests: Seq[Traced[OrderedRequest]],
      tickTopology: Option[TickTopology] = None,
  )
  object Block {

    /** @param atMicrosFromEpoch
      *   Sequencing timestamp of the tick.
      * @param broadcast
      *   Whether the tick should be broadcast to all members of synchronizer or only to sequencers.
      */
    final case class TickTopology(
        atMicrosFromEpoch: Long,
        broadcast: Boolean,
    )
  }

  final case class OrderedRequest(
      microsecondsSinceEpoch: Long,
      tag: String,
      body: ByteString,
      orderingSequencerId: String,
  )

  def blockOrdererBlockToRawLedgerBlock(
      logger: TracedLogger
  )(block: Block): RawLedgerBlock =
    block match {
      case Block(
            blockHeight,
            baseSequencingTimeMicrosFromEpoch,
            requests,
            tickTopologyAtMicrosFromEpoch,
          ) =>
        RawLedgerBlock(
          blockHeight,
          baseSequencingTimeMicrosFromEpoch,
          requests.map {
            case event @ Traced(OrderedRequest(orderingTime, tag, body, orderingSequencerId)) =>
              implicit val traceContext: TraceContext =
                event.traceContext // Preserve the request trace ID in the log
              tag match {
                case AcknowledgeTag =>
                  Traced(RawLedgerBlock.RawBlockEvent.Acknowledgment(body, orderingTime))
                case SendTag =>
                  Traced(
                    RawLedgerBlock.RawBlockEvent.Send(body, orderingTime, orderingSequencerId)
                  )
                case _ =>
                  logger.error(s"Unexpected tag $tag")
                  // It's OK to crash b/c the reference block sequencer is only used for testing
                  sys.exit(1)
              }
          },
          tickTopologyAtMicrosFromEpoch.map { case TickTopology(atMicrosFromEpoch, broadcast) =>
            atMicrosFromEpoch -> broadcast
          },
        )
    }

  private[synchronizer] val AcknowledgeTag = "acknowledge"
  private[synchronizer] val SendTag = "send"
  private[synchronizer] val BatchTag = "batch"
}
