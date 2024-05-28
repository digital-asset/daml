// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString

object BlockFormat {

  val DefaultFirstBlockHeight: Long = 0

  final case class Block(
      blockHeight: Long,
      requests: Seq[Traced[OrderedRequest]],
  )

  final case class OrderedRequest(
      microsecondsSinceEpoch: Long,
      tag: String,
      body: ByteString,
  )

  def blockOrdererBlockToRawLedgerBlock(
      logger: TracedLogger
  )(block: Block): RawLedgerBlock =
    block match {
      case Block(blockHeight, requests) =>
        RawLedgerBlock(
          blockHeight,
          requests.map { case event @ Traced(OrderedRequest(orderingTime, tag, body)) =>
            implicit val traceContext: TraceContext =
              event.traceContext // Preserve the request trace ID in the log
            tag match {
              case AcknowledgeTag =>
                Traced(RawLedgerBlock.RawBlockEvent.Acknowledgment(body))
              case SendTag =>
                Traced(RawLedgerBlock.RawBlockEvent.Send(body, orderingTime))
              case _ =>
                logger.error(s"Unexpected tag $tag")
                // It's OK to crash b/c the reference block sequencer is only used for testing
                sys.exit(1)
            }
          },
        )
    }

  private[domain] val AcknowledgeTag = "acknowledge"
  private[domain] val SendTag = "send"
  private[domain] val BatchTag = "batch"
}
