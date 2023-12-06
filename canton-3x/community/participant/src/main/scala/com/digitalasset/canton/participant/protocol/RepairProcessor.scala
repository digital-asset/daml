// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestData
import com.digitalasset.canton.tracing.{TraceContext, W3CTraceContext}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference

/** Deals with repair request as part of messsage processing.
  * As is, it merely skips the request counters.
  */
class RepairProcessor(
    requestCounterAllocator: RequestCounterAllocator,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val remainingRepairRequests: AtomicReference[Seq[RequestData]] =
    new AtomicReference[Seq[RequestData]](RepairProcessor.noRepairRequests)

  // Lazy initialization of `remainingRepairRequests`
  private[participant] def setRemainingRepairRequests(repairRequests: Seq[RequestData]): Unit = {
    val replaced =
      remainingRepairRequests.compareAndSet(RepairProcessor.noRepairRequests, repairRequests)
    if (!replaced)
      throw new IllegalStateException("Cannot replace outstanding repair requests")
  }

  def wedgeRepairRequests(
      upToExclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): Unit = {
    val remaining = remainingRepairRequests.get()
    val (current, rest) = remaining.span(_.requestTimestamp < upToExclusive)

    NonEmpty.from(current).foreach { currentNE =>
      val firstRc = currentNE.head1
      val lastRc = currentNE.last1
      logger.info(s"Skipping over repair requests with counters $firstRc to $lastRc")
      currentNE.foreach(skipRequest)
      val replaced = remainingRepairRequests.compareAndSet(remaining, rest)
      if (!replaced)
        throw new ConcurrentModificationException(
          "The remaining repair requests have been modified concurrently."
        )
    }
  }

  private def skipRequest(requestData: RequestData): Unit = {
    val RequestData(rc, _, _, _, repairContext) = requestData
    implicit val repairTraceContext =
      W3CTraceContext.toTraceContext(repairContext.map(_.unwrap), None)
    requestCounterAllocator.skipRequestCounter(rc)
  }
}

object RepairProcessor {
  private def noRepairRequests: Seq[RequestData] = Seq.empty
}
