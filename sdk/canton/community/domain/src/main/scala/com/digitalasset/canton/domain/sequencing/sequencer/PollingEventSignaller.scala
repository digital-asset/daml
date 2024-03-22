// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Ignore local writes and simply trigger reads periodically based on a static polling interval.
  * Suitable for horizontally scaled sequencers where the local process will not have in-process visibility of all writes.
  */
class PollingEventSignaller(
    pollingInterval: NonNegativeFiniteDuration,
    val loggerFactory: NamedLoggerFactory,
) extends EventSignaller
    with NamedLogging {
  override def notifyOfLocalWrite(notification: WriteNotification)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.unit

  override def readSignalsForMember(
      member: Member,
      memberId: SequencerMemberId,
  )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] =
    Source
      .tick(pollingInterval.toScala, pollingInterval.toScala, ReadSignal)
      .conflate((a, _) => a)
      .mapMaterializedValue(_ => NotUsed)

  override def close(): Unit = ()
}
