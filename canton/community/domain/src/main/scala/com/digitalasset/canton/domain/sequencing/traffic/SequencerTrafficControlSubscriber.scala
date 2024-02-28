// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.SetTrafficBalanceMessage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TrafficControlProcessor.TrafficControlSubscriber

class SequencerTrafficControlSubscriber(override protected val loggerFactory: NamedLoggerFactory)
    extends TrafficControlSubscriber
    with NamedLogging {
  override def observedTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Traffic control handler observed timestamp: $timestamp")
    // Take note that sequenced messages up to `timestamp` have been processed
    // by the traffic control handler
  }

  override def balanceUpdate(update: SetTrafficBalanceMessage)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Received balance update from traffic control processor: $update")

    // Check the serial of the new balance against the latest for the affected member
    // if not more recent -> info and skip

    // Update the balance for the affected member
  }
}
