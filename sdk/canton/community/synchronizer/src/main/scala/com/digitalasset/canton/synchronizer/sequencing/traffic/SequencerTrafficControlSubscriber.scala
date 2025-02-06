// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.SetTrafficPurchasedMessage
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor.TrafficControlSubscriber
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.tracing.TraceContext

class SequencerTrafficControlSubscriber(
    manager: TrafficPurchasedManager,
    override protected val loggerFactory: NamedLoggerFactory,
) extends TrafficControlSubscriber
    with NamedLogging {
  override def observedTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Traffic control handler observed timestamp: $timestamp")
    manager.tick(timestamp)
  }

  override def trafficPurchasedUpdate(
      update: SetTrafficPurchasedMessage,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    logger.debug(s"Received balance update from traffic control processor: $update")
    manager.addTrafficPurchased(
      TrafficPurchased(
        update.member,
        update.serial,
        update.totalTrafficPurchased,
        sequencingTimestamp,
      )
    )
  }
}
