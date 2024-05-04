// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.SetTrafficPurchasedMessage
import com.digitalasset.canton.sequencing.traffic.TrafficControlProcessor.TrafficControlSubscriber
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

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
  ): Future[Unit] = {
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
