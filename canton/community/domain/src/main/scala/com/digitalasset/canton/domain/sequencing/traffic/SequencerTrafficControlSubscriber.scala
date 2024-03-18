// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.SetTrafficBalanceMessage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TrafficControlProcessor.TrafficControlSubscriber

import scala.concurrent.Future

class SequencerTrafficControlSubscriber(
    manager: TrafficBalanceManager,
    override protected val loggerFactory: NamedLoggerFactory,
) extends TrafficControlSubscriber
    with NamedLogging {
  override def observedTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Traffic control handler observed timestamp: $timestamp")
    manager.tick(timestamp)
  }

  override def balanceUpdate(
      update: SetTrafficBalanceMessage,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    logger.debug(s"Received balance update from traffic control processor: $update")
    manager.addTrafficBalance(
      TrafficBalance(
        update.member,
        update.serial,
        update.totalTrafficBalance,
        sequencingTimestamp,
      )
    )
  }
}
