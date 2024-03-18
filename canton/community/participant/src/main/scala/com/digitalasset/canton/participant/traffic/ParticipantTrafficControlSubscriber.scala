// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.SetTrafficBalanceMessage
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TrafficControlErrors.InvalidTrafficControlBalanceMessage
import com.digitalasset.canton.traffic.TrafficControlProcessor.TrafficControlSubscriber

import scala.concurrent.Future

class ParticipantTrafficControlSubscriber(
    trafficStateController: TrafficStateController,
    participantId: ParticipantId,
    override protected val loggerFactory: NamedLoggerFactory,
) extends TrafficControlSubscriber
    with NamedLogging {
  override def observedTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Traffic control handler observed timestamp: $timestamp")
    // Nothing to do here for the participant, only interested in balance updates
  }

  override def balanceUpdate(
      update: SetTrafficBalanceMessage,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful {
    if (update.member == participantId) {
      logger.debug(s"Received balance update from traffic control processor: $update")

      trafficStateController.updateBalance(
        update.totalTrafficBalance,
        update.serial,
        sequencingTimestamp,
      )
    } else {
      InvalidTrafficControlBalanceMessage
        .Error(s"Received a traffic balance update for another member: $update")
        .report()
    }
  }
}
