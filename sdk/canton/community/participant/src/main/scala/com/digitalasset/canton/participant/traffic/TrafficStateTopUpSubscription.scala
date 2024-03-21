// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriberX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{TopologyChangeOpX, TrafficControlStateX}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TopUpEvent

class TrafficStateTopUpSubscription(
    trafficStateController: TrafficStateController,
    override val loggerFactory: NamedLoggerFactory,
) extends TopologyTransactionProcessingSubscriberX
    with NamedLogging {
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    transactions
      .flatMap(_.transaction.selectMapping[TrafficControlStateX])
      .filter(_.mapping.member == trafficStateController.participant)
      .toList
      .foreach { tx =>
        if (tx.op.select[TopologyChangeOpX.Replace].isEmpty) {
          logger.warn("Expected replace operation for traffic top up")
        } else {
          logger.debug(
            s"Updating total extra traffic limit for ${tx.mapping.member} from topology transaction with hash ${tx.mapping.uniqueKey.hash}"
          )
          trafficStateController.addTopUp(
            TopUpEvent(
              tx.mapping.totalExtraTrafficLimit,
              effectiveTimestamp.value,
              tx.serial,
            )
          )
        }
      }
  }
}
