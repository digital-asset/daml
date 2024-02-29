// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.instances.list.*
import cats.syntax.parallel.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
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

import scala.concurrent.ExecutionContext

class TopologyTransactionTrafficSubscription(
    rateLimitManager: SequencerRateLimitManager,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessingSubscriberX
    with NamedLogging {
  override def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    transactions
      .flatMap(_.transaction.selectMapping[TrafficControlStateX])
      .toList
      .parTraverse_ { tx =>
        if (tx.operation.select[TopologyChangeOpX.Replace].isEmpty) {
          logger.warn("Expected replace operation for traffic top up")
          FutureUnlessShutdown.unit
        } else {
          logger.debug(
            s"Updating total extra traffic limit for ${tx.mapping.member} from topology transaction with hash ${tx.mapping.uniqueKey.hash}"
          )
          // The rate limit manager takes care of synchronizing top ups and applying them in sequenced time order
          // so we don't need to worry about concurrency here
          FutureUnlessShutdown.outcomeF(
            rateLimitManager
              .topUp(
                tx.mapping.member,
                TopUpEvent(
                  tx.mapping.totalExtraTrafficLimit,
                  effectiveTimestamp.value,
                  tx.serial,
                ),
              )
          )
        }
      }
  }
}
