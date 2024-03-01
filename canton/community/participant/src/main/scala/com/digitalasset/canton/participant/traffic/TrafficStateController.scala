// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import cats.instances.option.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEventTrafficState}
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.{MemberTrafficStatus, TopUpEvent}
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.FutureUtil
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Maintains the current traffic state up to date for a given domain.
  */
class TrafficStateController(
    val participant: ParticipantId,
    override val loggerFactory: NamedLoggerFactory,
    topologyClient: DomainTopologyClientWithInit,
    metrics: SyncDomainMetrics,
    clock: Clock,
) extends NamedLogging {
  private val currentTrafficState =
    new AtomicReference[Option[SequencedEventTrafficState]](None)
  def addTopUp(topUp: TopUpEvent)(implicit tc: TraceContext): Unit = {
    metrics.trafficControl.topologyTransaction.updateValue(topUp.limit.value)
    FutureUtil.doNotAwaitUnlessShutdown(
      // Getting the state will update metrics with the latest top up value
      clock.scheduleAt(
        _ => metrics.trafficControl.extraTrafficAvailable.updateValue(topUp.limit.value),
        topUp.validFromInclusive,
      ),
      "update metrics after top up is effective",
    )
  }

  def updateState(event: PossiblyIgnoredSequencedEvent[ClosedEnvelope])(implicit
      tc: TraceContext
  ): Unit = {
    event.trafficState.foreach { newState =>
      logger.trace(s"Updating traffic control state with $newState")
      currentTrafficState.set(Some(newState))
      metrics.trafficControl.extraTrafficAvailable.updateValue(newState.extraTrafficRemainder.value)
    }
  }

  def getState(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[MemberTrafficStatus]] = {
    currentTrafficState
      .get()
      .parTraverse { trafficState =>
        val currentSnapshot = topologyClient.headSnapshot

        currentSnapshot
          .trafficControlStatus(Seq(participant))
          .map(_.get(participant).flatten)
          .map { topologyTrafficOpt =>
            MemberTrafficStatus(
              participant,
              currentSnapshot.timestamp,
              topologyTrafficOpt
                .map { totalTrafficLimit =>
                  // If there is a traffic limit in the topology state, use that to compute the traffic state returned
                  val newRemainder =
                    totalTrafficLimit.totalExtraTrafficLimit.value - trafficState.extraTrafficConsumed.value
                  trafficState
                    .focus(_.extraTrafficRemainder)
                    .replace(
                      NonNegativeLong.tryCreate(newRemainder)
                    )
                }
                .getOrElse(trafficState),
              List.empty, // TODO(i17477): Was never used, set to empty for now and remove when we're done with the rework
            )
          }

      }
  }
}
