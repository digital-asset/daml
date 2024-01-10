// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import cats.instances.option.*
import cats.syntax.parallel.*
import com.daml.error.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.sync.SyncServiceError.TrafficControlErrorGroup
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEventTrafficState}
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.{MemberTrafficStatus, TopUpEvent, TopUpQueue}
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
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  private val currentTrafficState =
    new AtomicReference[Option[SequencedEventTrafficState]](None)
  private val topUpQueue = new TopUpQueue(List.empty)
  def addTopUp(topUp: TopUpEvent)(implicit tc: TraceContext): Unit = {
    metrics.trafficControl.topologyTransaction.updateValue(topUp.limit.value)
    topUpQueue.addOne(topUp)

    FutureUtil.doNotAwaitUnlessShutdown(
      // Getting the state will update metrics with the latest top up value
      clock.scheduleAt(_ => getState.discard, topUp.validFromInclusive),
      "update local state after top up is effective",
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
                  // If it changed, update metrics
                  if (newRemainder != trafficState.extraTrafficRemainder.value)
                    metrics.trafficControl.extraTrafficAvailable.updateValue(newRemainder)
                  trafficState
                    .focus(_.extraTrafficRemainder)
                    .replace(
                      NonNegativeLong.tryCreate(newRemainder)
                    )
                }
                .getOrElse(trafficState),
              topUpQueue.pruneUntilAndGetAllTopUpsFor(currentSnapshot.timestamp),
            )
          }

      }
  }
}

object TrafficStateController {

  object TrafficControlError extends TrafficControlErrorGroup {
    sealed trait TrafficControlError extends Product with Serializable with CantonError

    @Explanation(
      """This error indicates that no available domain with that id could be found, and therefore
       no traffic state could be retrieved."""
    )
    @Resolution("Ensure that the participant is connected to the domain with the provided id.")
    object DomainIdNotFound
        extends ErrorCode(
          id = "TRAFFIC_CONTROL_DOMAIN_ID_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        ) {
      final case class Error(domainId: DomainId)(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "The domain id was not found"
          )
          with TrafficControlError
    }

    @Explanation(
      """This error indicates that the participant does not have a traffic state."""
    )
    @Resolution(
      """Ensure that the the participant is connected to a domain with traffic control enabled,
        and that it has received at least one event from the domain since its connection."""
    )
    object TrafficStateNotFound
        extends ErrorCode(
          id = "TRAFFIC_CONTROL_STATE_NOT_FOUND",
          ErrorCategory.InvalidGivenCurrentSystemStateResourceMissing,
        ) {
      final case class Error()(implicit
          val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Traffic state not found"
          )
          with TrafficControlError
    }
  }

}
