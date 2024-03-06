// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.metrics.SyncDomainMetrics
import com.digitalasset.canton.participant.traffic.TrafficStateController.ParticipantTrafficState
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEventTrafficState}
import com.digitalasset.canton.store.SequencedEventStore.PossiblyIgnoredSequencedEvent
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.MemberTrafficStatus

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

/** Maintains the current traffic state up to date for a given domain.
  */
class TrafficStateController(
    val participant: ParticipantId,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SyncDomainMetrics,
) extends NamedLogging {
  private val currentTrafficState =
    new AtomicReference[Option[ParticipantTrafficState]](None)
  def updateBalance(newBalance: NonNegativeLong, serial: PositiveInt, timestamp: CantonTimestamp)(
      implicit tc: TraceContext
  ): Unit = {
    metrics.trafficControl.topologyTransaction.updateValue(newBalance.value)
    metrics.trafficControl.extraTrafficAvailable.updateValue(newBalance.value)
    val newState = currentTrafficState.updateAndGet {
      case Some(old) if old.timestamp <= timestamp =>
        old.state
          .updateLimit(newBalance)
          .map { newState =>
            Some(old.copy(state = newState, timestamp = timestamp, serial = Some(serial)))
          }
          .valueOr { err =>
            logger.info(s"Failed to update traffic state after balance update: $err")
            Some(old)
          }
      case other => other
    }
    logger.debug(s"Updating traffic state after balance update to $newState")
  }

  def updateState(event: PossiblyIgnoredSequencedEvent[ClosedEnvelope])(implicit
      tc: TraceContext
  ): Unit = {
    event.trafficState.foreach { newState =>
      logger.trace(s"Updating traffic control state with $newState")
      metrics.trafficControl.extraTrafficAvailable.updateValue(newState.extraTrafficRemainder.value)
      currentTrafficState.updateAndGet {
        case Some(old) if old.timestamp <= event.timestamp => Some(old.copy(state = newState))
        case None => Some(ParticipantTrafficState(newState, event.timestamp))
        case other => other
      }
    }
  }

  def getState(): Future[Option[MemberTrafficStatus]] = Future.successful {
    currentTrafficState
      .get()
      .map { participantState =>
        MemberTrafficStatus(
          participant,
          participantState.timestamp,
          participantState.state,
          participantState.serial,
        )
      }
  }
}

object TrafficStateController {
  private final case class ParticipantTrafficState(
      state: SequencedEventTrafficState,
      timestamp: CantonTimestamp,
      serial: Option[PositiveInt] = None,
  )
}
