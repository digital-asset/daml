// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.data.EitherT
import cats.instances.list.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.domain.sequencing.traffic.EnterpriseSequencerRateLimitManager.TrafficStateUpdateResult
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  TrafficState,
}
import com.digitalasset.canton.sequencing.traffic.{EventCostCalculator, TrafficPurchased}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class EnterpriseSequencerRateLimitManager(
    @VisibleForTesting
    private[canton] val trafficPurchasedManager: TrafficPurchasedManager,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    metrics: SequencerMetrics,
    sequencerMemberRateLimiterFactory: SequencerMemberRateLimiterFactory =
      DefaultSequencerMemberRateLimiterFactory,
    eventCostCalculator: EventCostCalculator,
    protocolVersion: ProtocolVersion,
) extends SequencerRateLimitManager
    with NamedLogging
    with FlagCloseable {

  // Holds in memory the rate limiter for each sequencer member
  private val rateLimitsPerMember = TrieMap[Member, SequencerMemberRateLimiter]()

  private def getOrCreateMemberRateLimiter(
      member: Member
  ): SequencerMemberRateLimiter = {
    rateLimitsPerMember.getOrElse(
      member, {
        val rateLimiter = sequencerMemberRateLimiterFactory.create(
          member,
          loggerFactory,
          metrics,
          eventCostCalculator,
        )
        rateLimitsPerMember.addOne(member -> rateLimiter).discard
        rateLimiter
      },
    )
  }

  override def createNewTrafficStateAt(
      member: Member,
      timestamp: CantonTimestamp,
      trafficControlParameters: TrafficControlParameters,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[TrafficState] = FutureUnlessShutdown.pure {
    TrafficState(
      extraTrafficRemainder = NonNegativeLong.zero,
      extraTrafficConsumed = NonNegativeLong.zero,
      baseTrafficRemainder = trafficControlParameters.maxBaseTrafficAmount,
      timestamp,
    )
  }

  /** Consume the traffic costs of the submission request from the sender's traffic state.
    *
    * NOTE: This method must be called in order of the sequencing timestamps.
    */
  override def consume(
      sender: Member,
      batch: Batch[ClosedEnvelope],
      sequencingTimestamp: CantonTimestamp,
      trafficState: TrafficState,
      trafficControlParameters: TrafficControlParameters,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      lastBalanceUpdateTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[
    FutureUnlessShutdown,
    SequencerRateLimitError,
    TrafficState,
  ] = {
    logger.debug(s"Consuming event for $sender at $sequencingTimestamp with state $trafficState")
    for {
      currentBalance <- trafficPurchasedManager
        .getTrafficPurchasedAt(
          sender,
          sequencingTimestamp,
          lastBalanceUpdateTimestamp,
          warnIfApproximate,
        )
        .leftMap { case TrafficPurchasedManager.TrafficPurchasedAlreadyPruned(member, timestamp) =>
          SequencerRateLimitError.UnknownBalance(member, timestamp)
        }
      newTrafficState <- EitherT
        .fromEither[FutureUnlessShutdown](
          getOrCreateMemberRateLimiter(sender)
            .tryConsume(
              batch,
              sequencingTimestamp,
              trafficControlParameters,
              trafficState,
              groupToMembers,
              currentBalance.map(_.extraTrafficPurchased).getOrElse(NonNegativeLong.zero),
              protocolVersion,
            )
        )
    } yield {
      logger.debug(s"Update state for $sender at $sequencingTimestamp is $newTrafficState")
      newTrafficState
    }
  }

  private def getBalanceOrNone(
      timestamp: CantonTimestamp,
      lastBalanceUpdateTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(member: Member)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Option[TrafficPurchased]] = {
    trafficPurchasedManager
      .getTrafficPurchasedAt(member, timestamp, lastBalanceUpdateTimestamp, warnIfApproximate)
      .leftMap { case TrafficPurchasedManager.TrafficPurchasedAlreadyPruned(member, timestamp) =>
        SequencerRateLimitError.UnknownBalance(member, timestamp)
      }
      .valueOr { err =>
        logger.warn(s"Failed to obtain the traffic purchased entry for $member at $timestamp", err)
        None
      }
  }

  private def getUpdatedTrafficStates(
      partialTrafficStates: Map[Member, TrafficState],
      getBalance: Member => FutureUnlessShutdown[Option[TrafficPurchased]],
      trafficControlParameters: TrafficControlParameters,
      requestedTimestamp: Option[CantonTimestamp],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Map[Member, TrafficStateUpdateResult]] = {
    partialTrafficStates.toList
      .parTraverse { case (member, originalState) =>
        getBalance(member)
          .map { balanceO =>
            // Timestamp used for update, in order of priority:
            // 1. Requested timestamp
            // 2. Timestamp of the balance update
            // 3. Original state timestamp
            val timestamp = requestedTimestamp
              .orElse(balanceO.map(_.sequencingTimestamp))
              .getOrElse(originalState.timestamp)

            if (timestamp >= originalState.timestamp) {
              val state = getOrCreateMemberRateLimiter(member)
                .updateTrafficState(
                  timestamp,
                  trafficControlParameters,
                  NonNegativeLong.zero,
                  originalState,
                  balanceO.map(_.extraTrafficPurchased).getOrElse(NonNegativeLong.zero),
                )
                ._1
              member -> TrafficStateUpdateResult(state, balanceO.map(_.serial))
            } else {
              member -> TrafficStateUpdateResult(originalState, balanceO.map(_.serial))
            }
          }
      }
      .map(_.toMap)
  }

  def getLatestTrafficStates(
      partialTrafficStates: Map[Member, TrafficState],
      trafficControlParameters: TrafficControlParameters,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Map[Member, TrafficStateUpdateResult]] = {
    getUpdatedTrafficStates(
      partialTrafficStates,
      trafficPurchasedManager.getLatestKnownBalance,
      trafficControlParameters,
      None,
    )
  }

  override def getUpdatedTrafficStatesAtTimestamp(
      partialTrafficStates: Map[Member, TrafficState],
      updateTimestamp: CantonTimestamp,
      trafficControlParameters: TrafficControlParameters,
      lastBalanceUpdateTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Map[Member, TrafficStateUpdateResult]] = {
    getUpdatedTrafficStates(
      partialTrafficStates,
      getBalanceOrNone(updateTimestamp, lastBalanceUpdateTimestamp, warnIfApproximate),
      trafficControlParameters,
      Some(updateTimestamp),
    )
  }

  override def lastKnownBalanceFor(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficPurchased]] =
    trafficPurchasedManager.getLatestKnownBalance(member)

  override def onClosed(): Unit = {
    Lifecycle.close(trafficPurchasedManager)(logger)
  }
  override def balanceUpdateSubscriber: SequencerTrafficControlSubscriber =
    trafficPurchasedManager.subscription

  override def safeForPruning(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    trafficPurchasedManager.setSafeToPruneBeforeExclusive(timestamp)

  override def balanceKnownUntil: Option[CantonTimestamp] = trafficPurchasedManager.maxTsO
}

object EnterpriseSequencerRateLimitManager {

  /** Wrapper class returned when updating the traffic state of members
    * Optionally contains the serial of the balance update that corresponds to the "balance" of the traffic state
    */
  final case class TrafficStateUpdateResult(
      state: TrafficState,
      balanceUpdateSerial: Option[PositiveInt],
  )
}
