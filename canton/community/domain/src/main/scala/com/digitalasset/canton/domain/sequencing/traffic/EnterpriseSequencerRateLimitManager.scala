// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.data.EitherT
import cats.instances.list.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.domain.sequencing.traffic.EnterpriseSequencerRateLimitManager.{
  BalanceUpdateClient,
  TrafficStateUpdateResult,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  TrafficState,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.EventCostCalculator

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class EnterpriseSequencerRateLimitManager(
    balanceUpdateClient: BalanceUpdateClient,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    override val timeouts: ProcessingTimeout,
    metrics: SequencerMetrics,
    sequencerMemberRateLimiterFactory: SequencerMemberRateLimiterFactory =
      DefaultSequencerMemberRateLimiterFactory,
    eventCostCalculator: EventCostCalculator = new EventCostCalculator(),
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
  ] = for {
    currentBalance <- balanceUpdateClient(
      sender,
      sequencingTimestamp,
      lastBalanceUpdateTimestamp,
      warnIfApproximate,
    )
    newTrafficState <- EitherT
      .fromEither[FutureUnlessShutdown](
        getOrCreateMemberRateLimiter(sender)
          .tryConsume(
            batch,
            sequencingTimestamp,
            trafficControlParameters,
            trafficState,
            groupToMembers,
            currentBalance.map(_.balance).getOrElse(NonNegativeLong.zero),
          )
      )
      .leftWiden[SequencerRateLimitError]
  } yield newTrafficState

  override def updateTrafficStates(
      partialTrafficStates: Map[Member, TrafficState],
      updateTimestamp: Option[CantonTimestamp],
      trafficControlParameters: TrafficControlParameters,
      lastBalanceUpdateTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): FutureUnlessShutdown[Map[Member, TrafficStateUpdateResult]] = {
    def getBalanceOrNone(member: Member, timestamp: CantonTimestamp) = {
      balanceUpdateClient(member, timestamp, lastBalanceUpdateTimestamp, warnIfApproximate)
        .valueOr { err =>
          logger.warn(s"Failed to obtain the traffic balance for $member at $timestamp", err)
          None
        }
    }
    // Use the provided timestamp or the latest known balance otherwise
    val timestampO = updateTimestamp.orElse(balanceUpdateClient.lastKnownTimestamp)
    for {
      updated <- partialTrafficStates.toList
        .parTraverse { case (member, originalState) =>
          timestampO match {
            // Only update if the provided timestamp is in the future compared to the latest known state
            // We don't provide updates in the past
            case Some(ts) if ts > originalState.timestamp =>
              getBalanceOrNone(member, ts)
                .map { balance =>
                  val state = getOrCreateMemberRateLimiter(member)
                    .updateTrafficState(
                      ts,
                      trafficControlParameters,
                      NonNegativeLong.zero,
                      originalState,
                      balance
                        .map(_.balance)
                        // If we don't have a balance, we use the original limit if there's one
                        .orElse(originalState.extraTrafficLimit.map(_.toNonNegative))
                        // Otherwise it means no traffic was bought
                        .getOrElse(NonNegativeLong.zero),
                    )
                    ._1
                  TrafficStateUpdateResult(state, balance.map(_.serial))
                }
                .map(member -> _)
            case _ =>
              getBalanceOrNone(member, originalState.timestamp)
                .map(balance =>
                  member -> TrafficStateUpdateResult(originalState, balance.map(_.serial))
                )
          }
        }
        .map(_.toMap)
    } yield updated
  }

  override def onClosed(): Unit = {
    Lifecycle.close(balanceUpdateClient)(logger)
  }
}

object EnterpriseSequencerRateLimitManager {

  /** Wrapper class returned when updating the traffic state of members
    * Optionally contains the serial of the balance update that corresponds to the "balance" of the traffic state
    */
  final case class TrafficStateUpdateResult(
      state: TrafficState,
      balanceUpdateSerial: Option[PositiveInt],
  )

  trait BalanceUpdateClient extends AutoCloseable {
    def apply(
        member: Member,
        timestamp: CantonTimestamp,
        lastSeen: Option[CantonTimestamp] = None,
        warnIfApproximate: Boolean = true,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SequencerRateLimitError, Option[TrafficBalance]]
    def lastKnownTimestamp: Option[CantonTimestamp]
  }
}
