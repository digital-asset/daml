// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import cats.data.EitherT
import cats.instances.future.*
import cats.instances.list.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficLimitsStore
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
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
import com.digitalasset.canton.traffic.{EventCostCalculator, MemberTrafficStatus, TopUpEvent}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.FutureUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class EnterpriseSequencerRateLimitManager(
    trafficLimitsStore: TrafficLimitsStore,
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
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[SequencerMemberRateLimiter] = {
    rateLimitsPerMember
      .get(member)
      .map(Future.successful)
      .getOrElse {
        trafficLimitsStore
          .getExtraTrafficLimits(member)
          .map { topUps =>
            logger.debug(
              s"Retrieved top ups from storage to initialize rate limiter for $member: $topUps"
            )
            val rateLimiter = sequencerMemberRateLimiterFactory.create(
              member,
              topUps,
              loggerFactory,
              metrics,
              eventCostCalculator,
            )
            rateLimitsPerMember.addOne(member -> rateLimiter).discard
            rateLimiter
          }
      }
  }

  override def getTrafficStatusFor(members: Map[Member, TrafficState])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Seq[MemberTrafficStatus]] = {
    members.toList
      .parFlatTraverse { case (member, state) =>
        val ts = state.timestamp
        for {
          rlm <- getOrCreateMemberRateLimiter(member)
          updatedState = state
            .update(rlm.getTrafficLimit(ts), ts)
            .valueOr { err =>
              logger.warn(s"Traffic state for $member could not be updated: $err")
              state
            }
            .toSequencedEventTrafficState
          currentAndFutureTopUps = rlm.pruneUntilAndGetAllTopUpsFor(ts)
        } yield List(
          MemberTrafficStatus(member, state.timestamp, updatedState, currentAndFutureTopUps)
        )
      }
  }

  override def createNewTrafficStateAt(
      member: Member,
      timestamp: CantonTimestamp,
      trafficControlParameters: TrafficControlParameters,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[TrafficState] = for {
    rlm <- getOrCreateMemberRateLimiter(member)
  } yield TrafficState(
    extraTrafficRemainder = rlm.getTrafficLimit(timestamp),
    NonNegativeLong.zero,
    baseTrafficRemainder = trafficControlParameters.maxBaseTrafficAmount,
    timestamp,
  )

  override def topUp(
      member: Member,
      event: TopUpEvent,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Unit] = for {
    // Update the in memory rate limiter for that member
    _ <- getOrCreateMemberRateLimiter(member)
      .map(_.topUp(event))
    // Update the persistence store
    _ <-
      trafficLimitsStore.updateTotalExtraTrafficLimit(
        member,
        event,
      )

  } yield ()

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
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[
    Future,
    SequencerRateLimitError,
    TrafficState,
  ] = for {
    rateLimiter <- EitherT.liftF(getOrCreateMemberRateLimiter(sender))
    memberConsumeResult = rateLimiter
      .tryConsume(
        batch,
        sequencingTimestamp,
        trafficControlParameters,
        trafficState,
        groupToMembers,
        sender,
      )
    (consumeResponse, newEffectiveTopUpOpt) = memberConsumeResult
    newTrafficState <- consumeResponse
      .leftWiden[SequencerRateLimitError]
      .toEitherT[Future]
    // If a new top up became effective, prune the store below that
    // Do it asynchronously on purpose because we don't need this to keep processing messages, it's just clean up
    _ = newEffectiveTopUpOpt.foreach(topUp =>
      FutureUtil.doNotAwait(
        trafficLimitsStore.pruneBelowSerial(sender, topUp.serial),
        s"Failed to prune traffic limits store for member $sender for new effective top up $topUp",
      )
    )
  } yield newTrafficState

  override def updateTrafficStates(
      partialTrafficStates: Map[Member, TrafficState],
      timestamp: CantonTimestamp,
      trafficControlParameters: TrafficControlParameters,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Map[Member, TrafficState]] = for {
    updated <- partialTrafficStates.toList
      .parTraverse { case (member, state) =>
        getOrCreateMemberRateLimiter(member)
          .map(
            // Update the traffic state by passing an event cost of 0. This will update all traffic info based on current timestamp
            // without consuming any traffic
            _.updateTrafficState(
              timestamp,
              trafficControlParameters,
              NonNegativeLong.zero,
              state,
            )._1
          )
          .map(member -> _)
      }
      .map(_.toMap)
  } yield updated

  override def onClosed(): Unit = {
    Lifecycle.close(trafficLimitsStore)(logger)
  }

}
