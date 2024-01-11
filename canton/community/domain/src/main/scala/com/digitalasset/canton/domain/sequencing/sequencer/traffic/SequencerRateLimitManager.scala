// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  GroupRecipient,
  TrafficState,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.{MemberTrafficStatus, TopUpEvent}

import scala.concurrent.{ExecutionContext, Future}

/** Holds the traffic control state and control rate limiting logic of members of a sequencer
  */
trait SequencerRateLimitManager {

  /** Compute the traffic status (including effective traffic limit) for members based on their traffic state
    */
  def getTrafficStatusFor(members: Map[Member, TrafficState])(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Seq[MemberTrafficStatus]]

  /** Create a traffic state for a new member at the given timestamp.
    * Its base traffic remainder will be equal to the max burst window configured at that point in time.
    */
  def createNewTrafficStateAt(
      member: Member,
      timestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[TrafficState]

  /** Top up a member with its new extra traffic limit. Must be strictly increasing between subsequent calls.
    *
    * @param member               member to top up
    * @param newExtraTrafficTotal new limit
    */
  def topUp(
      member: Member,
      newExtraTrafficTotal: TopUpEvent,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Unit]

  /** Consume the traffic costs of the submission request from the sender's traffic state.
    *
    * NOTE: This method must be called in order of the sequencing timestamps.
    */
  def consume(
      sender: Member,
      batch: Batch[ClosedEnvelope],
      sequencingTimestamp: CantonTimestamp,
      trafficState: TrafficState,
      trafficControlConfig: TrafficControlParameters,
      groupToMembers: Map[GroupRecipient, Set[Member]],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[
    Future,
    SequencerRateLimitError,
    TrafficState,
  ]

  /** Takes a partial map of traffic states and update them to be up to date with the given timestamp.
    * In particular recompute relevant state fields for the effective extra traffic limit at that timestamp.
    * The return state map will be merged back into the complete state.
    */
  def updateTrafficStates(
      partialTrafficStates: Map[Member, TrafficState],
      timestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Map[Member, TrafficState]]
}

sealed trait SequencerRateLimitError {
  def trafficState: Option[TrafficState]
}

object SequencerRateLimitError {
  final case class AboveTrafficLimit(
      member: Member,
      trafficCost: NonNegativeLong,
      override val trafficState: Option[TrafficState],
  ) extends SequencerRateLimitError
}
