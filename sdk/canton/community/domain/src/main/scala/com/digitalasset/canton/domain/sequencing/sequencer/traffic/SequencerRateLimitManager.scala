// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.SequencerTrafficControlSubscriber
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.{SubmissionRequest, TrafficState}
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator.EventCostDetails
import com.digitalasset.canton.sequencing.traffic.TrafficConsumedManager.NotEnoughTraffic
import com.digitalasset.canton.sequencing.traffic.{TrafficPurchased, TrafficReceipt}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** Holds the traffic control state and control rate limiting logic of members of a sequencer
  */
trait SequencerRateLimitManager extends AutoCloseable {

  def trafficConsumedStore: TrafficConsumedStore

  /** Create a traffic state for a new member at the given timestamp.
    * Its base traffic remainder will be equal to the max burst window configured at that point in time.
    */
  def registerNewMemberAt(
      member: Member,
      timestamp: CantonTimestamp,
      trafficControlConfig: TrafficControlParameters,
  )(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Returns the latest known balance for the given member.
    */
  def lastKnownBalanceFor(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficPurchased]]

  /** Optional subscriber to the traffic control processor, only used for the new top up implementation
    */
  def balanceUpdateSubscriber: SequencerTrafficControlSubscriber

  /** Prunes traffic control data such as it can still be queried up to 'upToExclusive'.
    */
  def prune(upToExclusive: CantonTimestamp)(implicit traceContext: TraceContext): Future[String]

  /** Timestamp of the latest known state of traffic purchased entries.
    */
  def balanceKnownUntil: Option[CantonTimestamp]

  /** Validate signed submission requests received by the sequencer.
    * Requests with a an invalid cost or a cost that exceeds the traffic credit will be rejected.
    * @param submissionTimestamp The timestamp the sender of the submission request claims to have used to compute
    *                            the submission cost.
    * @param lastSequencerEventTimestamp timestamp of the last event addressed to the sequencer.
    */
  def validateRequestAtSubmissionTime(
      request: SubmissionRequest,
      submissionTimestamp: Option[CantonTimestamp],
      lastSequencedTimestamp: CantonTimestamp,
      lastSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      tc: TraceContext,
      closeContext: CloseContext,
  ): EitherT[
    FutureUnlessShutdown,
    SequencerRateLimitError,
    Unit,
  ]

  /** Validate submission requests after they've been sequenced.
    * The same validation as performed at submission is run again, to ensure that the submitting sequencer was not malicious.
    * If the request is valid, the cost is consumed.
    * This method MUST be called sequentially for a given sender, meaning concurrent updates are not supported.
    * This method MUST be called in order of sequencing times, meaning 2 events sequenced at T1 and T2 such that T2 > T1 must be
    * processed by this method in the order T1 -> T2.
    * However, if T1 and T2 have been processed in the correct order, it is then ok to call the method with T1 again,
    * which will result in the same output as when it was first called.
    * @param lastSequencerEventTimestamp timestamp of the last event addressed to the sequencer.
    */
  def validateRequestAndConsumeTraffic(
      request: SubmissionRequest,
      sequencingTime: CantonTimestamp,
      submissionTimestamp: Option[CantonTimestamp],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
      sequencerSignature: Signature,
  )(implicit
      tc: TraceContext,
      closeContext: CloseContext,
  ): EitherT[
    FutureUnlessShutdown,
    SequencerRateLimitError,
    Option[TrafficReceipt],
  ]

  /** Return the latest known traffic states for the requested members, or all currently cached members if requestedMembers is empty.
    * @param minTimestamp The minimum timestamp at which to get the traffic states.
    *                      If the current known state is more recent than minTimestamp, it will be returned.
    *                      If minTimestamp is more recent than the current known state, an APPROXIMATION of the state at minTimestamp will be used.
    *                      Specifically, the base traffic remainder will be extrapolated to minTimestamp. There is no guarantee
    *                      that the state returned will be the same as the correct one, when the domain time actually reaches minTimestamp.
    * @param lastSequencerEventTimestamp timestamp of the last event addressed to the sequencer.
    * @return A Map of member and their traffic state.
    */
  def getStates(
      requestedMembers: Set[Member],
      minTimestamp: Option[CantonTimestamp],
      lastSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Map[Member, Either[String, TrafficState]]]

  def getTrafficStateForMemberAt(
      member: Member,
      timestamp: CantonTimestamp,
      lastSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[TrafficState]]
}

sealed trait SequencerRateLimitError

object SequencerRateLimitError extends SequencerErrorGroup {

  /** Errors specifically coming from failed validation of the submission cost.
    * They contain the actual cost that was consumed so it can be communicated to the sender in the deliver error.
    */
  sealed trait SequencingCostValidationError extends SequencerRateLimitError {
    def correctEventCost: NonNegativeLong
  }

  /** This will be raised in the case where an event's traffic is being re-processed by the SequencerRateLimitManager
    * (for instance in case of crash-recovery), but the TrafficConsumed object corresponding to the event cannot be found.
    * This could indicate that events are being processed out of order (the first time), which is not supported.
    * Or the traffic consumed store may have been pruned and the information is not available anymore.
    * @param member member for which traffic was requested
    */
  final case class TrafficNotFound(
      member: Member
  ) extends SequencerRateLimitError
      with PrettyPrinting {
    override def pretty: Pretty[TrafficNotFound] = prettyOfClass(
      param("member", _.member)
    )
  }

  /** The event cost exceeds the available traffic. Only returned during validation at submission time.
    * @param member sender of the event
    * @param trafficCost cost of the event
    * @param trafficState current traffic state estimated by the sequencer when processing the submission
    */
  final case class AboveTrafficLimit(
      member: Member,
      trafficCost: NonNegativeLong,
      trafficState: TrafficState,
  ) extends SequencerRateLimitError
      with PrettyPrinting {

    override def pretty: Pretty[AboveTrafficLimit] = prettyOfClass(
      param("member", _.member),
      param("trafficCost", _.trafficCost),
      param("trafficState", _.trafficState),
    )
  }

  object AboveTrafficLimit {
    def apply(notEnoughTraffic: NotEnoughTraffic): AboveTrafficLimit = AboveTrafficLimit(
      notEnoughTraffic.member,
      notEnoughTraffic.cost,
      notEnoughTraffic.trafficState,
    )
  }

  object IncorrectEventCost extends AlarmErrorCode("INCORRECT_EVENT_COST") {
    final case class Error(
        member: Member,
        submissionTimestamp: Option[CantonTimestamp],
        submittedEventCost: Option[NonNegativeLong],
        validationTimestamp: CantonTimestamp,
        sequencerProcessingSubmissionRequest: Option[Fingerprint],
        trafficReceipt: Option[TrafficReceipt] = None,
        correctCostDetails: EventCostDetails,
    ) extends Alarm(
          s"Missing or incorrect event cost provided by member $member at $submissionTimestamp." +
            s" Submitted: $submittedEventCost, valid: ${correctCostDetails.eventCost}, validationTimestamp: $validationTimestamp. " +
            s"Processing sequencer: $sequencerProcessingSubmissionRequest"
        )
        with SequencingCostValidationError
        with PrettyPrinting {
      override val correctEventCost: NonNegativeLong = correctCostDetails.eventCost
      override def pretty: Pretty[Error] = prettyOfClassWithName("IncorrectEventCost")(
        param("member", _.member),
        param("submissionTimestamp", _.submissionTimestamp),
        paramIfDefined("submittedEventCost", _.submittedEventCost),
        param("validationTimestamp", _.validationTimestamp),
        paramIfDefined(
          "sequencerProcessingSubmissionRequest",
          _.sequencerProcessingSubmissionRequest,
        ),
        paramIfDefined(
          "trafficReceipt",
          _.trafficReceipt,
        ),
        param(
          "correctCostDetails",
          _.correctCostDetails,
        ),
      )
    }
  }

  final case class OutdatedEventCost(
      member: Member,
      submittedEventCost: Option[NonNegativeLong],
      submissionTimestamp: CantonTimestamp,
      correctEventCost: NonNegativeLong,
      timestampUsedForValidation: CantonTimestamp,
      trafficReceipt: Option[TrafficReceipt] = None,
  ) extends SequencingCostValidationError
}
