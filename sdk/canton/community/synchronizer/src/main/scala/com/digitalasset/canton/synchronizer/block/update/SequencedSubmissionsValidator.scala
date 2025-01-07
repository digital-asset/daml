// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.syntax.functor.*
import com.digitalasset.canton.crypto.SynchronizerSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.Sequencer.SignedOrderingRequestOps
import com.digitalasset.canton.synchronizer.sequencing.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

import BlockUpdateGeneratorImpl.{SequencedSubmission, State}
import SequencedSubmissionsValidator.SequencedSubmissionsValidationResult
import SubmissionRequestValidator.SubmissionRequestValidationResult

/** Validates a list of [[SequencedSubmission]]s corresponding to a chunk.
  */
private[update] final class SequencedSubmissionsValidator(
    synchronizerId: SynchronizerId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: SynchronizerSyncCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    memberValidator: SequencerMemberValidator,
)(implicit closeContext: CloseContext)
    extends NamedLogging {

  private val submissionRequestValidator =
    new SubmissionRequestValidator(
      synchronizerId,
      protocolVersion,
      domainSyncCryptoApi,
      sequencerId,
      rateLimitManager,
      loggerFactory,
      metrics,
      memberValidator = memberValidator,
    )

  def validateSequencedSubmissions(
      state: State,
      height: Long,
      submissionRequestsWithSnapshots: Seq[SequencedSubmission],
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] =
    MonadUtil.foldLeftM(
      initialState =
        SequencedSubmissionsValidationResult(inFlightAggregations = state.inFlightAggregations),
      submissionRequestsWithSnapshots,
    )(validateSequencedSubmissionAndAddEvents(state.latestSequencerEventTimestamp, height))

  /** @param latestSequencerEventTimestamp
    * Since each chunk contains at most one event addressed to the sequencer,
    * (and if so it's the last event), we can treat this timestamp static for the whole chunk and
    * need not update it in the accumulator.
    */
  private def validateSequencedSubmissionAndAddEvents(
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      height: Long,
  )(
      partialResult: SequencedSubmissionsValidationResult,
      sequencedSubmissionRequest: SequencedSubmission,
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] = {
    val SequencedSubmissionsValidationResult(
      inFlightAggregations,
      inFlightAggregationUpdates,
      sequencerEventTimestampSoFar,
      reversedOutcomes,
    ) = partialResult

    val SequencedSubmission(
      sequencingTimestamp,
      signedOrderingRequest,
      topologyOrSequencingSnapshot,
      topologyTimestampError,
    ) = sequencedSubmissionRequest

    implicit val traceContext: TraceContext = sequencedSubmissionRequest.traceContext

    ErrorUtil.requireState(
      sequencerEventTimestampSoFar.isEmpty,
      "Only the last event in a chunk could be addressed to the sequencer",
    )

    for {
      newStateAndOutcome <-
        submissionRequestValidator.validateAndGenerateSequencedEvents(
          inFlightAggregations,
          sequencingTimestamp,
          signedOrderingRequest,
          topologyOrSequencingSnapshot,
          topologyTimestampError,
          latestSequencerEventTimestamp,
        )
      SubmissionRequestValidationResult(inFlightAggregations, outcome, sequencerEventTimestamp) =
        newStateAndOutcome
      result <-
        processSubmissionOutcome(
          inFlightAggregations,
          outcome,
          resultIfNoDeliverEvents = partialResult,
          inFlightAggregationUpdates,
          sequencerEventTimestamp,
          remainingReversedOutcomes = reversedOutcomes,
        )
      _ = logger.debug(
        s"At block $height, the submission request ${signedOrderingRequest.submissionRequest.messageId} " +
          s"at $sequencingTimestamp created the following counters: \n" ++ outcome.eventsByMember
            .map { case (member, sequencedEvent) =>
              s"\t counter ${sequencedEvent.counter} for $member"
            }
            .mkString("\n")
      )
    } yield result
  }

  private def processSubmissionOutcome(
      inFlightAggregations: InFlightAggregations,
      outcome: SubmissionRequestOutcome,
      resultIfNoDeliverEvents: SequencedSubmissionsValidationResult,
      inFlightAggregationUpdates: InFlightAggregationUpdates,
      sequencerEventTimestamp: Option[CantonTimestamp],
      remainingReversedOutcomes: Seq[SubmissionRequestOutcome],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencedSubmissionsValidationResult] = {
    val SubmissionRequestOutcome(
      _,
      newAggregationO,
      unifiedOutcome,
    ) = outcome

    unifiedOutcome match {
      case _: DeliverableSubmissionOutcome =>
        val (newInFlightAggregations, newInFlightAggregationUpdates) =
          newAggregationO.fold(inFlightAggregations -> inFlightAggregationUpdates) {
            case (aggregationId, inFlightAggregationUpdate) =>
              InFlightAggregations.tryApplyUpdates(
                inFlightAggregations,
                Map(aggregationId -> inFlightAggregationUpdate),
                ignoreInFlightAggregationErrors = false,
              ) ->
                MapsUtil.extendedMapWith(
                  inFlightAggregationUpdates,
                  Iterable(aggregationId -> inFlightAggregationUpdate),
                )(_ tryMerge _)
          }
        FutureUnlessShutdown.pure(
          SequencedSubmissionsValidationResult(
            newInFlightAggregations,
            newInFlightAggregationUpdates,
            sequencerEventTimestamp,
            outcome +: remainingReversedOutcomes,
          )
        )
      case _ => // Discarded submission
        FutureUnlessShutdown.pure(resultIfNoDeliverEvents)
    }
  }
}

private[update] object SequencedSubmissionsValidator {

  final case class SequencedSubmissionsValidationResult(
      inFlightAggregations: InFlightAggregations,
      inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
      lastSequencerEventTimestamp: Option[CantonTimestamp] = None,
      reversedOutcomes: Seq[SubmissionRequestOutcome] = Seq.empty,
  )
}
