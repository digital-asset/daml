// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.syntax.functor.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}

import scala.concurrent.ExecutionContext

import BlockUpdateGeneratorImpl.{SequencedValidatedSubmission, State}
import SequencedSubmissionsValidator.SequencedSubmissionsValidationResult
import SubmissionRequestValidator.SubmissionRequestValidationResult

/** Validates a list of
  * [[com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest]]s
  * corresponding to a chunk.
  */
private[update] final class SequencedSubmissionsValidator(
    override val loggerFactory: NamedLoggerFactory,
    submissionRequestValidator: SubmissionRequestValidator,
) extends NamedLogging {

  def sequentialApplySubmissionsAndEmitOutcomes(
      state: State,
      height: Long,
      sequencedValidatedSubmissions: Seq[SequencedValidatedSubmission],
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] =
    MonadUtil.foldLeftM(
      initialState =
        SequencedSubmissionsValidationResult(inFlightAggregations = state.inFlightAggregations),
      sequencedValidatedSubmissions,
    )(applySubmissionAndAddOutcome(state.latestSequencerEventTimestamp, height))

  /** @param latestSequencerEventTimestamp
    *   Since each chunk contains at most one event addressed to the sequencer, (and if so it's the
    *   last event), we can treat this timestamp static for the whole chunk and need not update it
    *   in the accumulator.
    */
  private def applySubmissionAndAddOutcome(
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      height: Long,
  )(
      partialResult: SequencedSubmissionsValidationResult,
      sequencedValidatedSubmissionRequest: SequencedValidatedSubmission,
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] = {
    val SequencedSubmissionsValidationResult(
      inFlightAggregations,
      inFlightAggregationUpdates,
      sequencerEventTimestampSoFar,
      reversedOutcomes,
    ) = partialResult

    val SequencedValidatedSubmission(
      sequencingTimestamp,
      signedSubmissionRequest,
      orderingSequencerId,
      _,
      _,
      trafficConsumption,
      errorOrResolvedGroups,
    ) = sequencedValidatedSubmissionRequest

    implicit val traceContext: TraceContext = sequencedValidatedSubmissionRequest.traceContext

    ErrorUtil.requireState(
      sequencerEventTimestampSoFar.isEmpty,
      "Only the last event in a chunk could be addressed to the sequencer",
    )

    for {
      newStateAndOutcome <-
        submissionRequestValidator.applyAggregationAndTrafficControlAndGenerateOutcomes(
          inFlightAggregations,
          sequencingTimestamp,
          signedSubmissionRequest,
          orderingSequencerId,
          trafficConsumption,
          errorOrResolvedGroups,
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
        s"At block $height, the submission request ${signedSubmissionRequest.content.messageId} " +
          s"at $sequencingTimestamp validated to: ${SubmissionOutcome.prettyString(outcome)}"
      )
    } yield result
  }

  private def processSubmissionOutcome(
      inFlightAggregations: InFlightAggregations,
      outcome: SubmissionOutcome,
      resultIfNoDeliverEvents: SequencedSubmissionsValidationResult,
      inFlightAggregationUpdates: InFlightAggregationUpdates,
      sequencerEventTimestamp: Option[CantonTimestamp],
      remainingReversedOutcomes: Seq[SubmissionOutcome],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencedSubmissionsValidationResult] =
    outcome match {
      case deliverable: DeliverableSubmissionOutcome =>
        val (newInFlightAggregations, newInFlightAggregationUpdates) =
          deliverable.inFlightAggregation.fold(inFlightAggregations -> inFlightAggregationUpdates) {
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

private[update] object SequencedSubmissionsValidator {

  final case class SequencedSubmissionsValidationResult(
      inFlightAggregations: InFlightAggregations,
      inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
      lastSequencerEventTimestamp: Option[CantonTimestamp] = None,
      reversedOutcomes: Seq[SubmissionOutcome] = Seq.empty,
  )
}
