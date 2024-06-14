// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.BlockUpdateEphemeralState
import com.digitalasset.canton.domain.block.update.BlockUpdateGeneratorImpl.{
  SequencedSubmission,
  State,
}
import com.digitalasset.canton.domain.block.update.SequencedSubmissionsValidator.SequencedSubmissionsValidationResult
import com.digitalasset.canton.domain.block.update.SubmissionRequestValidator.SubmissionRequestValidationResult
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.store.{
  CounterCheckpoint,
  SequencerMemberValidator,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** Validates a list of [[SequencedSubmission]]s corresponding to a chunk.
  */
private[update] final class SequencedSubmissionsValidator(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    unifiedSequencer: Boolean,
    memberValidator: SequencerMemberValidator,
)(implicit closeContext: CloseContext)
    extends NamedLogging {

  private val submissionRequestValidator =
    new SubmissionRequestValidator(
      domainId,
      protocolVersion,
      domainSyncCryptoApi,
      sequencerId,
      rateLimitManager,
      loggerFactory,
      metrics,
      unifiedSequencer = unifiedSequencer,
      memberValidator = memberValidator,
    )

  def validateSequencedSubmissions(
      state: State,
      height: Long,
      submissionRequestsWithSnapshots: Seq[SequencedSubmission],
  )(implicit ec: ExecutionContext): FutureUnlessShutdown[SequencedSubmissionsValidationResult] =
    MonadUtil.foldLeftM(
      initialState = SequencedSubmissionsValidationResult(ephemeralState = state.ephemeral),
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
      stateFromPartialResult,
      reversedEvents,
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
          stateFromPartialResult,
          sequencingTimestamp,
          signedOrderingRequest,
          topologyOrSequencingSnapshot,
          topologyTimestampError,
          latestSequencerEventTimestamp,
        )
      SubmissionRequestValidationResult(newState, outcome, sequencerEventTimestamp) =
        newStateAndOutcome
      result <-
        processSubmissionOutcome(
          newState,
          outcome,
          resultIfNoDeliverEvents = partialResult,
          inFlightAggregationUpdates,
          topologyOrSequencingSnapshot,
          sequencingTimestamp,
          sequencerEventTimestamp,
          latestSequencerEventTimestamp,
          signedOrderingRequest,
          remainingReversedEvents = reversedEvents,
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
      state: BlockUpdateEphemeralState,
      outcome: SubmissionRequestOutcome,
      resultIfNoDeliverEvents: SequencedSubmissionsValidationResult,
      inFlightAggregationUpdates: InFlightAggregationUpdates,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
      sequencerEventTimestamp: Option[CantonTimestamp],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      signedOrderingRequest: SignedOrderingRequest,
      remainingReversedEvents: Seq[UnsignedChunkEvents],
      remainingReversedOutcomes: Seq[SubmissionRequestOutcome],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencedSubmissionsValidationResult] = {
    val SubmissionRequestOutcome(
      deliverEvents,
      newAggregationO,
      unifiedOutcome,
    ) = outcome

    if (unifiedSequencer) {
      unifiedOutcome match {
        case deliverableOutcome: DeliverableSubmissionOutcome =>
          val (newInFlightAggregations, newInFlightAggregationUpdates) =
            newAggregationO.fold(state.inFlightAggregations -> inFlightAggregationUpdates) {
              case (aggregationId, inFlightAggregationUpdate) =>
                InFlightAggregations.tryApplyUpdates(
                  state.inFlightAggregations,
                  Map(aggregationId -> inFlightAggregationUpdate),
                  ignoreInFlightAggregationErrors = false,
                ) ->
                  MapsUtil.extendedMapWith(
                    inFlightAggregationUpdates,
                    Iterable(aggregationId -> inFlightAggregationUpdate),
                  )(_ tryMerge _)
            }
          val newState = state.copy(inFlightAggregations = newInFlightAggregations)
          FutureUnlessShutdown.pure(
            SequencedSubmissionsValidationResult(
              newState,
              Seq.empty,
              newInFlightAggregationUpdates,
              sequencerEventTimestamp,
              outcome +: remainingReversedOutcomes,
            )
          )
        case _ => // Discarded submission
          FutureUnlessShutdown.pure(resultIfNoDeliverEvents)
      }
    } else {
      NonEmpty.from(deliverEvents) match {
        case None => // No state update if there is nothing to deliver
          FutureUnlessShutdown.pure(resultIfNoDeliverEvents)
        case Some(deliverEventsNE) =>
          val newCheckpoints = state.checkpoints ++ deliverEvents.fmap(d =>
            CounterCheckpoint(d.counter, d.timestamp, None)
          ) // ordering of the two operands matters
          val (newInFlightAggregations, newInFlightAggregationUpdates) =
            newAggregationO.fold(state.inFlightAggregations -> inFlightAggregationUpdates) {
              case (aggregationId, inFlightAggregationUpdate) =>
                InFlightAggregations.tryApplyUpdates(
                  state.inFlightAggregations,
                  Map(aggregationId -> inFlightAggregationUpdate),
                  ignoreInFlightAggregationErrors = false,
                ) ->
                  MapsUtil.extendedMapWith(
                    inFlightAggregationUpdates,
                    Iterable(aggregationId -> inFlightAggregationUpdate),
                  )(_ tryMerge _)
            }
          val newState =
            state.copy(
              inFlightAggregations = newInFlightAggregations,
              checkpoints = newCheckpoints,
            )

          val unsignedEvents = UnsignedChunkEvents(
            signedOrderingRequest.submissionRequest.sender,
            deliverEventsNE,
            topologyOrSequencingSnapshot,
            sequencingTimestamp,
            latestSequencerEventTimestamp,
            traceContext,
          )
          FutureUnlessShutdown.pure(
            SequencedSubmissionsValidationResult(
              newState,
              unsignedEvents +: remainingReversedEvents,
              newInFlightAggregationUpdates,
              sequencerEventTimestamp,
              outcome +: remainingReversedOutcomes,
            )
          )
      }
    }
  }
}

private[update] object SequencedSubmissionsValidator {

  final case class SequencedSubmissionsValidationResult(
      ephemeralState: BlockUpdateEphemeralState,
      reversedSignedEvents: Seq[UnsignedChunkEvents] = Seq.empty,
      inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
      lastSequencerEventTimestamp: Option[CantonTimestamp] = None,
      reversedOutcomes: Seq[SubmissionRequestOutcome] = Seq.empty,
  )
}
