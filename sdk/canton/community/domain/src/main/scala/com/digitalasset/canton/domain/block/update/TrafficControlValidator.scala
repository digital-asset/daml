// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.BlockUpdateEphemeralState
import com.digitalasset.canton.domain.block.update.SubmissionRequestValidator.{
  SequencedEventValidationF,
  SubmissionRequestValidationResult,
  TrafficConsumption,
}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.domain.sequencing.sequencer.{
  DeliverableSubmissionOutcome,
  SubmissionOutcome,
  SubmissionRequestOutcome,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  SequencerDeliverError,
  SequencerErrors,
  SubmissionRequest,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

/** This class contains utility methods to validate, enforce and record metrics over traffic control logic applied
  * after events have been ordered and are read by the sequencer.
  * Largely it applies traffic control rules, and insert a traffic receipt in the deliver receipts to the sender with the result.
  */
private[update] class TrafficControlValidator(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    rateLimitManager: SequencerRateLimitManager,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    unifiedSequencer: Boolean,
)(implicit closeContext: CloseContext)
    extends NamedLogging {

  private def invalidSubmissionRequest(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencerError: SequencerDeliverError,
  )(implicit traceContext: TraceContext): SubmissionRequestOutcome = {
    SubmissionRequestValidator.invalidSubmissionRequest(
      state,
      submissionRequest,
      sequencingTimestamp,
      sequencerError,
      logger,
      domainId,
      protocolVersion,
      unifiedSequencer = unifiedSequencer,
    )
  }

  def applyTrafficControl(
      submissionValidation: SequencedEventValidationF[SubmissionRequestValidationResult],
      state: BlockUpdateEphemeralState,
      signedOrderingRequest: SignedOrderingRequest,
      sequencingTimestamp: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      sender: Member,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[SubmissionRequestValidationResult] = {
    lazy val metricsContext = SequencerMetrics
      .submissionTypeMetricsContext(
        signedOrderingRequest.submissionRequest.batch.allRecipients,
        signedOrderingRequest.submissionRequest.sender,
        logger,
        warnOnUnexpected = false,
      )
      .withExtraLabels(
        "sequencer" -> signedOrderingRequest.content.sequencerId.member.toProtoPrimitive
      )

    submissionValidation.run
      .flatMap {
        // Now we're ready to perform rate limiting if the flag is true and the result should consume traffic
        case (TrafficConsumption(true), result) if result.shouldTryToConsumeTraffic =>
          enforceTrafficControl(
            signedOrderingRequest,
            sequencingTimestamp,
            latestSequencerEventTimestamp,
            warnIfApproximate =
              state.headCounterAboveGenesis(signedOrderingRequest.submissionRequest.sender),
            state,
          )
            .map { receipt =>
              // On successful consumption, updated the result with the receipt
              val updated = result.updateTrafficReceipt(sender, receipt)
              // It's still possible that the result itself is not a `Deliver` (because of earlier failed validation)
              // In that case we record it as wasted traffic
              updated.wastedTrafficReason match {
                case Some(wastedReason) =>
                  recordTrafficWasted(
                    receipt,
                    metricsContext.withExtraLabels("reason" -> wastedReason),
                    signedOrderingRequest,
                  )
                case None =>
                  // Otherwise we record it as traffic successfully spent
                  recordTrafficSpentSuccessfully(receipt, metricsContext)
              }
              updated
            }
            .leftMap { trafficConsumptionErrorOutcome =>
              // If the traffic consumption failed and the processing so far had produced a valid outcome
              // we replace it with the failed outcome from traffic validation
              val updated = result.outcome.outcome match {
                case _: DeliverableSubmissionOutcome =>
                  result.copy(outcome = trafficConsumptionErrorOutcome)
                // Otherwise we keep the existing outcome
                case SubmissionOutcome.Discard => result
              }
              recordSequencingWasted(
                signedOrderingRequest,
                metricsContext
                  .withExtraLabels("reason" -> updated.wastedTrafficReason.getOrElse("unknown")),
              )
              updated
            }
            .merge
        case (_, result) =>
          FutureUnlessShutdown.pure(result)
      }
  }

  private def enforceTrafficControl(
      orderingRequest: SignedOrderingRequest,
      sequencingTimestamp: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
      st: BlockUpdateEphemeralState,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Option[TrafficReceipt]] = {
    val request = orderingRequest.signedSubmissionRequest
    val sender = request.content.sender
    rateLimitManager
      .validateRequestAndConsumeTraffic(
        request.content,
        sequencingTimestamp,
        submissionTimestamp = request.timestampOfSigningKey,
        latestSequencerEventTimestamp = latestSequencerEventTimestamp,
        warnIfApproximate = warnIfApproximate,
        orderingRequest.signature,
      )
      .leftMap {
        // The sender does not have enough traffic for the submission request.
        // Even though it already has been sequenced, we will not deliver it (traffic has not been deducted)
        // This can be due to concurrent submissions to multiple sequencers or the sequencer itself lagging
        // behind the ordering layer.
        // This is not necessarily a sign of malicious activity as the sequencer might just be behind or the sender
        // sent submissions to multiple sequencers at once, therefore temporarily bypassing its limit.
        case error: SequencerRateLimitError.AboveTrafficLimit =>
          logger.debug(
            s"Sender does not have enough traffic at $sequencingTimestamp for event with cost ${error.trafficCost} processed by sequencer ${orderingRequest.signature.signedBy}"
          )
          invalidSubmissionRequest(
            st,
            request.content,
            sequencingTimestamp,
            SequencerErrors.TrafficCredit(error.toString),
          )
            .updateTrafficReceipt(
              sender,
              // When above traffic limit we don't consume traffic, hence cost = 0
              Some(
                error.trafficState.copy(lastConsumedCost = NonNegativeLong.zero).toTrafficReceipt
              ),
            )
        // Outdated event costs are possible if the sender is too far behind and out of the tolerance window.
        // This could be the case if the processing sequencer itself is so far behind that it lets the request
        // through assuming that it's within the window, but ends up being outside the window here at sequencing time
        // Therefore we can't really assume malicious activity here. The cost was not consumed by the rate limiter,
        // we don't deliver the events, and we inform the sender with a deliver error
        case error: SequencerRateLimitError.OutdatedEventCost =>
          logger.info(
            s"Event cost for event at $sequencingTimestamp from sender ${request.content.sender} sent" +
              s" to sequencer ${orderingRequest.signature.signedBy} was outdated: $error."
          )
          invalidSubmissionRequest(
            st,
            request.content,
            sequencingTimestamp,
            SequencerErrors.OutdatedTrafficCost(error.toString),
          ).updateTrafficReceipt(sender, error.trafficReceipt)
        // An incorrect event cost means the sender calculated an incorrect submission cost even
        // according to its own supplied submission topology timestamp.
        // Additionally, the sequencer that received the submission request did not stop it.
        // This indicates both the sender and the sequencer who processed it are malicious.
        // So we raise an alarm and discard the request.
        case error: SequencerRateLimitError.IncorrectEventCost.Error =>
          error.report()
          SubmissionRequestOutcome.discardSubmissionRequest
        // This indicates either that we're doing crash recovery but the traffic consumed data has been pruned already
        // Or that we somehow consumed traffic out of order. We can't really distinguish between the 2 but either way
        // this needs to be visible because it requires investigation and is very likely sign of a bug.
        case error: SequencerRateLimitError.TrafficNotFound =>
          ErrorUtil.invalidState(
            s"Traffic state for event at $sequencingTimestamp for member ${error.member} was not found. This indicates either a bug or overly aggressive pruning."
          )
      }
  }

  // Record when traffic is spent and the event is delivered
  private def recordTrafficSpentSuccessfully(
      receipt: Option[TrafficReceipt],
      metricsContext: MetricsContext,
  ): Unit = {
    receipt.map(_.consumedCost.value).foreach { cost =>
      metrics.trafficControl.eventDelivered.mark(cost)(metricsContext)
    }
  }

  // Record when traffic is deducted but results in a event that is not delivered
  private def recordTrafficWasted(
      receipt: Option[TrafficReceipt],
      metricsContext: MetricsContext,
      signedOrderingRequest: SignedOrderingRequest,
  )(implicit traceContext: TraceContext): Unit = {
    val costO = receipt.map(_.consumedCost.value)
    val messageId = signedOrderingRequest.submissionRequest.messageId
    val sequencerFingerprint = signedOrderingRequest.signature.signedBy
    val sender = signedOrderingRequest.submissionRequest.sender

    // Note that the fingerprint of the submitting sequencer is not validated yet by the driver layer
    // So it does not protect against malicious sequencers, only notifies when honest sequencers let requests to be sequenced
    // which end up being invalidated on the read path
    logger.debug(
      s"Wasted traffic cost${costO.map(c => s" (cost = $c)").getOrElse("")} for messageId $messageId accepted by sequencer $sequencerFingerprint from sender $sender."
    )
    costO.foreach { cost =>
      metrics.trafficControl.wastedTraffic.mark(cost)(metricsContext)
    }
  }

  // We haven't consumed any traffic for the sender but the event will still not be delivered
  // We record this as wasted sequencing work using the raw size of the signed submission request
  private def recordSequencingWasted(
      signedOrderingRequest: SignedOrderingRequest,
      metricsContext: MetricsContext,
  )(implicit traceContext: TraceContext) = {
    val messageId = signedOrderingRequest.submissionRequest.messageId
    val sequencerId = signedOrderingRequest.content.sequencerId
    val sender = signedOrderingRequest.submissionRequest.sender
    val byteSize =
      signedOrderingRequest.signedSubmissionRequest.getCryptographicEvidence.size().toLong

    // Note that the fingerprint of the submitting sequencer is not validated yet by the driver layer
    // So it does not protect against malicious sequencers, only notifies when honest sequencers let requests to be sequenced
    // which end up being invalidated on the read path
    logger.debug(
      s"Wasted sequencing of event with raw byte size $byteSize for messageId $messageId accepted by sequencer $sequencerId from sender $sender."
    )
    metrics.trafficControl.wastedSequencing.mark(byteSize)(metricsContext)
  }

}
