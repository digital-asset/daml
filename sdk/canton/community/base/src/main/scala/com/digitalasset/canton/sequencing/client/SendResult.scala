// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.protocol.{
  Deliver,
  DeliverError,
  Envelope,
  SequencerErrors,
}
import com.digitalasset.canton.tracing.TraceContext

/** Possible outcomes for a send operation can be observed by a SequencerClient */
sealed trait SendResult extends Product with Serializable

object SendResult {

  /** Send caused a deliver event to be successfully sequenced.
    * For aggregatable submission requests, success means that the aggregatable submission was assigned a timestamp.
    * It does not mean that the [[com.digitalasset.canton.sequencing.protocol.AggregationRule.threshold]]
    * was reached and the envelopes are delivered.
    * Accordingly, the [[com.digitalasset.canton.sequencing.protocol.Deliver]] event may contain an empty batch.
    */
  final case class Success(deliver: Deliver[Envelope[_]]) extends SendResult

  /** Send caused an event that indicates that the submission was not and never will be sequenced */
  sealed trait NotSequenced extends SendResult

  /** Send caused a deliver error to be sequenced */
  final case class Error(error: DeliverError) extends NotSequenced

  /** No event was sequenced for the send up until the provided max sequencing time.
    * A correct sequencer implementation will no longer sequence any events from the send past this point.
    */
  final case class Timeout(sequencerTime: CantonTimestamp) extends NotSequenced

  /** Log the value of this result to the given logger at an appropriate level and given description */
  def log(sendDescription: String, logger: TracedLogger)(
      result: UnlessShutdown[SendResult]
  )(implicit traceContext: TraceContext): Unit = result match {
    case UnlessShutdown.Outcome(SendResult.Success(deliver)) =>
      logger.trace(s"$sendDescription was sequenced at ${deliver.timestamp}")
    case UnlessShutdown.Outcome(SendResult.Error(error)) =>
      error match {
        case DeliverError(_, _, _, _, SequencerErrors.AggregateSubmissionAlreadySent(_)) =>
          logger.info(
            s"$sendDescription was rejected by the sequencer at ${error.timestamp} because [${error.reason}]"
          )
        case _ =>
          logger.warn(
            s"$sendDescription was rejected by the sequencer at ${error.timestamp} because [${error.reason}]"
          )
      }
    case UnlessShutdown.Outcome(SendResult.Timeout(sequencerTime)) =>
      logger.warn(s"$sendDescription timed out at $sequencerTime")
    case UnlessShutdown.AbortedDueToShutdown =>
      logger.debug(s"$sendDescription aborted due to shutdown")
  }

  def toFutureUnlessShutdown(
      sendDescription: String
  )(result: SendResult): FutureUnlessShutdown[Unit] =
    result match {
      case SendResult.Success(_) =>
        FutureUnlessShutdown.pure(())
      case SendResult.Error(
            DeliverError(_, _, _, _, SequencerErrors.AggregateSubmissionAlreadySent(_))
          ) =>
        // Stop retrying
        FutureUnlessShutdown.unit
      case SendResult.Error(error) =>
        FutureUnlessShutdown.failed(
          new RuntimeException(
            s"$sendDescription was rejected by the sequencer at ${error.timestamp} because [${error.reason}]"
          )
        )
      case SendResult.Timeout(sequencerTime) =>
        FutureUnlessShutdown.failed(
          new RuntimeException(s"$sendDescription timed out at $sequencerTime")
        )
    }
}
