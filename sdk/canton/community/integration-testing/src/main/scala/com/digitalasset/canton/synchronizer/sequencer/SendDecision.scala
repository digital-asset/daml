// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{SignedContent, SubmissionRequest}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Defines what the [[ProgrammableSequencer]] should do with a [[SubmissionRequest]] */
sealed trait SendDecision extends Product with Serializable

object SendDecision {

  /** Forward the [[SubmissionRequest]] to the underlying [[Sequencer]] for processing */
  case object Process extends SendDecision

  /** Immediately reject the [[SubmissionRequest]] and return a [[protocol.SendAsyncError]] */
  case object Reject extends SendDecision

  /** Drop the submission request but provide a successful [[protocol.SendAsyncError]] emulating the
    * send getting lost
    */
  case object Drop extends SendDecision

  /** Delay the [[SubmissionRequest]] by the given duration. */
  final case class Delay(duration: Duration) extends SendDecision

  /** Delay the [[SubmissionRequest]] until the timestamp has elapsed. */
  final case class DelayUntil(timestamp: CantonTimestamp) extends SendDecision

  /** Hold back the [[SubmissionRequest]] until the given future completes. */
  final case class HoldBack(processWhenCompleted: Future[Unit]) extends SendDecision

  /** Put the [[SubmissionRequest]] on hold until a message has been sent for which the given
    * predicate holds.
    *
    * @param releaseAfter
    *   Predicate to decide when to release the [[SubmissionRequest]] on hold. All calls to this
    *   predicate happen one after the other.
    */
  final case class OnHoldUntil(releaseAfter: SubmissionRequest => Boolean) extends SendDecision

  /** Replaces the message with `replacement` and sends the replacement. */
  final case class Replace(
      replacement: SignedContent[SubmissionRequest],
      more: SignedContent[SubmissionRequest]*
  ) extends SendDecision
  object Replace {
    def apply(replacement: SubmissionRequest, more: SubmissionRequest*): Replace = {
      def fakeSignature(request: SubmissionRequest): SignedContent[SubmissionRequest] =
        SignedContent(request, Signature.noSignature, None, BaseTest.testedProtocolVersion)

      Replace(fakeSignature(replacement), more.map(fakeSignature)*)
    }
  }
}
