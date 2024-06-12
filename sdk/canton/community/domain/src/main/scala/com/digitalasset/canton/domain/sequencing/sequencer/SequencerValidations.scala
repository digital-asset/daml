// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  SendAsyncError,
  SubmissionRequest,
}
import com.digitalasset.canton.topology.Member

object SequencerValidations {
  def checkSenderAndRecipientsAreRegistered(
      submission: SubmissionRequest,
      isRegistered: Member => Boolean,
  ): Either[SendAsyncError, Unit] = for {
    _ <- Either.cond(
      isRegistered(submission.sender),
      (),
      SendAsyncError.SenderUnknown(
        s"Sender is unknown: ${submission.sender}"
      ): SendAsyncError,
    )
    // TODO(#19476): Why we don't check group recipients here?
    unregisteredRecipients = submission.batch.allMembers.toList.filterNot(isRegistered)
    _ <- Either.cond(
      unregisteredRecipients.isEmpty,
      (),
      SendAsyncError.UnknownRecipients(
        s"The following recipients are invalid: ${unregisteredRecipients.mkString(",")}"
      ): SendAsyncError,
    )
    unregisteredEligibleSenders = submission.aggregationRule.fold(Seq.empty[Member])(
      _.eligibleSenders.filterNot(isRegistered)
    )
    _ <- Either.cond(
      unregisteredEligibleSenders.isEmpty,
      (),
      SendAsyncError.SenderUnknown(
        s"The following senders in the aggregation rule are unknown: $unregisteredEligibleSenders"
      ),
    )
  } yield ()

  def wellformedAggregationRule(sender: Member, rule: AggregationRule): Either[String, Unit] = {
    val AggregationRule(eligibleSenders, threshold) = rule
    for {
      _ <- Either.cond(
        eligibleSenders.distinct.sizeIs >= threshold.unwrap,
        (),
        s"Threshold $threshold cannot be reached",
      )
      _ <- Either.cond(
        eligibleSenders.contains(sender),
        (),
        "Sender is not eligible according to the aggregation rule",
      )
    } yield ()
  }

  /** An util to reject requests that try to send something to multiple mediators (mediator groups).
    * Mediators/groups are identified by their [[com.digitalasset.canton.topology.MemberCode]]
    */
  def checkToAtMostOneMediator(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.batch.allMediatorRecipients.sizeCompare(1) <= 0
}
