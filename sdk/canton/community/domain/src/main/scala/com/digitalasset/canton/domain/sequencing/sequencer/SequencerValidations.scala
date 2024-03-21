// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SubmissionRequest}
import com.digitalasset.canton.topology.{Member, ParticipantId}

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
    unregisteredRecipients = submission.batch.allMembers.toList.filterNot(isRegistered)
    _ <- Either.cond(
      unregisteredRecipients.isEmpty,
      (),
      SendAsyncError.UnknownRecipients(
        s"The following recipients are invalid: ${unregisteredRecipients.mkString(",")}"
      ): SendAsyncError,
    )
  } yield ()

  /** An util to reject requests from participants that try to send something to multiple mediators (mediator groups).
    * Mediators/groups are identified by their [[com.digitalasset.canton.topology.KeyOwnerCode]]
    */
  def checkFromParticipantToAtMostOneMediator(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.sender match {
      case ParticipantId(_) =>
        submissionRequest.batch.allMediatorRecipients.sizeCompare(1) <= 0
      case _ => true
    }
}
