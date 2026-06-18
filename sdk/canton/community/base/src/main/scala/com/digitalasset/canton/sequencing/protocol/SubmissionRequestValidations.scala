// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

object SubmissionRequestValidations {
  def checkSenderAndRecipientsAreRegistered(
      submission: SubmissionRequest,
      snapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, MemberCheckError, Unit] =
    EitherT {
      val senders =
        submission.aggregationRule.fold(Set.empty[Member])(
          _.eligibleSenders.toSet
        ) incl submission.sender
      val allRecipients = submission.batch.allMembers

      // TODO(#19476): Why we don't check group recipients here?
      val allMembers = allRecipients ++ senders

      for {
        registeredMembers <- snapshot.areMembersKnown(allMembers)
      } yield {
        Either.cond(
          registeredMembers.sizeCompare(allMembers) == 0,
          (), {
            val unregisteredRecipients = allRecipients.diff(registeredMembers)
            val unregisteredSenders = senders.diff(registeredMembers)
            MemberCheckError(unregisteredRecipients, unregisteredSenders)
          },
        )
      }
    }

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
        s"Sender [$sender] is not eligible according to the aggregation rule",
      )
    } yield ()
  }

  /** A utility function to reject requests that try to send something to multiple mediators
    * (mediator groups). Mediators/groups are identified by their
    * [[com.digitalasset.canton.topology.MemberCode]]
    */
  def checkToAtMostOneMediator(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.batch.allMediatorRecipients.sizeIs <= 1

  final case class MemberCheckError(
      unregisteredRecipients: Set[Member],
      unregisteredSenders: Set[Member],
  ) {
    def toSequencerDeliverError: SequencerDeliverError =
      if (unregisteredRecipients.nonEmpty)
        SequencerErrors.UnknownRecipients(unregisteredRecipients.toSeq)
      else SequencerErrors.SenderUnknown(unregisteredSenders.toSeq)
  }
}
