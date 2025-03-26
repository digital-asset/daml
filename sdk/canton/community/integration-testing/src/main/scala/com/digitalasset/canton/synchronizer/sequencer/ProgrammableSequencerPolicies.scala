// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  MemberRecipient,
  SequencersOfSynchronizer,
  SubmissionRequest,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Promise
import scala.util.chaining.*

object ProgrammableSequencerPolicies {
  /*
    Delay the specified messages by some duration.
   */
  def delay(environment: Environment)(
      confirmationResponses: Map[ParticipantId, NonNegativeFiniteDuration] = Map.empty,
      mediatorMessages: Option[NonNegativeFiniteDuration] = None,
  ): SendPolicy = {

    /*
      To avoid interference between `Clock.uniqueTime` and time progressing,
      we truncate to the second (i.e., we forget micro and milli seconds).
     */
    def advanceClockBy(d: NonNegativeFiniteDuration)(implicit traceContext: TraceContext): Unit = {
      val simClock = environment.simClock.getOrElse(
        throw new IllegalArgumentException("SimClock should be used")
      )

      simClock.advanceTo(
        CantonTimestamp.assertFromInstant(
          (simClock.now + d).toInstant.truncatedTo(ChronoUnit.SECONDS)
        )
      )
    }

    val logger = environment.loggerFactory.getLogger(getClass)

    SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
      submissionRequest.sender match {
        case mediatorId: MediatorId =>
          mediatorMessages
            .filter(_ > NonNegativeFiniteDuration.Zero)
            .tap(duration =>
              logger.info(
                s"Progressing time by ${duration.getOrElse(NonNegativeFiniteDuration.Zero)} for message sent by mediator `$mediatorId`"
              )
            )
            .foreach(advanceClockBy)

        case id: ParticipantId if isConfirmationResponse(submissionRequest) =>
          confirmationResponses
            .get(id)
            .filter(_ > NonNegativeFiniteDuration.Zero)
            .tap(duration =>
              logger.info(
                s"Progressing time by ${duration.getOrElse(NonNegativeFiniteDuration.Zero)} for confirmation response sent by `$id`"
              )
            )
            .foreach(advanceClockBy)

        case _ => ()
      }

      SendDecision.Process
    }
  }

  /*
    We want two participants to concurrently submit a command that refers to the same contract.

    If we denote by P1 (hosting signatory) and P2 (hosting the observers) the two participants,
    we want the following situation:

         submit      3                            7
    P1 ----|-------|---|-----------------------|-----|-----
    P2 ----------------------|-------|---|---------|-----|-
                          submit       3              7

   Here, P1 will see a command completion and P2 a command rejected (because the contract is locked).

   Parameters:
     - mediatorId: id of the mediator of the synchronizer
     - submission1Phase3Done: Promise which will be completed when the phase 3 of the first submission
                              is done. Should be used to delay the second submission.
   */
  def lockedContract(submission1Phase3Done: Promise[Unit]): SendPolicy = {

    val submission2Phase3Done = Promise[Unit]() // Completed when phase 3 for P2 submission is done

    val confirmationResponsesCount = new AtomicLong(0)

    SendPolicy.processTimeProofs { _ => submissionRequest =>
      val isMediatorResult = ProgrammableSequencerPolicies.isMediatorResult(submissionRequest)

      submissionRequest.sender match {
        case _: MediatorId =>
          if (isMediatorResult) {
            // Wait until the phase 3 for the second submission is done
            SendDecision.HoldBack(submission2Phase3Done.future)
          } else
            SendDecision.Process

        case _: ParticipantId =>
          if (isConfirmationResponse(submissionRequest)) {
            val newConfirmationResponsesCount = confirmationResponsesCount.incrementAndGet()

            // After one confirmation, phase 3 of first submission is considered as done
            if (newConfirmationResponsesCount == 1)
              submission1Phase3Done.trySuccess(())
            // After two confirmations sent, phase 3 of second submission is considered as done
            else if (newConfirmationResponsesCount == 2)
              submission2Phase3Done.trySuccess(())
          }

          SendDecision.Process

        case _ => SendDecision.Process
      }
    }
  }

  def isConfirmationResponse(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.batch.envelopes.nonEmpty && submissionRequest.batch.envelopes.forall {
      envelope =>
        val allRecipients = envelope.recipients.allRecipients
        allRecipients.sizeIs == 1 && allRecipients.forall {
          case MediatorGroupRecipient(_) => true
          case _ => false
        }
    }

  def isMediatorResult(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.batch.envelopes.nonEmpty && submissionRequest.sender.code == MediatorId.Code

  def isConfirmationResult(submissionRequest: SubmissionRequest, mediator: Member): Boolean =
    submissionRequest.batch.envelopes.nonEmpty && submissionRequest.sender == mediator

  def isAcsCommitment(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.sender.code == ParticipantId.Code && submissionRequest.batch.envelopes.nonEmpty &&
      submissionRequest.batch.envelopes.forall { envelope =>
        val recipients = envelope.recipients.allRecipients
        recipients.sizeIs == 1 && recipients.forall {
          case MemberRecipient(_: ParticipantId) => true
          case _ => false
        }
      }

  def isTopUpBalance(submissionRequest: SubmissionRequest): Boolean =
    submissionRequest.sender.code == SequencerId.Code && submissionRequest.aggregationRule.isDefined &&
      submissionRequest.batch.envelopes.nonEmpty && submissionRequest.batch.envelopes.forall {
        envelope =>
          val allRecipients = envelope.recipients.allRecipients
          allRecipients.sizeIs == 2 && allRecipients.contains(SequencersOfSynchronizer)
      }
}
