// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.daml.ledger.api.v2 as proto
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.transaction_filter.Filters
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.UpdateWrapper
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{
  BaseTest,
  LedgerCommandId,
  LedgerSubmissionId,
  LedgerUserId,
  LfPartyId,
  LfWorkflowId,
}

import scala.concurrent.Future

private[integration] trait HasCommandRunnersHelpers {
  this: BaseTest =>

  /*
     Run a command and returns the result as well as the generated event and the completion.
     Expect exactly one event and one completion to be published.
   */
  protected def runCommand[T](
      cmd: => T,
      synchronizerId: SynchronizerId,
      userId: String,
      submittingParty: PartyId,
      participantOverride: Option[LocalParticipantReference] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): (T, UpdateWrapper, Completion) = {

    import env.*

    val participant = participantOverride.getOrElse(participant1)
    val ledgerEnd = participant.ledger_api.state.end()

    logger.debug(
      s"Retrieving completions stream for party $submittingParty hosted on ${participant.id} with ledger end $ledgerEnd"
    )

    val completionsF = Future {
      getCompletions(
        participant,
        submittingParty,
        ledgerEnd,
        filterBy =
          _.synchronizerTime.map(_.synchronizerId).getOrElse("") == synchronizerId.toProtoPrimitive,
        expected = 1,
        userId,
      )
    }

    val startOffset =
      participant.ledger_api.state.end()
    val res = cmd

    val update = eventually() {
      val endOffset =
        participant.ledger_api.state.end()

      logger.debug(
        s"Retrieving updates stream for party $submittingParty hosted on ${participant.id} with offsets $startOffset/$endOffset"
      )

      val updates = participant.ledger_api.updates.flat(
        partyIds = Set(submittingParty),
        completeAfter = 1,
        beginOffsetExclusive = startOffset,
        endOffsetInclusive = Some(endOffset),
        synchronizerFilter = Some(synchronizerId),
      )

      updates.size shouldBe 1
      updates.headOption.value
    }

    val completions = completionsF.futureValue
    completions.size shouldBe 1
    (res, update, completions.headOption.value)
  }

  protected def getCompletions(
      participant: LocalParticipantReference,
      submittingParty: PartyId,
      startExclusive: Long,
      filterBy: Completion => Boolean = _ => true,
      expected: Int = Int.MaxValue,
      userId: String = HasCommandRunnersHelpers.userId,
  ): Seq[Completion] =
    participant.ledger_api.completions
      .list(
        partyId = submittingParty,
        atLeastNumCompletions = expected,
        beginOffsetExclusive = startExclusive,
        filter = filterBy,
        userId = userId,
      )

  protected def getTransactionFilter(
      stakeholders: List[LfPartyId]
  ): proto.transaction_filter.TransactionFilter = {
    val noTemplateFilter = Filters(Seq.empty)

    proto.transaction_filter.TransactionFilter(
      stakeholders.map(party => party -> noTemplateFilter).toMap,
      None,
    )
  }

  /*
     Run a command and returns the result as well as the generated completion.
     Expect exactly one completion to be published.
   */
  protected def runFailingCommand[T](
      cmd: => T,
      synchronizerId: SynchronizerId,
      userId: String,
      submittingParty: PartyId,
      participantOverrideO: Option[LocalParticipantReference],
  )(implicit
      env: TestConsoleEnvironment
  ): Completion = {
    import env.*

    val participant = participantOverrideO.getOrElse(participant1)
    val ledgerEnd = participant.ledger_api.state.end()

    val completionsF = Future {
      getCompletions(
        participant,
        submittingParty,
        ledgerEnd,
        filterBy =
          _.synchronizerTime.map(_.synchronizerId).getOrElse("") == synchronizerId.toProtoPrimitive,
        userId = userId,
        expected = 1,
      )
    }

    logger.debug("Submitting failing command")
    cmd.discard

    logger.debug("Waiting for the failing command to complete")
    val completions = completionsF.futureValue
    completions.size shouldBe 1

    completions.headOption.value
  }

}

object HasCommandRunnersHelpers {

  lazy val userId: LedgerUserId =
    LedgerUserId.assertFromString("enterprise-user")

  lazy val workflowId: LfWorkflowId = LfWorkflowId.assertFromString("some-workflow")

  lazy val commandId: LedgerCommandId =
    LedgerCommandId.assertFromString("enterprise-reassignment-command-id")

  lazy val submissionId: Option[LedgerSubmissionId] =
    Some(LedgerSubmissionId.assertFromString("some-submission-id"))
}
