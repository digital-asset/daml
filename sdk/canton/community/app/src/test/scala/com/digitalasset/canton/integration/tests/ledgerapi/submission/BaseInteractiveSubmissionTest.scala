// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitResponse,
  PrepareSubmissionResponse,
}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
  WildcardFilter,
}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.{
  ParticipantSelector,
  defaultConfirmingParticipant,
  defaultExecutingParticipant,
  defaultPreparingParticipant,
}
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLogging}
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{ExternalParty, ForceFlags, PartyId, SynchronizerId}
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Suite

import java.util.UUID

object BaseInteractiveSubmissionTest {
  type ParticipantSelector = TestConsoleEnvironment => LocalParticipantReference
  type ParticipantsSelector = TestConsoleEnvironment => Seq[LocalParticipantReference]
  val defaultPreparingParticipant: ParticipantSelector = _.participant1
  val defaultExecutingParticipant: ParticipantSelector = _.participant2
  val defaultConfirmingParticipant: ParticipantSelector = _.participant3
}

trait BaseInteractiveSubmissionTest extends BaseTest {

  this: Suite & NamedLogging =>

  protected def ppn(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    defaultPreparingParticipant(env)
  protected def cpn(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    defaultConfirmingParticipant(env)
  protected def epn(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    defaultExecutingParticipant(env)

  protected val enableInteractiveSubmissionTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.ledgerApi.interactiveSubmissionService.enableVerboseHashing)
        .replace(true)
    ),
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.topology.broadcastBatchSize).replace(PositiveInt.one)
    ),
  )

  protected def offboardParty(
      party: ExternalParty,
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val partyToParticipantTx = participant.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = party.toProtoPrimitive)
      .loneElement
    val partyToParticipantMapping = partyToParticipantTx.item
    val removeTopologyTx = TopologyTransaction(
      TopologyChangeOp.Remove,
      partyToParticipantTx.context.serial.increment,
      partyToParticipantMapping,
      testedProtocolVersion,
    )

    val removeCharlieSignedTopologyTx =
      global_secret.sign(removeTopologyTx, party, testedProtocolVersion)
    participant.topology.transactions.load(
      Seq(removeCharlieSignedTopologyTx),
      synchronizerId,
      forceFlags = ForceFlags(DisablePartyWithActiveContracts),
    )
  }

  protected def exec(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      execParticipant: ParticipantReference,
  ): (String, Long) = {
    val submissionId = UUID.randomUUID().toString
    val ledgerEnd = execParticipant.ledger_api.state.end()
    execParticipant.ledger_api.interactive_submission.execute(
      prepared.preparedTransaction.value,
      signatures,
      submissionId,
      prepared.hashingSchemeVersion,
    )
    (submissionId, ledgerEnd)
  }

  protected def execAndWait(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      execParticipant: ParticipantSelector = defaultExecutingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): ExecuteSubmissionAndWaitResponse = {
    val submissionId = UUID.randomUUID().toString
    execParticipant(env).ledger_api.interactive_submission.execute_and_wait(
      prepared.preparedTransaction.value,
      signatures,
      submissionId,
      prepared.hashingSchemeVersion,
    )
  }

  protected def execAndWaitForTransaction(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      transactionShape: TransactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      execParticipant: ParticipantSelector = defaultExecutingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): Transaction = {
    val submissionId = UUID.randomUUID().toString
    execParticipant(env).ledger_api.interactive_submission
      .execute_and_wait_for_transaction(
        prepared.preparedTransaction.value,
        signatures,
        submissionId,
        prepared.hashingSchemeVersion,
        Some(transactionShape),
      )
  }

  protected def execFailure(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      additionalExpectedFailure: String = "",
  )(implicit
      env: TestConsoleEnvironment
  ): Unit =
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      a[CommandFailure] shouldBe thrownBy {
        exec(prepared, signatures, epn)
      },
      LogEntry.assertLogSeq(
        Seq(
          (
            m =>
              m.errorMessage should (include(
                "The participant failed to execute the transaction"
              ) and include(additionalExpectedFailure)),
            "invalid signature",
          )
        ),
        Seq.empty,
      ),
    )

  protected def findCompletion(
      submissionId: String,
      ledgerEnd: Long,
      observingPartyE: ExternalParty,
      execParticipant: ParticipantReference,
  ): Completion =
    eventually() {
      execParticipant.ledger_api.completions
        .list(
          observingPartyE.partyId,
          atLeastNumCompletions = 1,
          ledgerEnd,
          filter = _.submissionId == submissionId,
        )
        .loneElement
    }

  protected def findTransactionByUpdateId(
      observingPartyE: ExternalParty,
      updateId: String,
      verbose: Boolean = false,
      confirmingParticipant: ParticipantSelector = defaultConfirmingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): Transaction = eventually() {
    val update: UpdateService.UpdateWrapper =
      confirmingParticipant(env).ledger_api.updates
        .update_by_id(
          updateId,
          UpdateFormat(
            includeTransactions = Some(
              TransactionFormat(
                eventFormat = Some(
                  EventFormat(
                    filtersByParty = Map(
                      observingPartyE.partyId.toProtoPrimitive -> Filters(
                        Seq(
                          CumulativeFilter(
                            CumulativeFilter.IdentifierFilter
                              .WildcardFilter(WildcardFilter(includeCreatedEventBlob = false))
                          )
                        )
                      )
                    ),
                    filtersForAnyParty = None,
                    verbose = verbose,
                  )
                ),
                transactionShape = TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
              )
            ),
            None,
            None,
          ),
        )
        .value
    update match {
      case TransactionWrapper(transaction) => transaction
      case _ => fail("Expected transaction update")
    }
  }

  protected def findTransactionInStream(
      observingPartyE: ExternalParty,
      beginOffset: Long = 0L,
      hash: ByteString,
      confirmingParticipant: ParticipantReference,
  ): Transaction = {
    val transactions =
      confirmingParticipant.ledger_api.updates.transactions(
        Set(observingPartyE.partyId),
        completeAfter = PositiveInt.one,
        beginOffsetExclusive = beginOffset,
        resultFilter = {
          case TransactionWrapper(transaction)
              if transaction.externalTransactionHash.contains(hash) =>
            true
          case _ => false
        },
      )

    clue("find a transaction in a stream with the external hash") {
      transactions.headOption
        .map(_.transaction)
        .value
    }
  }
}
