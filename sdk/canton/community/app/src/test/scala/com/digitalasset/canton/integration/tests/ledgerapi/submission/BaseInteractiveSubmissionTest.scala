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
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.OnboardingTransactions
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
import com.digitalasset.canton.interactive.ExternalPartyUtils
import com.digitalasset.canton.logging.{LogEntry, NamedLogging}
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.{
  ExternalParty,
  ForceFlags,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Suite

import java.util.UUID
import scala.concurrent.ExecutionContext

object BaseInteractiveSubmissionTest {
  type ParticipantSelector = TestConsoleEnvironment => LocalParticipantReference
  type ParticipantsSelector = TestConsoleEnvironment => Seq[LocalParticipantReference]
  val defaultPreparingParticipant: ParticipantSelector = _.participant1
  val defaultExecutingParticipant: ParticipantSelector = _.participant2
  val defaultConfirmingParticipant: ParticipantSelector = _.participant3
}

trait BaseInteractiveSubmissionTest
    extends ExternalPartyUtils
    with BaseTest
    with HasExecutionContext {

  this: Suite & NamedLogging =>

  override val externalPartyExecutionContext: ExecutionContext = parallelExecutionContext

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

  protected def loadOnboardingTransactions(
      externalParty: ExternalParty,
      confirming: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      onboardingTransactions: OnboardingTransactions,
      extraConfirming: Seq[ParticipantReference] = Seq.empty,
      observing: Seq[ParticipantReference] = Seq.empty,
  )(implicit env: TestConsoleEnvironment): Unit = {
    // Start by loading the transactions signed by the party
    confirming.topology.transactions.load(
      onboardingTransactions.toSeq,
      store = synchronizerId,
    )

    val partyId = externalParty.partyId
    val allParticipants = Seq(confirming) ++ extraConfirming ++ observing

    // Then each hosting participant must sign and load the PartyToParticipant transaction
    allParticipants.map { hp =>
      // Eventually because it could take some time before the transaction makes it to all participants
      val partyToParticipantProposal = eventually() {
        hp.topology.party_to_participant_mappings
          .list(
            synchronizerId,
            proposals = true,
            filterParty = partyId.toProtoPrimitive,
          )
          .loneElement
      }

      // In practice, participant operators are expected to inspect the transaction here before authorizing it
      val transactionHash = partyToParticipantProposal.context.transactionHash
      hp.topology.transactions.authorize[PartyToParticipant](
        TxHash(Hash.fromByteString(transactionHash).value),
        mustBeFullyAuthorized = false,
        store = synchronizerId,
      )
    }

    allParticipants.foreach { hp =>
      // Wait until all participants agree the hosting is effective
      env.utils.retry_until_true(
        hp.topology.party_to_participant_mappings
          .list(
            synchronizerId,
            filterParty = partyId.toProtoPrimitive,
          )
          .nonEmpty
      )
    }
  }

  // TODO(#27680) Extract into PartyToParticipantDeclarative
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
      TopologyStoreId.Synchronizer(synchronizerId),
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
