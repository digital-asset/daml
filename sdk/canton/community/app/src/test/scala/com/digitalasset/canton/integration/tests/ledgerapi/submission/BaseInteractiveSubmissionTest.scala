// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitResponse,
  PrepareSubmissionResponse,
}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
  WildcardFilter,
}
import com.daml.ledger.javaapi.data.codegen.{Created, HasCommands, Update}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest.{
  ParticipantSelector,
  defaultConfirmingParticipant,
  defaultExecutingParticipant,
  defaultPreparingParticipant,
}
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.interactive.ExternalPartyUtils
import com.digitalasset.canton.interactive.ExternalPartyUtils.{
  ExternalParty,
  OnboardingTransactions,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLogging}
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.{
  ForceFlags,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Suite

import java.time.{Duration, Instant}
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

  protected val ppn = defaultPreparingParticipant
  protected val cpn = defaultConfirmingParticipant
  protected val epn = defaultExecutingParticipant

  protected val defaultTransactionFormat = TransactionFormat(
    eventFormat = Some(
      EventFormat(
        filtersByParty = Map.empty,
        filtersForAnyParty = Some(
          Filters(
            Seq(
              CumulativeFilter(
                CumulativeFilter.IdentifierFilter.WildcardFilter(WildcardFilter(true))
              )
            )
          )
        ),
        verbose = true,
      )
    ),
    transactionShape = TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS,
  )

  protected def exportPrivateKeys(
      externalParty: ExternalParty
  ): NonEmpty[Seq[PrivateKey]] =
    externalParty.signingFingerprints.map(
      crypto.cryptoPrivateStore.toExtended.value.exportPrivateKey(_).futureValueUS.value.value
    )

  protected def exportNamespaceKey(
      externalParty: ExternalParty
  ): PrivateKey =
    crypto.cryptoPrivateStore.toExtended.value
      .exportPrivateKey(externalParty.partyId.uid.namespace.fingerprint)
      .futureValueUS
      .value
      .value

  protected def importExternalPartyPrivateKeys(privateKeys: NonEmpty[Seq[PrivateKey]]): Unit =
    privateKeys.foreach(
      crypto.cryptoPrivateStore.toExtended.value.storePrivateKey(_, None).futureValueUS.value
    )

  val enableInteractiveSubmissionTransforms: Seq[ConfigTransform] = Seq(
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.ledgerApi.interactiveSubmissionService.enableVerboseHashing)
        .replace(true)
    ),
    ConfigTransforms.updateAllParticipantConfigs_(
      _.focus(_.topology.broadcastBatchSize).replace(PositiveInt.one)
    ),
  )

  def waitForExternalPartyToBecomeEffective(
      party: ExternalParty,
      nodesToSynchronize: InstanceReference*
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    nodesToSynchronize.foreach { p =>
      eventually() {
        p.topology.party_to_participant_mappings
          .list(daId, filterParty = party.partyId.filterString) should not be empty
        p.topology.party_to_key_mappings
          .list(daId, filterParty = party.partyId.filterString) should not be empty
      }
    }
  }

  def onboardParty(
      name: String,
      confirming: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      extraConfirming: Seq[ParticipantReference] = Seq.empty,
      observing: Seq[ParticipantReference] = Seq.empty,
      confirmationThreshold: PositiveInt = PositiveInt.one,
      numberOfKeys: PositiveInt = PositiveInt.one,
      keyThreshold: PositiveInt = PositiveInt.one,
  )(implicit env: TestConsoleEnvironment): ExternalParty = {

    val (onboardingTransactions, externalParty) = generateExternalPartyOnboardingTransactions(
      name,
      Seq(confirming.id) ++ extraConfirming.map(_.id),
      observing.map(_.id),
      confirmationThreshold,
      numberOfKeys,
      keyThreshold,
    )

    loadOnboardingTransactions(
      externalParty,
      confirming,
      synchronizerId,
      onboardingTransactions,
      extraConfirming,
      observing,
    )
    externalParty
  }

  def loadOnboardingTransactions(
      externalParty: ExternalParty,
      confirming: ParticipantReference,
      synchronizerId: PhysicalSynchronizerId,
      onboardingTransactions: OnboardingTransactions,
      extraConfirming: Seq[ParticipantReference] = Seq.empty,
      observing: Seq[ParticipantReference] = Seq.empty,
  )(implicit env: TestConsoleEnvironment) = {
    // Start by loading the transactions signed by the party
    confirming.topology.transactions.load(
      Seq(
        onboardingTransactions.namespaceDelegation,
        onboardingTransactions.partyToKeyMapping,
        onboardingTransactions.partyToParticipant,
      ),
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
      // If the test runs with static time move the time up a bit so that the proposal becomes effective in the topology
      if (env.environment.clock.isSimClock)
        env.environment.simClock.value.advance(Duration.ofSeconds(1))
      // In practice, participant operators are expected to inspect the transaction here before authorizing it
      val transactionHash = partyToParticipantProposal.context.transactionHash
      hp.topology.transactions.authorize[PartyToParticipant](
        TxHash(Hash.fromByteString(transactionHash).value),
        mustBeFullyAuthorized = false,
        store = synchronizerId,
      )
    }

    if (env.environment.clock.isSimClock)
      env.environment.simClock.value.advance(Duration.ofSeconds(1))
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

  def updateSigningKeysThreshold(
      participant: ParticipantReference,
      party: PartyId,
      newThreshold: PositiveInt,
      regenerateKeys: Option[PositiveInt] = None,
  )(implicit env: TestConsoleEnvironment): NonEmpty[Seq[SigningPublicKey]] = {
    val store = env.synchronizer1Id
    val current = participant.topology.party_to_key_mappings
      .list(store, filterParty = party.toProtoPrimitive)
      .loneElement
    val keys = regenerateKeys.map(generateProtocolSigningKeys).getOrElse(current.item.signingKeys)
    val updatedTransaction = TopologyTransaction(
      TopologyChangeOp.Replace,
      current.context.serial.increment,
      PartyToKeyMapping.create(current.item.party, newThreshold, keys).value,
      testedProtocolVersion,
    )
    val namespaceSignature = crypto.privateCrypto
      .sign(
        updatedTransaction.hash.hash,
        party.fingerprint,
        NonEmpty.mk(Set, SigningKeyUsage.Namespace),
      )
      .futureValueUS
      .value
    // If we regenerate the keys we need to re-sign the party to key with the new keys
    val protocolSignatures = if (regenerateKeys.isDefined) {
      keys.map { key =>
        crypto.privateCrypto
          .sign(
            updatedTransaction.hash.hash,
            key.fingerprint,
            NonEmpty.mk(Set, SigningKeyUsage.Protocol),
          )
          .futureValueUS
          .value
      }.forgetNE
    } else Seq.empty
    val signedTopologyTransaction = SignedTopologyTransaction.tryCreate(
      updatedTransaction,
      NonEmpty
        .mk(Set, namespaceSignature, protocolSignatures*)
        .map(SingleTransactionSignature(updatedTransaction.hash, _)),
      isProposal = false,
      protocolVersion = testedProtocolVersion,
    )
    participant.topology.transactions.load(
      Seq(signedTopologyTransaction),
      store,
      synchronize = Some(NonNegativeDuration.ofSeconds(10)),
    )
    keys
  }

  def createCycleContract(
      partyE: ExternalParty
  )(implicit env: TestConsoleEnvironment): CreatedEvent =
    externalSubmit(
      new Cycle("test-external-signing", partyE.primitiveId).create(),
      partyE,
      epn(env),
    ).events.loneElement.getCreated

  def offboardParty(
      party: PartyId,
      participant: LocalParticipantReference,
      synchronizerId: SynchronizerId,
  ): Unit = {
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
    val removeCharlieSignedTopologyTx = signTopologyTransaction(party, removeTopologyTx)
    participant.topology.transactions.load(
      Seq(removeCharlieSignedTopologyTx),
      TopologyStoreId.Synchronizer(synchronizerId),
      forceFlags = ForceFlags(DisablePartyWithActiveContracts),
    )
  }

  def signTxAs(
      prep: PrepareSubmissionResponse,
      p: ExternalParty,
  ): Map[PartyId, Seq[Signature]] =
    signTxAs(prep.preparedTransactionHash, p)

  def externalSubmit(
      hasCommands: HasCommands,
      as: ExternalParty,
      executingParticipant: LocalParticipantReference,
      transactionFormat: Option[TransactionFormat] = Some(defaultTransactionFormat),
      prepareParticipantOverride: Option[LocalParticipantReference] = None,
      commandId: String = UUID.randomUUID().toString,
      disclosedContracts: Seq[DisclosedContract] = Seq.empty,
      // TODO(i26426): Remove when we move to 3.4
      expectTransactionHashOnLAPI: Boolean = true,
  )(implicit env: TestConsoleEnvironment): Transaction = {
    val prepare = prepareCommand(
      as,
      protoCmd(hasCommands),
      preparingParticipant = _ => prepareParticipantOverride.getOrElse(executingParticipant),
      commandId = commandId,
      disclosedContracts = disclosedContracts,
    )
    val transaction = execAndWaitForTransaction(
      prepare,
      signTxAs(prepare, as),
      execParticipant = _ => executingParticipant,
      transactionFormat = transactionFormat,
    )
    if (expectTransactionHashOnLAPI)
      transaction.externalTransactionHash shouldBe Some(prepare.preparedTransactionHash)
    transaction
  }

  def findContractForUpdateId(
      party: PartyId,
      updateId: String,
      cpn: LocalParticipantReference,
      requireBlob: Option[TemplateId] = None,
  ): CreatedEvent = {
    val tx = eventually() {
      cpn.ledger_api.updates
        .update_by_id(updateId, getUpdateFormat(Set(party)))
        .collect { case tx: TransactionWrapper => tx.transaction }
        .value
    }
    // By default, the transaction returned above does not have the create-blob
    // so use the contract id to look up the created event, with blob
    val contractId = tx.events.loneElement.getCreated.contractId
    getCreatedEvent(contractId, party, cpn, requireBlob)
  }

  protected def getCreatedEvent(
      contractId: String,
      partyId: PartyId,
      participant: ParticipantReference,
      requireBlob: Option[TemplateId] = None,
  ): CreatedEvent = requireBlob
    .map { templateId =>
      participant.ledger_api.state.acs
        .active_contracts_of_party(
          partyId,
          filterTemplates = Seq(templateId),
          includeCreatedEventBlob = true,
        )
        .flatMap(_.createdEvent)
        .filter(_.contractId == contractId)
        .loneElement
    }
    .getOrElse {
      participant.ledger_api.event_query
        .by_contract_id(contractId, Seq(partyId))
        .getCreated
        .getCreatedEvent
    }

  def prepareCommand(
      as: ExternalParty,
      command: Command,
      disclosedContracts: Seq[DisclosedContract] = Seq.empty,
      minLedgerTimeAbs: Option[Instant] = None,
      preparingParticipant: ParticipantSelector = defaultPreparingParticipant,
      commandId: String = UUID.randomUUID().toString,
  )(implicit env: TestConsoleEnvironment): PrepareSubmissionResponse =
    preparingParticipant(env).ledger_api.interactive_submission.prepare(
      Seq(as.partyId),
      Seq(command),
      disclosedContracts = disclosedContracts,
      minLedgerTimeAbs = minLedgerTimeAbs,
      synchronizerId = None,
      verboseHashing = true,
      commandId = commandId,
    )

  def prepareCycle(as: ExternalParty)(implicit
      env: TestConsoleEnvironment
  ): PrepareSubmissionResponse =
    prepareCommand(as, protoCreateCycleCmd(as))

  def exec(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      execParticipant: ParticipantSelector = defaultExecutingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): (String, Long) = {
    val submissionId = UUID.randomUUID().toString
    val ledgerEnd = execParticipant(env).ledger_api.state.end()
    execParticipant(env).ledger_api.interactive_submission.execute(
      prepared.preparedTransaction.value,
      signatures,
      submissionId,
      prepared.hashingSchemeVersion,
    )
    (submissionId, ledgerEnd)
  }

  def execAndWait(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      execParticipant: ParticipantSelector = defaultExecutingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): ExecuteSubmissionAndWaitResponse = {
    val submissionId = UUID.randomUUID().toString
    execParticipant(env).ledger_api.interactive_submission.executeAndWait(
      prepared.preparedTransaction.value,
      signatures,
      submissionId,
      prepared.hashingSchemeVersion,
    )
  }

  def execAndWaitForTransaction(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      transactionFormat: Option[TransactionFormat] = None,
      execParticipant: ParticipantSelector = defaultExecutingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): Transaction = {
    val submissionId = UUID.randomUUID().toString
    execParticipant(env).ledger_api.interactive_submission
      .executeAndWaitForTransaction(
        prepared.preparedTransaction.value,
        signatures,
        submissionId,
        prepared.hashingSchemeVersion,
        transactionFormat,
      )
  }

  def execFailure(
      prepared: PrepareSubmissionResponse,
      signatures: Map[PartyId, Seq[Signature]],
      additionalExpectedFailure: String = "",
  )(implicit
      env: TestConsoleEnvironment
  ): Unit =
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      a[CommandFailure] shouldBe thrownBy {
        exec(prepared, signatures)
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

  def findCompletion(
      submissionId: String,
      ledgerEnd: Long,
      observingPartyE: ExternalParty,
      execParticipant: ParticipantSelector = defaultExecutingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): Completion =
    eventually() {
      val completions =
        execParticipant(env).ledger_api.completions.list(
          observingPartyE.partyId,
          atLeastNumCompletions = 1,
          ledgerEnd,
        )
      completions.find(_.submissionId == submissionId).value
    }

  def findTransactionByUpdateId(
      observingPartyE: ExternalParty,
      updateId: String,
      confirmingParticipant: ParticipantSelector = defaultConfirmingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): Transaction = {
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
                    verbose = false,
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

  def findTransactionInStream(
      observingPartyE: ExternalParty,
      beginOffset: Long = 0L,
      hash: ByteString,
      confirmingParticipant: ParticipantSelector = defaultConfirmingParticipant,
  )(implicit
      env: TestConsoleEnvironment
  ): Transaction = {
    val transactions =
      confirmingParticipant(env).ledger_api.updates.transactions(
        Set(observingPartyE.partyId),
        completeAfter = PositiveInt.one,
        beginOffsetExclusive = beginOffset,
      )

    clue("find a transaction in a stream with the external hash") {
      transactions
        .find(_.transaction.externalTransactionHash.contains(hash))
        .map(_.transaction)
        .value
    }
  }

  def protoCmd(hasCommands: HasCommands): Command =
    Command.fromJavaProto(hasCommands.commands.loneElement.toProtoCommand)

  def protoCreateCycleCmd(ownerE: ExternalParty): Command =
    protoCmd(createCycleCommand(ownerE))

  def createCycleCommand(ownerE: ExternalParty): Update[Created[Cycle.ContractId]] =
    new Cycle("test-external-signing", ownerE.primitiveId).create()

}
