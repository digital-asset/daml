// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  Metadata,
  PrepareSubmissionResponse,
  PreparedTransaction,
}
import com.daml.ledger.api.v2.interactive.transaction.v1.interactive_submission_data.Node.NodeType
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.api.v2.value.Value
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.javaapi.data.codegen.ContractId as CodeGenCID
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.damltests.java.test.DummyFactory
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.examples.java.trailingnone as T
import com.digitalasset.canton.examples.java.trailingnone.TrailingNone
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.interactive.ExternalPartyUtils.ExternalParty
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.google.protobuf.ByteString
import io.grpc.Status
import monocle.Optional
import monocle.macros.GenLens
import org.scalactic.source.Position
import org.slf4j.event.Level

import java.util.UUID

trait InteractiveSubmissionIntegrationTestSetup
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest {

  protected val preparedTxMetadataOpt: Optional[PreparedTransaction, Metadata] =
    Optional[PreparedTransaction, Metadata](_.metadata)(md => tx => tx.copy(metadata = Some(md)))

  protected val preparedSubmissionResponseOpt
      : Optional[PrepareSubmissionResponse, PreparedTransaction] =
    Optional[PrepareSubmissionResponse, PreparedTransaction](_.preparedTransaction)(tx =>
      res => res.copy(preparedTransaction = Some(tx))
    )

  protected val preparedTxResponseInputContractsOpt = preparedSubmissionResponseOpt
    .andThen(preparedTxMetadataOpt)
    .andThen(
      GenLens[Metadata](_.inputContracts)
    )

  protected var aliceE: ExternalParty = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        Seq(participant1, participant2, participant3).foreach { p =>
          p.dars.upload(CantonExamplesPath)
          p.dars.upload(CantonTestsPath)
          p.synchronizers.connect_local(sequencer1, alias = daName)
        }
        aliceE = onboardParty("Alice", cpn(env), env.synchronizer1Id)
        waitForExternalPartyToBecomeEffective(aliceE, ppn(env), cpn(env), env.sequencer1)
      }
      .addConfigTransforms(enableInteractiveSubmissionTransforms*)

  protected def createTrailingNoneContract(
      party: ExternalParty
  )(implicit env: FixtureParam): CreatedEvent =
    externalSubmit(
      TrailingNone
        .create(party.partyId.toProtoPrimitive, java.util.Optional.empty()),
      party,
      cpn(env),
    ).events.loneElement.getCreated
}

class InteractiveSubmissionIntegrationTest extends InteractiveSubmissionIntegrationTestSetup {
  def assertLabelsAndIdentifiersNonEmpty(value: Value): Unit =
    value.sum match {
      case Sum.Optional(value) =>
        value.value.foreach(assertLabelsAndIdentifiersNonEmpty)
      case Sum.List(value) =>
        value.elements.foreach(assertLabelsAndIdentifiersNonEmpty)
      case Sum.TextMap(value) =>
        value.entries.flatMap(_.value).foreach(assertLabelsAndIdentifiersNonEmpty)
      case Sum.GenMap(value) =>
        value.entries.flatMap(_.value).foreach(assertLabelsAndIdentifiersNonEmpty)
        value.entries.flatMap(_.key).foreach(assertLabelsAndIdentifiersNonEmpty)
      case Sum.Record(value) =>
        clue("record id should not be empty") {
          value.recordId should not be empty
        }
        clue("record labels should not be empty") {
          value.fields.foreach(_.label should not be empty)
        }
        value.fields.flatMap(_.value).foreach(assertLabelsAndIdentifiersNonEmpty)
      case Sum.Variant(value) =>
        clue("variant id should not be empty") {
          value.variantId should not be empty
        }
        value.value.foreach(assertLabelsAndIdentifiersNonEmpty)
      case Sum.Enum(value) =>
        clue("enum id should not be empty") {
          value.enumId should not be empty
        }
      case Sum.Empty =>
      case Sum.Unit(_) =>
      case Sum.Bool(_) =>
      case Sum.Int64(_) =>
      case Sum.Date(_) =>
      case Sum.Timestamp(_) =>
      case Sum.Numeric(_) =>
      case Sum.Party(_) =>
      case Sum.Text(_) =>
      case Sum.ContractId(_) =>
    }

  "Interactive submission" should {
    var danE: ExternalParty = null

    "onboard a new party with external keys" in { implicit env =>
      danE = onboardParty(
        "Dan",
        cpn(env),
        env.synchronizer1Id,
        numberOfKeys = PositiveInt.three,
        keyThreshold = PositiveInt.two,
      )
      waitForExternalPartyToBecomeEffective(danE, ppn(env), cpn(env), env.sequencer1)
    }

    "fail to submit when party is not hosted with confirmation permission on the synchronizer" in {
      implicit env =>
        val (onboardingTransactions, externalParty) = generateExternalPartyOnboardingTransactions(
          "nothosted",
          confirming = Seq.empty,
          // Host only with observation rights
          observing = Seq(cpn(env).id),
        )

        loadOnboardingTransactions(
          externalParty,
          cpn(env),
          env.synchronizer1Id,
          onboardingTransactions,
          Seq.empty,
          Seq(cpn(env)),
        )

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          createCycleContract(externalParty),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.errorMessage should include("No valid synchronizer for submission found"),
                "expected submission failure due party only hosted with observation permissions",
              )
            ),
            Seq.empty,
          ),
        )
    }

    "create a contract" in { implicit env =>
      createCycleContract(aliceE)
    }

    "create a transaction with multiple nodes" in { implicit env =>
      val createdEvent = externalSubmit(
        DummyFactory.create(aliceE.primitiveId),
        aliceE,
        epn(env),
      ).events.loneElement.getCreated
      val exerciseCommand = DummyFactory.ContractId
        .fromContractId(new CodeGenCID(createdEvent.contractId))
        .exerciseDummyFactoryCall()
      val exerciseTransaction = externalSubmit(
        exerciseCommand,
        aliceE,
        epn(env),
        // prepare on the cpn to avoid having to explicitly disclose the contract
        prepareParticipantOverride = Some(cpn(env)),
      )
      // One exercise, 2 creates
      exerciseTransaction.events.size shouldBe 3
    }

    // Failure case is tested in InteractiveSubmissionConfirmationIntegrationTest:
    // "fail execute and wait if the signatures are invalid"
    "get a completion for a successful externally signed transaction from submissionId" in {
      implicit env =>
        val prepared = prepareCycle(aliceE)
        val submissionId = UUID.randomUUID().toString
        val ledgerEnd = epn(env).ledger_api.state.end()
        epn(env).ledger_api.interactive_submission
          .execute(
            prepared.preparedTransaction.value,
            signTxAs(prepared, aliceE),
            submissionId,
            prepared.hashingSchemeVersion,
          )
          .discard
        val completion = findCompletion(submissionId, ledgerEnd, aliceE, epn)
        completion.status.map(_.code) shouldBe Some(io.grpc.Status.OK.getCode.value())
    }

    "return events from executeAndWaitForTransaction accordingly to format and hosting" in {
      implicit env =>
        forAll(
          Table(
            ("EPN = CPN", "Format", "Has events"),
            (true, None, true),
            // If the CPN != EPN, events won't show up with a default filter (ACS_DELTA) as the EPN does not host the party
            (
              false,
              None,
              false,
            ),
            (
              true,
              Some(defaultTransactionFormat.copy(transactionShape = TRANSACTION_SHAPE_ACS_DELTA)),
              true,
            ),
            (
              true,
              Some(
                defaultTransactionFormat.copy(transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS)
              ),
              true,
            ),
            // If the CPN != EPN, events won't show up with an ACS_DELTA filter as the EPN does not host the party
            (
              false,
              Some(defaultTransactionFormat.copy(transactionShape = TRANSACTION_SHAPE_ACS_DELTA)),
              false,
            ),
            (
              false,
              Some(
                defaultTransactionFormat.copy(transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS)
              ),
              true,
            ),
          )
        ) { case (epnIsCpn, format, expectsEvents) =>
          val prepared = prepareCycle(aliceE)
          val pn = if (epnIsCpn) cpn(env) else epn(env)
          val expectedSize = if (expectsEvents) 1L else 0L
          val transaction = pn.ledger_api.interactive_submission
            .executeAndWaitForTransaction(
              prepared.preparedTransaction.value,
              signTxAs(prepared, aliceE),
              UUID.randomUUID().toString,
              prepared.hashingSchemeVersion,
              transactionFormat = format,
            )
          transaction.events should have size expectedSize
        }
    }

    "get the transaction from the transaction stream" in { implicit env =>
      val prepared = prepareCycle(aliceE)
      val submissionId = UUID.randomUUID().toString
      val ledgerEnd = cpn(env).ledger_api.state.end()
      epn(env).ledger_api.interactive_submission
        .execute(
          prepared.preparedTransaction.value,
          signTxAs(prepared, aliceE),
          submissionId,
          prepared.hashingSchemeVersion,
        )
        .discard
      val transaction = findTransactionInStream(aliceE, ledgerEnd, prepared.preparedTransactionHash)
      transaction.externalTransactionHash shouldBe Some(prepared.preparedTransactionHash)
    }

    "get the transaction from the update ID" in { implicit env =>
      val prepared = prepareCycle(aliceE)
      val submissionId = UUID.randomUUID().toString
      val response = epn(env).ledger_api.interactive_submission.executeAndWait(
        prepared.preparedTransaction.value,
        signTxAs(prepared, aliceE),
        submissionId,
        prepared.hashingSchemeVersion,
      )
      val transaction = eventually() {
        findTransactionByUpdateId(aliceE, response.updateId)
      }
      transaction.externalTransactionHash shouldBe Some(prepared.preparedTransactionHash)
    }

    "prepare on p1, submit on p2, confirm on p3 with threshold > 1" in { implicit env =>
      externalSubmit(createCycleCommand(danE), danE, epn(env)).discard
    }

    "execute and wait if transaction succeeds" in { implicit env =>
      val prepared = prepareCycle(danE)
      val transactionSignatures = signTxAs(prepared, danE)
      val response = execAndWait(prepared, transactionSignatures)
      findContractForUpdateId(danE.partyId, response.updateId, cpn(env))
    }

    "execute and wait if transaction fails" in { implicit env =>
      val prepared = prepareCycle(aliceE)
      val transactionSignatures = signTxAs(prepared, danE)
      val badSignatures = Map(aliceE.partyId -> transactionSignatures(danE.partyId))
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        a[CommandFailure] shouldBe thrownBy {
          execAndWait(prepared, badSignatures)
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                "The participant failed to execute the transaction: Received 0 valid signatures"
              ),
              "expected invalid signatures error",
            )
          )
        ),
      )
    }

    "execute and wait for transaction if transaction fails" in { implicit env =>
      val prepared = prepareCycle(aliceE)
      val transactionSignatures = signTxAs(prepared, danE)
      val badSignatures = Map(aliceE.partyId -> transactionSignatures(danE.partyId))
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        a[CommandFailure] shouldBe thrownBy {
          execAndWaitForTransaction(prepared, badSignatures)
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                "The participant failed to execute the transaction: Received 0 valid signatures"
              ),
              "expected invalid signatures error",
            )
          )
        ),
      )
    }

    "support contracts with trailing None" in { implicit env =>
      // Create 2 contracts, one we'll do an exercise on with explicit disclosure (using a ppn different from cpn)
      // And the other we'll prepare on the cpn, which will test local contract store lookup
      val contract1 = createTrailingNoneContract(aliceE)
      val contract2 = createTrailingNoneContract(aliceE)

      // Now exercise the archiveMe choice with explicit disclosure using contract1
      val archiveCmd = new T.TrailingNone.ContractId(contract1.contractId)
        .exerciseArchiveMe(aliceE.primitiveId)

      externalSubmit(
        archiveCmd,
        aliceE,
        cpn(env),
        prepareParticipantOverride = Some(ppn(env)),
        disclosedContracts = Seq(
          DisclosedContract(
            contract1.templateId,
            contract1.contractId,
            contract1.createdEventBlob,
            env.daId.logical.toProtoPrimitive,
          )
        ),
      ).discard

      // And now with contract2, using the cpn to prepare as well so we don't need to explicitly disclose the contract
      val archiveCmd2 = new T.TrailingNone.ContractId(contract2.contractId)
        .exerciseArchiveMe(aliceE.primitiveId)

      externalSubmit(
        archiveCmd2,
        aliceE,
        cpn(env),
      ).discard
    }

    "create a contract with verbose hashing" in { implicit env =>
      val prepared = ppn(env).ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(protoCreateCycleCmd(aliceE)),
        synchronizerId = None,
        verboseHashing = true,
      )

      prepared.hashingDetails.value should not be "Verbose hashing is disabled on this participant. Contact the node administrator for more details."

      prepared.preparedTransaction.value.transaction.value.nodes
        .map(_.versionedNode.v1.value.nodeType)
        .collect {
          case NodeType.Create(value) =>
            value.argument.foreach(assertLabelsAndIdentifiersNonEmpty)
          case NodeType.Exercise(value) =>
            value.chosenValue.foreach(assertLabelsAndIdentifiersNonEmpty)
        }
      execAndWait(prepared, signTxAs(prepared, aliceE)).discard
    }

    "execute a choice on an existing contract via explicit disclosure" in { implicit env =>
      import env.*

      val createdEvent = createCycleContract(aliceE)

      // Exercise the Repeat choice
      val exerciseRepeatOnCycleContract = ledger_api_utils.exercise(
        "Repeat",
        Map.empty,
        createdEvent,
      )

      // Call the prepare endpoint - this gives us back a serialized transaction, and the hash to be signed
      val prepared = prepareCommand(
        aliceE,
        exerciseRepeatOnCycleContract,
        disclosedContracts = Seq(
          DisclosedContract(
            createdEvent.templateId,
            createdEvent.contractId,
            createdEvent.createdEventBlob,
            daId.logical.toProtoPrimitive,
          )
        ),
        preparingParticipant = ppn,
      )

      // Check that input contracts also have identifiers and labels
      prepared.preparedTransaction.value.metadata.value.inputContracts
        .map(_.contract.v1.value)
        .foreach { value =>
          value.argument.foreach(assertLabelsAndIdentifiersNonEmpty)
        }
      execAndWait(prepared, signTxAs(prepared, aliceE)).discard
    }

    "fail if signature is signed by non acting party" in { implicit env =>
      val prepared = prepareCycle(aliceE)
      val transactionSignatures = signTxAs(prepared, danE)
      val badSignatures = Map(aliceE.partyId -> transactionSignatures(danE.partyId))
      execFailure(prepared, badSignatures, "Received 0 valid signatures")
    }

    "fail in phase 1 if the number of signatures is under the threshold" in { implicit env =>
      val prepared = prepareCycle(danE)
      // Not enough because threshold is 2
      val singleSignature = crypto.privateCrypto
        .signBytes(
          prepared.preparedTransactionHash,
          danE.signingFingerprints.head1,
          SigningKeyUsage.ProtocolOnly,
        )
        .valueOrFailShutdown("Failed to sign transaction hash")
        .futureValue
      execFailure(
        prepared,
        Map(danE.partyId -> Seq(singleSignature)),
        s"Received 1 valid signatures (0 invalid), but expected at least 2 valid for ${danE.partyId}",
      )
    }

    "fail to execute if input contracts are missing" in { implicit env =>
      import env.*

      val createdEvent = createCycleContract(aliceE)

      // Exercise the Repeat choice
      val exerciseRepeatOnCycleContract = ledger_api_utils.exercise(
        "Repeat",
        Map.empty,
        createdEvent,
      )

      val prepared = prepareCommand(
        aliceE,
        exerciseRepeatOnCycleContract,
        disclosedContracts = Seq.empty,
        preparingParticipant = cpn,
      )

      // Remove input contracts from the prepared transaction
      val txWithoutInputContract =
        preparedTxResponseInputContractsOpt.replace(Seq.empty)(prepared)

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        {
          val (submissionId, ledgerEnd) =
            exec(txWithoutInputContract, signTxAs(prepared, aliceE), cpn)
          val completion = findCompletion(submissionId, ledgerEnd, aliceE, cpn)
          completion.status.map(_.code) shouldBe Some(3) // Code 3 = Command failure
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include("Missing input contracts"),
              "expected missing input contracts",
            )
          ),
          Seq.empty,
        ),
      )
    }

    "fail to prepare a transaction if input contracts are neither disclosed nor available in the PPN" in {
      implicit env =>
        import env.*

        val createdEvent = createCycleContract(aliceE)

        // Exercise the Repeat choice
        val exerciseRepeatOnCycleContract = ledger_api_utils.exercise(
          "Repeat",
          Map.empty,
          createdEvent,
        )

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          prepareCommand(
            aliceE,
            exerciseRepeatOnCycleContract,
            disclosedContracts = Seq.empty,
            // The PPN does not know about the newly created contract, so the prepare command fails
            preparingParticipant = ppn,
          ),
          LogEntry.assertLogSeq(
            Seq(
              (
                _.errorMessage should include("CONTRACT_NOT_FOUND"),
                "expected contract not found",
              )
            ),
            Seq.empty,
          ),
        )
    }

    "re-use local contracts from PPN if available" in { implicit env =>
      val createdEvent = createCycleContract(aliceE)

      val exerciseRepeatOnCycleContract =
        Cycle.ContractId.fromContractId(new CodeGenCID(createdEvent.contractId)).exerciseRepeat()
      externalSubmit(
        exerciseRepeatOnCycleContract,
        aliceE,
        epn(env),
        // Prepare on the cpn without disclosing the contract
        prepareParticipantOverride = Some(cpn(env)),
      ).discard
    }

    "fail to prepare a transaction if the preparing party is not authorized" in { implicit env =>
      import env.*

      val bob = cpn(env).parties.enable("Bob")
      // Find Alice cycle contract
      val aliceCycleContract =
        cpn(env).ledger_api.state.acs
          .find_generic(aliceE.partyId, _.templateId.entityName == "Cycle")

      val prepareExerciseOnAliceContract = ledger_api_utils.exercise(
        packageId = Cycle.PACKAGE_ID,
        module = "Cycle",
        template = "Cycle",
        choice = "Repeat",
        arguments = Map.empty[String, Any],
        contractId = aliceCycleContract.contractId,
      )

      // This should fail, bob should not be able to generate a transaction for which he's not authorized
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        a[CommandFailure] shouldBe thrownBy {
          cpn(env).ledger_api.interactive_submission.prepare(
            Seq(bob),
            Seq(prepareExerciseOnAliceContract),
            synchronizerId = Some(daId),
          )
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include("Request failed for participant3"),
              "prepare transaction from non authorized party",
            )
          )
        ),
      )

      // but alice should be able to
      cpn(env).ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(prepareExerciseOnAliceContract),
        synchronizerId = Some(daId),
      )
    }

    "fail synchronously for an invalid signature" in { implicit env =>
      import env.*

      val prepared = prepareCommand(aliceE, protoCreateCycleCmd(aliceE))

      val badSignature =
        crypto.privateCrypto
          .signBytes(
            ByteString.copyFromUtf8("gipfeli"),
            aliceE.signingFingerprints.head1,
            SigningKeyUsage.ProtocolOnly,
            crypto.privateCrypto.signingAlgorithmSpecs.default,
          )
          .valueOrFailShutdown("Failed to sign transaction hash")(
            executionContext,
            implicitly[Position],
          )
          .futureValue

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        a[CommandFailure] shouldBe thrownBy {
          exec(prepared, Map(aliceE.partyId -> Seq(badSignature)))
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                "The participant failed to execute the transaction: Received 0 valid signatures (1 invalid)"
              ),
              "invalid signature",
            )
          ),
          Seq.empty,
        ),
      )
    }

  }

}

class InteractiveSubmissionIntegrationTestTimeouts
    extends InteractiveSubmissionIntegrationTestSetup {
  "timeout if CPN does not respond" in { implicit env =>
    // Reduce timeouts so this test completes faster.
    env.sequencer1.topology.synchronizer_parameters.propose_update(
      env.sequencer1.synchronizer_id,
      _.update(
        confirmationResponseTimeout = NonNegativeFiniteDuration.ofSeconds(2),
        mediatorReactionTimeout = NonNegativeFiniteDuration.ofSeconds(2),
      ),
    )

    cpn(env).synchronizers.disconnect_all()
    val prepared = prepareCommand(aliceE, protoCreateCycleCmd(aliceE))
    val signatures = signTxAs(prepared, aliceE)
    val (submissionId, ledgerEnd) = exec(prepared, signatures)
    val completion = findCompletion(
      submissionId,
      ledgerEnd,
      aliceE,
      epn,
    )
    completion.status.value.code shouldBe Status.Code.ABORTED.value()
    completion.status.value.message should include("MEDIATOR_SAYS_TX_TIMED_OUT")
    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
      cpn(env).synchronizers.reconnect_all(),
      LogEntry.assertLogSeq(
        Seq(
          (
            _.warningMessage should (include("Response message for request") and include(
              "timed out"
            )),
            "expected timed out message",
          )
        )
      ),
    )
  }
}

/** Test suite for tests that specifically need persistence (for instance to test behavior after
  * node restart)
  */
class InteractiveSubmissionIntegrationTestPostgres
    extends InteractiveSubmissionIntegrationTestSetup {
  registerPlugin(new UsePostgres(loggerFactory))

  "provide transaction hash in transaction stream from DB" in { implicit env =>
    val prepared = prepareCommand(aliceE, protoCreateCycleCmd(aliceE))
    val signatures = signTxAs(prepared, aliceE)
    val cpnLedgerEnd = cpn(env).ledger_api.state.end()
    val (submissionId, epnLedgerEnd) = exec(prepared, signatures)
    val completionStatus = findCompletion(submissionId, epnLedgerEnd, aliceE).status.value
    completionStatus.code shouldBe Status.Code.OK.value()
    cpn(env).stop()
    cpn(env).start()
    cpn(env).synchronizers.reconnect_all()
    findTransactionInStream(aliceE, cpnLedgerEnd, prepared.preparedTransactionHash, cpn)
  }
}
