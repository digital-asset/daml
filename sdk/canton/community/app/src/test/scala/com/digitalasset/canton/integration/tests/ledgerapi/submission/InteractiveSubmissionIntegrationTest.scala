// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  HashingSchemeVersion,
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
import com.daml.ledger.javaapi.data.DisclosedContract
import com.daml.ledger.javaapi.data.codegen.ContractId as CodeGenCID
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.console.commands.PartiesAdministration
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.damltests.java.test.DummyFactory
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.examples.java.trailingnone.TrailingNone
import com.digitalasset.canton.examples.java.{cycle as M, trailingnone as T}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.util.UpdateFormatHelpers.getUpdateFormat
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  MultiTransactionSignature,
  NamespaceDelegation,
  ParticipantPermission,
  PartyToKeyMapping,
  PartyToParticipant,
  SignedTopologyTransaction,
  SingleTransactionSignature,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, ExternalParty, Namespace, PartyId}
import com.google.protobuf.ByteString
import io.grpc.Status
import monocle.Optional
import monocle.macros.GenLens
import org.slf4j.event.Level

import java.util.UUID
import scala.jdk.OptionConverters.RichOption

trait InteractiveSubmissionIntegrationTestSetup
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  protected val preparedTxMetadataOpt: Optional[PreparedTransaction, Metadata] =
    Optional[PreparedTransaction, Metadata](_.metadata)(md => tx => tx.copy(metadata = Some(md)))

  protected val preparedSubmissionResponseOpt
      : Optional[PrepareSubmissionResponse, PreparedTransaction] =
    Optional[PrepareSubmissionResponse, PreparedTransaction](_.preparedTransaction)(tx =>
      res => res.copy(preparedTransaction = Some(tx))
    )

  protected val preparedTxResponseInputContractsOpt
      : Optional[PrepareSubmissionResponse, Seq[Metadata.InputContract]] =
    preparedSubmissionResponseOpt
      .andThen(preparedTxMetadataOpt)
      .andThen(
        GenLens[Metadata]((m: Metadata) => m.inputContracts)
      )

  protected var aliceE: ExternalParty = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonTestsPath, synchronizerId = daId)

        aliceE = cpn.parties.testing.external.enable("Alice")
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)

  protected def createTrailingNoneContract(
      party: ExternalParty
  )(implicit env: FixtureParam): CreatedEvent =
    cpn.ledger_api.javaapi.commands
      .submit(
        Seq(party),
        Seq(
          TrailingNone
            .create(party.partyId.toProtoPrimitive, java.util.Optional.empty())
            .commands()
            .loneElement
        ),
        includeCreatedEventBlob = true,
      )
      .getEvents
      .asScalaProtoCreatedContracts
      .loneElement
}

class InteractiveSubmissionIntegrationTest extends InteractiveSubmissionIntegrationTestSetup {
  private def assertLabelsAndIdentifiersNonEmpty(value: Value): Unit =
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
      danE = cpn.parties.testing.external.enable(
        "Dan",
        keysCount = PositiveInt.three,
        keysThreshold = PositiveInt.two,
      )
    }

    "fail preparing multiple commands" in { implicit env =>
      val commands = Seq(
        createCycleCommand(aliceE, "a"),
        createCycleCommand(aliceE, "b"),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        cpn.ledger_api.interactive_submission.prepare(Seq(aliceE), commands),
        _.errorMessage should include("Preparing multiple commands is currently not supported"),
      )
    }

    "fail executing multiple root nodes" in { implicit env =>
      import env.*

      val prepared = cpn.ledger_api.interactive_submission
        .prepare(Seq(aliceE), Seq(createCycleCommand(aliceE, "a")))
      val preparedWithAddedRootNode = prepared.preparedTransaction.value.update(
        _.transaction.update(_.roots.modify(_ :+ "new_node"))
      )
      // The hash is technically wrong but the root node check happens even before we verify the signature so it
      // doesn't matter
      val signature = global_secret.sign(prepared.preparedTransactionHash, aliceE)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        cpn.ledger_api.interactive_submission.execute(
          preparedWithAddedRootNode,
          Map(aliceE.partyId -> signature),
          UUID.randomUUID().toString,
          HashingSchemeVersion.HASHING_SCHEME_VERSION_V2,
        ),
        _.errorMessage should include("Transaction with multiple root nodes are not supported"),
      )
    }

    "require threshold-many signatures from different keys" in { implicit env =>
      import env.*

      // Onboard the external party the old school way with a PartyToKeyMapping,
      // Because PTK does not have the check on duplicate keys, on purpose for backwards compatibility with
      // existing transactions with duplicated keys on mainnet
      val namespaceKey = global_secret.keys.secret
        .generate_keys(PositiveInt.one, usage = NonEmpty.mk(Set, SigningKeyUsage.Namespace))
        .head

      val protocolKeys = global_secret.keys.secret
        .generate_keys(PositiveInt.two, usage = NonEmpty.mk(Set, SigningKeyUsage.Protocol))

      val protocolKey1 = protocolKeys.head
      val protocolKey2 = protocolKeys.forgetNE(1)

      val partyId = PartyId.tryCreate("Alice", namespaceKey.fingerprint)

      val namespaceDelegation = TopologyTransaction(
        mapping = NamespaceDelegation.tryCreate(
          Namespace(namespaceKey.fingerprint),
          namespaceKey,
          CanSignAllMappings,
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val partyToParticipant = TopologyTransaction(
        mapping = PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          participants = Seq(HostingParticipant(participant1, ParticipantPermission.Confirmation)),
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      val partyToKeyMapping = TopologyTransaction(
        mapping = PartyToKeyMapping.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.two,
          // Use twice the same key.. it will get deduplicated
          signingKeys = NonEmpty.mk(Seq, protocolKey1, protocolKey1, protocolKey2),
        ),
        op = TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        protocolVersion = testedProtocolVersion,
      )

      // The signing keys in the PTK get silently deduplicated
      partyToKeyMapping.mapping.signingKeys.forgetNE should contain theSameElementsAs
        List(protocolKey1, protocolKey2)

      val hashSet =
        NonEmpty.mk(Set, namespaceDelegation.hash, partyToParticipant.hash, partyToKeyMapping.hash)

      val multihash = MultiTransactionSignature.computeCombinedHash(
        hashSet,
        tryGlobalCrypto.pureCrypto,
      )

      val multiHashSignature = global_secret.sign(
        multihash.getCryptographicEvidence,
        namespaceKey.fingerprint,
        SigningKeyUsage.NamespaceOnly,
      )

      val ptkProofOfOwnershipSignature1 = global_secret.sign(
        partyToKeyMapping.hash.hash.getCryptographicEvidence,
        protocolKey1.fingerprint,
        SigningKeyUsage.ProofOfOwnershipOnly,
      )

      val ptkProofOfOwnershipSignature2 = global_secret.sign(
        partyToKeyMapping.hash.hash.getCryptographicEvidence,
        protocolKey2.fingerprint,
        SigningKeyUsage.ProofOfOwnershipOnly,
      )

      val signedNsd = SignedTopologyTransaction
        .create(
          transaction = namespaceDelegation,
          signatures = NonEmpty.mk(Set, MultiTransactionSignature(hashSet, multiHashSignature)),
          isProposal = false,
          protocolVersion = testedProtocolVersion,
        )
        .value

      val signedPtp = SignedTopologyTransaction
        .create(
          transaction = partyToParticipant,
          signatures = NonEmpty.mk(Set, MultiTransactionSignature(hashSet, multiHashSignature)),
          isProposal = false,
          protocolVersion = testedProtocolVersion,
        )
        .value

      val signedPtk = SignedTopologyTransaction
        .create(
          transaction = partyToKeyMapping,
          signatures = NonEmpty.mk(
            Set,
            MultiTransactionSignature(hashSet, multiHashSignature),
            SingleTransactionSignature(partyToKeyMapping.hash, ptkProofOfOwnershipSignature1),
            SingleTransactionSignature(partyToKeyMapping.hash, ptkProofOfOwnershipSignature2),
          ),
          isProposal = false,
          protocolVersion = testedProtocolVersion,
        )
        .value

      // Ptp with participant signature
      val fullySignedPtp =
        participant1.topology.transactions.sign(Seq(signedPtp), synchronizer1Id).loneElement

      participant1.topology.transactions.load(
        Seq(signedNsd, fullySignedPtp, signedPtk),
        synchronizer1Id,
      )

      PartiesAdministration.Allocation.waitForPartyKnown(
        partyId = partyId,
        hostingParticipant = participant1,
        synchronizeParticipants = Seq(participant1),
        synchronizerId = synchronizer1Id.logical,
      )

      // Wait until the PartyToKey mapping is also there.
      // The outbox may dispatch it in a different batch than the PartyToParticipant mapping.
      eventually() {
        val ptks = participant1.topology.transactions.list(
          store = daId,
          filterMappings = Seq(Code.PartyToKeyMapping),
          filterNamespace = partyId.namespace.filterString,
        )
        ptks should not be Seq.empty
      }
      participant1.topology.synchronisation.await_idle()

      val prepared = participant1.ledger_api.interactive_submission.prepare(
        Seq(partyId),
        Seq(createCycleCommand(partyId, UUID.randomUUID().toString)),
      )

      def signWithProtocolKey1 = global_secret.sign(
        prepared.preparedTransactionHash,
        protocolKey1.fingerprint,
        SigningKeyUsage.ProtocolOnly,
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.interactive_submission.execute_and_wait(
          prepared.getPreparedTransaction,
          // Provide 2 signatures from the same key
          Map(partyId -> Seq(signWithProtocolKey1, signWithProtocolKey1)),
          UUID.randomUUID().toString,
          HASHING_SCHEME_VERSION_V2,
        ),
        _.errorMessage should include(
          "Received 1 valid signatures from distinct keys (0 invalid), but expected at least 2 valid"
        ),
      )
    }

    "fail to submit when party is not hosted with confirmation permission on the synchronizer" in {
      implicit env =>
        import env.*

        val partyE = cpn.parties.testing.external.enable("Party")
        val currentP2P = cpn.topology.party_to_participant_mappings
          .list(synchronizer1Id, filterParty = partyE.filterString)
          .loneElement
          .item

        // Change cpn to observation rights
        val newPTP = TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.two,
          mapping = PartyToParticipant
            .create(
              partyE.partyId,
              threshold = PositiveInt.one,
              Seq(
                HostingParticipant(cpn, ParticipantPermission.Observation, false)
              ),
              partySigningKeysWithThreshold = currentP2P.partySigningKeysWithThreshold,
            )
            .value,
          protocolVersion = testedProtocolVersion,
        )

        cpn.topology.transactions.load(
          Seq(global_secret.sign(newPTP, partyE, testedProtocolVersion)),
          daId,
        )

        utils.retry_until_true {
          epn.topology.party_to_participant_mappings.is_known(
            daId,
            partyE,
            hostingParticipants = Seq(cpn),
            permission = Some(ParticipantPermission.Observation),
          )
        }

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          createCycleContract(epn, partyE, "test-external-signing"),
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
      createCycleContract(epn, aliceE, "test-external-signing")
    }

    "create a transaction with multiple nodes" in { implicit env =>
      val dummyFactoryTx = epn.ledger_api.javaapi.commands.submit(
        Seq(aliceE),
        Seq(DummyFactory.create(aliceE.toProtoPrimitive).commands().loneElement),
      )

      val dummyFactory =
        JavaDecodeUtil.decodeAllCreated(DummyFactory.COMPANION)(dummyFactoryTx).loneElement

      val exerciseCommand = DummyFactory.ContractId
        .fromContractId(new CodeGenCID(dummyFactory.id.contractId))
        .exerciseDummyFactoryCall()

      // prepare on the cpn to avoid having to explicitly disclose the contract
      val preparedTransaction = cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(exerciseCommand.commands().loneElement),
      )

      val exerciseTransaction = epn.ledger_api.commands.external.submit_prepared(
        aliceE,
        preparedTransaction,
      )

      // We should have one exercise and two creates
      val created = exerciseTransaction.events.map(_.event).collect { case created: Event.Created =>
        created
      }
      val exercised = exerciseTransaction.events.map(_.event).collect {
        case exercised: Event.Exercised => exercised
      }

      created.size shouldBe 2
      exercised.size shouldBe 1
    }

    // Failure case is tested in InteractiveSubmissionConfirmationIntegrationTest:
    // "fail execute and wait if the signatures are invalid"
    "get a completion for a successful externally signed transaction from submissionId" in {
      implicit env =>
        import env.*

        val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
          Seq(aliceE.partyId),
          Seq(
            new M.Cycle(
              "test-external-signing",
              aliceE.toProtoPrimitive,
            ).create.commands.loneElement
          ),
        )

        val submissionId = UUID.randomUUID().toString
        val ledgerEnd = epn.ledger_api.state.end()
        epn.ledger_api.interactive_submission
          .execute(
            prepared.preparedTransaction.value,
            Map(
              aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
            ),
            submissionId,
            prepared.hashingSchemeVersion,
          )
          .discard
        val completion = findCompletion(submissionId, ledgerEnd, aliceE, epn)
        completion.status.map(_.code) shouldBe Some(io.grpc.Status.OK.getCode.value())
    }

    "return events from executeAndWaitForTransaction accordingly to format and hosting" in {
      implicit env =>
        import env.*

        forAll(
          Table(
            ("EPN = CPN", "TransactionShape", "Has events"),
            (true, None, true),
            // If the CPN != EPN, events won't show up with a default filter (ACS_DELTA) as the EPN does not host the party
            (
              false,
              None,
              false,
            ),
            (
              true,
              Some(TRANSACTION_SHAPE_ACS_DELTA),
              true,
            ),
            (
              true,
              Some(TRANSACTION_SHAPE_LEDGER_EFFECTS),
              true,
            ),
            // If the CPN != EPN, events won't show up with an ACS_DELTA filter as the EPN does not host the party
            (
              false,
              Some(TRANSACTION_SHAPE_ACS_DELTA),
              false,
            ),
            (
              false,
              Some(TRANSACTION_SHAPE_LEDGER_EFFECTS),
              true,
            ),
          )
        ) { case (epnIsCpn, transactionShape, expectsEvents) =>
          val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
            Seq(aliceE.partyId),
            Seq(
              new M.Cycle(
                "test-external-signing",
                aliceE.toProtoPrimitive,
              ).create.commands.loneElement
            ),
          )
          val pn = if (epnIsCpn) cpn else epn
          val expectedSize = if (expectsEvents) 1L else 0L
          val transaction = pn.ledger_api.interactive_submission
            .execute_and_wait_for_transaction(
              prepared.preparedTransaction.value,
              Map(
                aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
              ),
              UUID.randomUUID().toString,
              prepared.hashingSchemeVersion,
              transactionShape = transactionShape,
            )
          transaction.events should have size expectedSize
        }
    }

    "get the transaction from the transaction stream" in { implicit env =>
      import env.*

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      val submissionId = UUID.randomUUID().toString
      val ledgerEnd = cpn.ledger_api.state.end()
      epn.ledger_api.interactive_submission
        .execute(
          prepared.preparedTransaction.value,
          Map(
            aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
          ),
          submissionId,
          prepared.hashingSchemeVersion,
        )
        .discard
      val transaction =
        findTransactionInStream(aliceE, ledgerEnd, prepared.preparedTransactionHash, cpn)
      transaction.externalTransactionHash shouldBe Some(prepared.preparedTransactionHash)
    }

    "get the transaction from the update ID" in { implicit env =>
      import env.*

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      val submissionId = UUID.randomUUID().toString
      val response = epn.ledger_api.interactive_submission.execute_and_wait(
        prepared.preparedTransaction.value,
        Map(
          aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
        ),
        submissionId,
        prepared.hashingSchemeVersion,
      )
      val transaction = eventually() {
        findTransactionByUpdateId(aliceE, response.updateId)
      }
      transaction.externalTransactionHash shouldBe Some(prepared.preparedTransactionHash)
    }

    "submit on epn, confirm on cpn with threshold > 1" in { implicit env =>
      createCycleContract(epn, danE, "test-external-signing")
    }

    "execute and wait if transaction succeeds" in { implicit env =>
      import env.*

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(danE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            danE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )
      val transactionSignatures = Map(
        danE.partyId -> global_secret.sign(prepared.preparedTransactionHash, danE)
      )

      val response = execAndWait(prepared, transactionSignatures)

      // Event is emitted on the update stream
      eventually() {
        cpn.ledger_api.updates
          .update_by_id(response.updateId, getUpdateFormat(Set(danE.partyId)))
          .collect { case tx: TransactionWrapper => tx.transaction }
          .value
      }
    }

    "execute and wait if transaction fails" in { implicit env =>
      import env.*

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      val transactionSignatures = Map(
        danE.partyId -> global_secret.sign(prepared.preparedTransactionHash, danE)
      )
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
      import env.*

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      val transactionSignatures = Map(
        danE.partyId -> global_secret.sign(prepared.preparedTransactionHash, danE)
      )
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
      val contract1CreatedEvent = createTrailingNoneContract(aliceE)
      val contract2CreatedEvent = createTrailingNoneContract(aliceE)

      // Now exercise the archiveMe choice with explicit disclosure using contract1
      val archiveCmd = new T.TrailingNone.ContractId(contract1CreatedEvent.contractId)
        .exerciseArchiveMe(aliceE.toProtoPrimitive)
        .commands()
        .loneElement

      val preparedArchive = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(archiveCmd),
        disclosedContracts = Seq(
          new DisclosedContract(
            contract1CreatedEvent.createdEventBlob,
            env.daId.logical.toProtoPrimitive,
            Some(TrailingNone.TEMPLATE_ID_WITH_PACKAGE_ID).toJava,
            Some(contract1CreatedEvent.contractId).toJava,
          )
        ),
      )

      cpn.ledger_api.commands.external.submit_prepared(aliceE, preparedArchive).discard

      // And now with contract2, using the cpn to prepare as well so we don't need to explicitly disclose the contract
      val archiveCmd2 = new T.TrailingNone.ContractId(contract2CreatedEvent.contractId)
        .exerciseArchiveMe(aliceE.toProtoPrimitive)
        .commands()
        .loneElement

      cpn.ledger_api.javaapi.commands.submit(Seq(aliceE), Seq(archiveCmd2))
    }

    "create a contract with verbose hashing" in { implicit env =>
      import env.*

      val prepared = ppn.ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(createCycleCommand(aliceE, "test-external-signing")),
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
      execAndWait(
        prepared,
        Map(
          aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
        ),
      ).discard
    }

    "execute a choice on an existing contract via explicit disclosure" in { implicit env =>
      import env.*

      val cycle = createCycleContract(epn, aliceE, "test-external-signing")

      val cycleCreated = cpn.ledger_api.state.acs
        .active_contracts_of_party(
          aliceE,
          filterTemplates = TemplateId.templateIdsFromJava(Cycle.TEMPLATE_ID),
          includeCreatedEventBlob = true,
        )
        .filter(_.getCreatedEvent.contractId == cycle.id.contractId)
        .loneElement
        .getCreatedEvent

      // Exercise the Repeat choice
      val exerciseRepeatOnCycleContract = cycle.id.exerciseRepeat().commands().loneElement

      // Call the prepare endpoint - this gives us back a serialized transaction, and the hash to be signed
      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(exerciseRepeatOnCycleContract),
        disclosedContracts = Seq(
          new DisclosedContract(
            cycleCreated.createdEventBlob,
            daId.logical.toProtoPrimitive,
            Some(Cycle.TEMPLATE_ID_WITH_PACKAGE_ID).toJava,
            Some(cycleCreated.contractId).toJava,
          )
        ),
      )

      // Check that input contracts also have identifiers and labels
      prepared.preparedTransaction.value.metadata.value.inputContracts
        .map(_.contract.v1.value)
        .foreach { value =>
          value.argument.foreach(assertLabelsAndIdentifiersNonEmpty)
        }
      execAndWait(
        prepared,
        Map(
          aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
        ),
      ).discard
    }

    "fail if signature is signed by non acting party" in { implicit env =>
      import env.*

      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      val transactionSignatures = Map(
        danE.partyId -> global_secret.sign(prepared.preparedTransactionHash, danE)
      )
      val badSignatures = Map(aliceE.partyId -> transactionSignatures(danE.partyId))
      execFailure(prepared, badSignatures, "Received 0 valid signatures")
    }

    "fail in phase 1 if the number of signatures is under the threshold" in { implicit env =>
      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(danE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            danE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      // Not enough because threshold is 2
      val singleSignature = env.global_secret.sign(
        prepared.preparedTransactionHash,
        danE.signingFingerprints.head1,
        SigningKeyUsage.ProtocolOnly,
      )

      execFailure(
        prepared,
        Map(danE.partyId -> Seq(singleSignature)),
        s"Received 1 valid signatures from distinct keys (0 invalid), but expected at least 2 valid for ${danE.partyId}",
      )
    }

    "fail to execute if input contracts are missing" in { implicit env =>
      import env.*
      val cycle = createCycleContract(epn, aliceE, "test-external-signing")

      // Exercise the Repeat choice
      val exerciseRepeatOnCycleContract = cycle.id.exerciseRepeat().commands().loneElement

      val prepared = cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(exerciseRepeatOnCycleContract),
      )

      // Remove input contracts from the prepared transaction
      val txWithoutInputContract =
        preparedTxResponseInputContractsOpt.replace(Seq.empty)(prepared)

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        {
          val (submissionId, ledgerEnd) =
            exec(
              txWithoutInputContract,
              Map(
                aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
              ),
              cpn,
            )
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
        val cycle = createCycleContract(epn, aliceE, "test-external-signing")

        // Exercise the Repeat choice
        val exerciseRepeatOnCycleContract = cycle.id.exerciseRepeat().commands().loneElement

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          // The PPN does not know about the newly created contract, so the prepare command fails

          ppn.ledger_api.javaapi.interactive_submission.prepare(
            Seq(aliceE.partyId),
            Seq(exerciseRepeatOnCycleContract),
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
      val cycle = createCycleContract(epn, aliceE, "test-external-signing")

      val exerciseRepeatOnCycleContract =
        Cycle.ContractId
          .fromContractId(new CodeGenCID(cycle.id.contractId))
          .exerciseRepeat()
          .commands()
          .loneElement

      // Prepare on the cpn without disclosing the contract
      val prepared = cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(exerciseRepeatOnCycleContract),
      )

      epn.ledger_api.commands.external.submit_prepared(aliceE, prepared)
    }

    "fail to prepare a transaction if the preparing party is not authorized" in { implicit env =>
      import env.*

      val bob = cpn.parties.enable("Bob")
      // Find Alice cycle contract
      val aliceCycleContract =
        cpn.ledger_api.state.acs
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
          cpn.ledger_api.interactive_submission.prepare(
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
      cpn.ledger_api.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(prepareExerciseOnAliceContract),
        synchronizerId = Some(daId),
      )
    }

    "fail synchronously for an invalid signature" in { implicit env =>
      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            aliceE.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

      val badSignature = env.global_secret.sign(
        ByteString.copyFromUtf8("gipfeli"),
        aliceE.signingFingerprints.head1,
        SigningKeyUsage.ProtocolOnly,
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        a[CommandFailure] shouldBe thrownBy {
          exec(prepared, Map(aliceE.partyId -> Seq(badSignature)), epn)
        },
        LogEntry.assertLogSeq(
          Seq(
            (
              _.errorMessage should include(
                "The participant failed to execute the transaction: Received 0 valid signatures from distinct keys (1 invalid)"
              ),
              "invalid signature",
            )
          ),
          Seq.empty,
        ),
      )
    }

    "fail to prepare if the party does not exist on the synchronizer" in { implicit env =>
      val nonExistingParty = DefaultTestIdentities.party1
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        ppn.ledger_api.javaapi.interactive_submission.prepare(
          Seq(nonExistingParty),
          Seq(
            new M.Cycle(
              "test-external-signing",
              nonExistingParty.toProtoPrimitive,
            ).create.commands.loneElement
          ),
        ),
        _.errorMessage should (include(UnknownInformees.id) and include(
          s"unknownInformees=>Set(${nonExistingParty.partyId.toProtoPrimitive})"
        )),
      )
    }

    "fail to execute if the party does not exist on the synchronizer" in { implicit env =>
      import env.*
      val temporaryPartyE = ppn.parties.testing.external.enable("temp")
      val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(temporaryPartyE.partyId),
        Seq(
          new M.Cycle(
            "test-external-signing",
            temporaryPartyE.partyId.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )
      // Offboard the party from the synchronizer before executing
      offboardParty(temporaryPartyE, ppn, synchronizer1Id)

      // Execute should fail
      val signatures = global_secret.sign(prepared.preparedTransactionHash, temporaryPartyE)
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        ppn.ledger_api.interactive_submission.execute(
          prepared.getPreparedTransaction,
          Map(temporaryPartyE.partyId -> signatures),
          UUID.randomUUID().toString,
          HASHING_SCHEME_VERSION_V2,
        ),
        _.errorMessage should include(
          s"Could not find party signing keys for ${temporaryPartyE.partyId}"
        ),
      )
    }
  }

}

class InteractiveSubmissionMultiSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.synchronizers.connect_local(sequencer3, alias = repairSynchronizerName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonTestsPath, synchronizerId = daId)
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)

  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
          Set(InstanceName.tryCreate("sequencer3")),
        )
      ),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  "External parties" should {
    "can be allocated in a multi-synchronizer scenario" in { implicit env =>
      import env.*

      val aliceE =
        participant1.parties.testing.external.enable("Alice", synchronizer = Some(daName))

      // Check that alice is hosted on `hostedCount` synchronizers
      def ensureCorrectHosting(hostedCount: Int) = {
        val synchronizers = Seq(daId, acmeId, repairSynchronizerId)
        val activeOn = synchronizers.take(hostedCount)
        val inactiveOn = synchronizers.drop(hostedCount)

        activeOn.foreach { psid =>
          participant1.topology.party_to_participant_mappings
            .list(psid, filterParty = aliceE.filterString) should not be empty
        }

        inactiveOn.foreach { psid =>
          participant1.topology.party_to_participant_mappings
            .list(psid, filterParty = aliceE.filterString) shouldBe empty
        }
      }

      ensureCorrectHosting(1)

      participant1.parties.testing.external.also_enable(aliceE, acmeName)
      ensureCorrectHosting(2)

      participant1.parties.testing.external.also_enable(aliceE, repairSynchronizerName)
      ensureCorrectHosting(3)
    }
  }
}

class InteractiveSubmissionIntegrationTestTimeouts
    extends InteractiveSubmissionIntegrationTestSetup {
  "timeout if CPN does not respond" in { implicit env =>
    import env.*

    // Reduce timeouts so this test completes faster.
    sequencer1.topology.synchronizer_parameters.propose_update(
      sequencer1.synchronizer_id,
      _.update(
        confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(2),
        mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(2),
      ),
    )

    cpn.synchronizers.disconnect_all()
    val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
      Seq(aliceE.partyId),
      Seq(
        new M.Cycle(
          "test-external-signing",
          aliceE.toProtoPrimitive,
        ).create.commands.loneElement
      ),
    )
    val signatures = Map(
      aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
    )
    val (submissionId, ledgerEnd) = exec(prepared, signatures, epn)
    val completion = findCompletion(
      submissionId,
      ledgerEnd,
      aliceE,
      epn,
    )
    completion.status.value.code shouldBe Status.Code.ABORTED.value()
    completion.status.value.message should include("MEDIATOR_SAYS_TX_TIMED_OUT")
    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
      cpn.synchronizers.reconnect_all(),
      LogEntry.assertLogSeq(
        Seq.empty,
        Seq(
          _.warningMessage should (include("Response message for request") and include(
            "timed out"
          ))
        ),
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
    import env.*

    val prepared = ppn.ledger_api.javaapi.interactive_submission.prepare(
      Seq(aliceE.partyId),
      Seq(
        new M.Cycle(
          "test-external-signing",
          aliceE.toProtoPrimitive,
        ).create.commands.loneElement
      ),
    )
    val signatures = Map(
      aliceE.partyId -> global_secret.sign(prepared.preparedTransactionHash, aliceE)
    )
    val cpnLedgerEnd = cpn.ledger_api.state.end()
    val (submissionId, epnLedgerEnd) = exec(prepared, signatures, epn)
    val completionStatus = findCompletion(submissionId, epnLedgerEnd, aliceE, epn).status.value
    completionStatus.code shouldBe Status.Code.OK.value()
    cpn.stop()
    cpn.start()
    cpn.synchronizers.reconnect_all()
    findTransactionInStream(aliceE, cpnLedgerEnd, prepared.preparedTransactionHash, cpn)
  }
}
