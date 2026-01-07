// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import better.files.File
import com.daml.ledger.api.v2.commands.DisclosedContract
import com.daml.ledger.api.v2.value.Value as ApiValue
import com.daml.ledger.javaapi as javab
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.LedgerApiTypeWrappers.WrappedContractEntry
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.auth.AuthorizationChecksErrors.PermissionDenied
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeProportion,
  PositiveInt,
}
import com.digitalasset.canton.config.{CommitmentSendDelay, PositiveDurationSeconds}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.trailingnone as T
import com.digitalasset.canton.examples.java.trailingnone.TrailingNone
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.ledgerapi.submission.BaseInteractiveSubmissionTest
import com.digitalasset.canton.integration.tests.manual.S3Synchronization
import com.digitalasset.canton.integration.tests.upgrade.MajorUpgradeUtils.CantonNodes
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasTrailingNoneUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceSynchronizerDisabledUs
import com.digitalasset.canton.topology.ForceFlag.DisablePartyWithActiveContracts
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, TopologyChangeOp}
import com.digitalasset.canton.topology.{
  ExternalParty,
  ForceFlags,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.jdk.CollectionConverters.*

/*
 * This test is a manual test that is used to test the major upgrade of the Canton network.
 * The SimpleUpgradeTest uses 2 participants, 1 sequencer, and 1 mediator.
 * The focus is on creating parties, uploading dars, creating contracts,
 * and updating domain parameters before and after the migration.
 *
 * Write side is about starting the nodes and exporting the data.
 */
class MajorUpgradeSimpleWriterIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasTrailingNoneUtils
    with MajorUpgradeUtils
    with BaseInteractiveSubmissionTest {
  override protected def testName: String = "major-upgrade-simple"

  // register all plugins
  registerPlugin(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup(networkBootstrapSetup(_))

  override def beforeAll(): Unit = {
    createDirectory()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    copyFiles()
    super.afterAll()
  }

  "Major upgrade simple case" should {
    "happy path" onlyRunWith ProtocolVersion.latest in { implicit env =>
      import env.*

      val alice = participant2.parties.enable(
        "alice",
        synchronizeParticipants = Seq(participant1),
      )
      val bob = participant1.parties.enable(
        "bob",
        synchronizeParticipants = Seq(participant2),
      )

      // charlie and dan are the external party equivalents of alice and bob
      val charlie = participant2.parties.testing.external.enable("charlie")
      val dan = participant1.parties.testing.external.enable("dan")

      exportExternalPartyKeys(charlie)
      exportExternalPartyKeys(dan)

      IouSyntax.createIou(participant1, Some(daId))(bob, alice)
      IouSyntax.createIou(participant2, Some(daId))(alice, bob)
      IouSyntax.createIou(participant1)(dan, charlie)
      IouSyntax.createIou(participant2)(charlie, dan)

      // We create two TrailingNone contracts: in the read test, one will be consumed via disclosure,
      // the other one by reading from the contract store.
      createTrailingNoneContract(participant1, bob, "creation of trailing none 1")
      createTrailingNoneContract(participant1, bob, "creation of trailing none 2")

      // Do the same for charlie as an external party
      val cmd = TrailingNone
        .create(dan.toProtoPrimitive, java.util.Optional.empty())

      participant1.ledger_api.javaapi.commands.submit(
        Seq(dan),
        cmd.commands().asScala.toSeq,
        commandId = "creation of trailing none 1 for dan",
      )
      participant1.ledger_api.javaapi.commands.submit(
        Seq(dan),
        cmd.commands().asScala.toSeq,
        commandId = "creation of trailing none 2 for dan",
      )

      List(bob, dan.partyId).foreach { partyWithDisclosure =>
        val createdEvent = trailingNoneAcsWithBlobs(participant1, partyWithDisclosure).head.event
        val disclosure = DisclosedContract(
          templateId = createdEvent.templateId,
          contractId = createdEvent.contractId,
          createdEventBlob = createdEvent.createdEventBlob,
          synchronizerId = "",
        )
        exportDisclosure(
          s"trailing-none-${partyWithDisclosure.uid.identifier.str}",
          DisclosedContract.toJavaProto(disclosure),
        )
      }
      exportNodesData()
    }
  }

  private def networkBootstrapSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    participants.all.synchronizers.connect_local(sequencer1, daName)

    // By uploading and removing the DARs, we increase the serial number of the vetted topology transaction.
    // The goal is to make sure we don't get a SerialMismatch between the authorize store and the domain store

    File(CantonExamplesPath).copyToDirectory(baseExportDirectory)

    participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)

    // explicitly allocate an admin party
    participant1.topology.party_to_participant_mappings.propose(
      participant1.id.adminParty,
      Seq(participant1.id -> ParticipantPermission.Submission),
    )

    // TODO(#27707) – Possibly revise it in the context of party replication
    // this is a workaround to avoid warning from acs commitment after we remove participant3's DTC
    sequencer1.topology.synchronizer_parameters.propose_update(
      synchronizerId = daId,
      _.update(reconciliationInterval = PositiveDurationSeconds.ofDays(365)),
    )
  }
}

/*
 * This test is used to test the major upgrade of the Canton network.
 * The SimpleUpgradeTest uses 2 participants, 1 sequencer, and 1 mediator.
 * The focus is on creating parties, uploading dars, creating contracts,
 * and updating synchronizer parameters before and after the migration.
 *
 * Read side is reading the data and acting on it.
 */
final class MajorUpgradeSimpleReaderIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with MajorUpgradeUtils
    with S3Synchronization
    with BaseInteractiveSubmissionTest {
  override protected def testName: String = "major-upgrade-simple"

  private var migratedCantonNodes: CantonNodes = _
  private var synchronizerId: SynchronizerId = _

  private lazy val s3Ref: S3Synchronization.ContinuityDumpRef = getPreviousDump()

  // triggers the download from s3 when accessed
  private lazy val exportDirectory: File = s3Ref.localDownloadPath / testName

  registerPlugin(new UsePostgres(loggerFactory))

  private lazy val envDef = EnvironmentDefinition.P2S1M1_Config

  override lazy val environmentDefinition: EnvironmentDefinition =
    envDef.withManualStart
      .addConfigTransform(
        ConfigTransforms.disableAutoInit(envDef.baseConfig.allLocalNodes.keySet.map(_.unwrap))
      )
      .withSetup { implicit env =>
        import env.*

        migratedCantonNodes = CantonNodes(
          participants = Seq(participant1, participant2),
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
        )

        participants.local.start()
        sequencers.local.start()
        mediators.local.start()
      }
      .updateTestingConfig(
        _.copy(
          participantsWithoutLapiVerification = Set(
            "participant1",
            "participant2",
          ),
          // Don't delay sending ACS commitments, so that we don't have scheduling uncertainty
          //  that could lead to flakes if they fail and log warnings due to topology changes
          //  made by the test logic.
          commitmentSendDelay = Some(
            CommitmentSendDelay(Some(NonNegativeProportion.zero), Some(NonNegativeProportion.zero))
          ),
        )
      )

  private def importExternalParty(name: String, parties: Seq[PartyId])(implicit
      env: TestConsoleEnvironment
  ): (ExternalParty, PartyId) = {
    val party = parties.find(_.identifier.str == name).value
    val externalKeys =
      NonEmpty.from(readExternalPartyPrivateKeys(party, exportDirectory)).value

    env.global_secret.keys.secret.store(externalKeys)

    val externalParty = ExternalParty(
      party,
      NonEmpty
        .from(externalKeys.map(_.id))
        .value,
      PositiveInt.one,
    )
    (externalParty, party)
  }

  "Major upgrade simple case" should {
    s"happy path (${s3Ref.localDownloadPath} with ${ProtocolVersion.latest})" onlyRunWith ProtocolVersion.latest in {
      implicit env =>
        import env.*

        logger.info(s"Starting major upgrade test with dump ${s3Ref.localDownloadPath}")

        synchronizerId =
          SynchronizerId.tryFromString((exportDirectory / "synchronizer-id").contentAsString)

        val newStaticSynchronizerParameters =
          StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = testedProtocolVersion)

        val physicalSynchronizerId =
          PhysicalSynchronizerId(synchronizerId, newStaticSynchronizerParameters.toInternal)

        // Migrate nodes preserving their data (and IDs)
        migratedCantonNodes.all.foreach { newNode =>
          suppressDarSelfConsistencyWarning(
            migrateNode(
              migratedNode = newNode,
              newStaticSynchronizerParameters = newStaticSynchronizerParameters,
              synchronizerId = physicalSynchronizerId,
              newSequencers = migratedCantonNodes.sequencers,
              dars = Seq((exportDirectory / "CantonExamples.dar").canonicalPath),
              exportDirectory = exportDirectory,
            )
          )
        }
        migratedCantonNodes.participants.foreach { newNode =>
          importAcs(newNode, migratedCantonNodes.sequencers, daName, exportDirectory)
        }

        // update synchronizer parameters
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId,
          _.update(
            confirmationRequestsMaxRate = NonNegativeInt.tryCreate(30000)
          ),
        )

        sequencer1.synchronizer_parameters.static
          .get()
          .protocolVersion shouldBe testedProtocolVersion

        val parties = sequencer1.topology.party_to_participant_mappings
          .list(synchronizerId)
          .map(_.item)
          .map(_.partyId)
        val alice = parties.find(_.identifier.str == "alice").value
        val bob = parties.find(_.identifier.str == "bob").value
        val (charlieE, charlie) = importExternalParty("charlie", parties)
        val (danE, dan) = importExternalParty("dan", parties)

        // checks
        participant1.parties
          .list(filterParticipant = participant1.id.filterString)
          .map(_.party) should contain theSameElementsAs List(
          participant1.adminParty,
          bob,
          dan,
        )

        participant2.parties
          .list(filterParticipant = participant2.id.filterString)
          .map(_.party) should contain theSameElementsAs List(
          participant2.adminParty,
          alice,
          charlie,
        )

        def checkNormalizedContractValues(
            contracts: Seq[WrappedContractEntry]
        ): Unit = {
          val contractsWrittenWithTrailingNones =
            contracts.filter(_.templateId.isModuleEntity("TrailingNone", "TrailingNone"))

          // 2 created in 3.3
          contractsWrittenWithTrailingNones should have size 2

          contractsWrittenWithTrailingNones.foreach { contract =>
            // Check no trailing None Optionals in the values
            contract.event.createArguments.value.fields.loneElement.getValue shouldBe ApiValue(
              ApiValue.Sum.Party(
                bob.toProtoPrimitive
              )
            )
          }
        }

        checkNormalizedContractValues(
          participant1.ledger_api.state.acs.of_party(bob, verbose = true)
        )
        checkNormalizedContractValues(
          participant1.ledger_api.state.acs.of_party(bob, verbose = false)
        )

        val contractsOfParty1 = participant1.ledger_api.state.acs.of_party(bob)

        // 2 IOUs + 2 TrailingNones
        contractsOfParty1 should have size 4

        participant1.health.ping(participant2.id)

        def checkDisclosures(
            partyWithDisclosedContract: PartyId, // Bob or Dan
            partyWithoutDisclosedContract: PartyId, // Alice or Charlie
            submit: (
                LocalParticipantReference,
                PartyId,
                com.daml.ledger.javaapi.data.Command,
                Seq[javab.data.DisclosedContract],
            ) => Unit,
        ): Assertion = {
          // Let's check that a disclosure obtained with the old version of canton can be
          // used in the new version on the migrated participants.
          val disclosures = importDisclosures(
            disclosureNamePrefix =
              s"trailing-none-${partyWithDisclosedContract.uid.identifier.str}",
            exportDirectory = exportDirectory,
          )
          disclosures.foreach { disclosure =>
            val disclosedContractId =
              new T.TrailingNone.ContractId(disclosure.contractId.asScala.value)
            // Alice (resp. Charlie) is not a signatory of the disclosed contracts: Bob (resp. Dan) is. The contracts are not even hosted on participant 2 because
            // Bob (resp. Dan) is only authorized on participant 1. But because we pass the contracts as disclosures, the
            // exercise is expected to succeed.
            // This situation is meant to force participant 2 to read the disclosures' contents instead of possibly
            // fetching from the contract store as it could choose to do: we want to test the decoding of
            // disclosures written by previous versions of canton.
            submit(
              participant2,
              partyWithoutDisclosedContract,
              disclosedContractId
                .exerciseArchiveMe(new T.ArchiveMe(partyWithoutDisclosedContract.toProtoPrimitive))
                .commands
                .asScala
                .toSeq
                .loneElement,
              Seq(disclosure),
            )
          }

          eventually() {
            participant1.ledger_api.state.acs
              .of_party(partyWithDisclosedContract)
              .filter(
                _.templateId.isModuleEntity("TrailingNone", "TrailingNone")
              ) should have size 1
          }

          // Let's now that test that the remaining contract with a trailing none can be still be exercised after the ACS
          // import when read directly from the contract store.
          val remainingTrailingNoneContractIds =
            participant1.ledger_api.state.acs
              .of_party(partyWithDisclosedContract)
              .filter(_.templateId.isModuleEntity("TrailingNone", "TrailingNone"))
              .map(_.contractId)
              .map(new T.TrailingNone.ContractId(_))

          remainingTrailingNoneContractIds.foreach { remainingTrailingNoneContractId =>
            submit(
              participant1,
              partyWithDisclosedContract,
              remainingTrailingNoneContractId
                .exerciseArchiveMe(new T.ArchiveMe(partyWithDisclosedContract.toProtoPrimitive))
                .commands
                .asScala
                .toSeq
                .loneElement,
              Seq.empty,
            )
          }

          eventually() {
            participant1.ledger_api.state.acs
              .of_party(partyWithDisclosedContract)
              .filter(
                _.templateId.isModuleEntity("TrailingNone", "TrailingNone")
              ) should have size 0
          }
        }

        // Check disclosures for bob, a local party
        checkDisclosures(
          bob,
          alice,
          (participant, party, command, disclosures) =>
            participant.ledger_api.javaapi.commands
              .submit(
                actAs = Seq(party),
                commands = Seq(command),
                disclosedContracts = disclosures,
              ),
        )

        // Check disclosures for dan, an external party
        checkDisclosures(
          dan,
          charlie,
          (participant, party, command, disclosures) => {
            val partyE = List(charlieE, danE).find(_.partyId == party).value

            participant.ledger_api.javaapi.commands
              .submit(
                Seq(partyE),
                Seq(command),
                disclosedContracts = disclosures,
              )
              .discard
          },
        )

        val party3 = participant2.parties.enable(
          "party3",
          synchronizeParticipants = Seq(participant1),
        )

        IouSyntax.createIou(participant1, Some(synchronizerId))(bob, party3, optTimeout = None)

        val contract1 = contractsOfParty1.headOption.value

        participant1.ledger_api.commands
          .submit(Seq(bob), Seq(ledger_api_utils.exercise("Archive", Map.empty, contract1.event)))

        // allocating admin parties also works now
        participant2.topology.party_to_participant_mappings.propose(
          participant2.id.adminParty,
          Seq(participant2.id -> ParticipantPermission.Submission),
          store = synchronizerId,
        )

        // now let's test moving participant1's admin party to participant2.
        // 1. remove bob from participant1
        participant1.topology.party_to_participant_mappings.propose_delta(
          bob,
          removes = List(participant1.id),
          store = synchronizerId,
          forceFlags = ForceFlags(DisablePartyWithActiveContracts),
        )

        danE.topology.party_to_participant_mappings.propose_delta(
          participant1,
          removes = List(participant1.id),
          store = synchronizerId,
          forceFlags = ForceFlags(DisablePartyWithActiveContracts),
        )

        // 2. remove the explicitly allocated admin party
        participant1.topology.party_to_participant_mappings.propose_delta(
          participant1.id.adminParty,
          removes = List(participant1.id),
          store = synchronizerId,
        )

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
          {
            // 3. revoke participant1's DTC on daId
            participant1.topology.synchronizer_trust_certificates.propose(
              participant1.id,
              synchronizerId,
              store = Some(synchronizerId),
              change = TopologyChangeOp.Remove,
              // as soon as the sequencer will see that the trust certificate is revoked, it will remove the participant and thus we cannot synchronize this topology change
              synchronize = None,
            )
            // 4. create the proposal to host participant1's admin party on participant2
            eventually() {
              participant2.topology.synchronizer_trust_certificates.list(
                operation = Some(TopologyChangeOp.Remove),
                store = Some(synchronizerId),
                filterUid = participant1.id.filterString,
              ) should not be empty
            }

            val ptpProposal = participant2.topology.party_to_participant_mappings.propose(
              participant1.id.adminParty,
              newParticipants = Seq(participant2.id -> ParticipantPermission.Submission),
              store = synchronizerId,
            )

            // 5. sign the proposal by participant1
            // this won't happen on CN, because participant2 would be in the same namespace as
            // participant1, so there would be no need to go through the hassle of collecting participant1's
            // signature. in this test though, we'll have to sign with both participant1 and participant2
            val ptpProposalSignedByParticipant3 =
              participant1.topology.transactions.sign(Seq(ptpProposal), TopologyStoreId.Authorized)

            participant2.topology.transactions
              .load(ptpProposalSignedByParticipant3, store = synchronizerId)

            // verify that the party is now available on participant2
            eventually() {
              participant2.topology.party_to_participant_mappings
                .is_known(synchronizerId, participant1.adminParty, Seq(participant2)) shouldBe true

              participant2.ownParties(filterSynchronizerId =
                synchronizerId
              ) should contain theSameElementsAs
                Set(participant1.id.adminParty, participant2.id.adminParty, alice, charlie, party3)
            }
          },
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq(
              (
                _.shouldBeCantonErrorCode(
                  PermissionDenied
                ),
                "MemberAuthenticationService terminates connection",
              ),
              (
                _.shouldBeCantonError(
                  SyncServiceSynchronizerDisabledUs,
                  _ should include(s"$daName rejected our subscription"),
                ),
                "CantonSyncService disconnect",
              ),
            ),
            mayContain = Seq(
              _.warningMessage should include("Token refresh aborted due to shutdown"),
              /* Participant3 may attempt to connect to the sequencer (e.g., via a health check), but fail
               * because the connection to the sequencer has been "revoked".
               */
              _.warningMessage should include("Request failed for sequencer."),
              _.warningMessage should include("RequestFailed(No connection available)"),
              _.warningMessage should include("failed the following topology transactions"),
            ),
          ),
        )
    }
  }
}
