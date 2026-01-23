// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import cats.syntax.either.*
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.test.evidence.scalatest.ScalaTestSupport.TagContainer
import com.daml.test.evidence.tag.EvidenceTag
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, FeatureFlag}
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.participant.admin.data.RepairContract
import com.digitalasset.canton.participant.util.JavaCodegenUtil.ContractIdSyntax
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ContractIdAbsolutizer.ContractIdAbsolutizationDataV1
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.util.TestEngine
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfVersioned,
  NeedsNewLfContractIds,
  ReassignmentCounter,
  SynchronizerAlias,
  config,
}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.CreationTime
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ValueParty, ValueRecord}
import org.scalatest.{Assertion, Tag}

import java.time.Duration
import java.util.UUID
import scala.annotation.nowarn
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

/** The RepairService"Integration"Test is more of a unit test addressing coverage of the
  * RepairService matrix of actually encountered contract states and expected contract states, but
  * implemented using the integration test framework. Accordingly the test checks are based on
  * testing.pcs_search to test the outcomes (not always visible via the ledger api in the case of
  * move).
  */
@nowarn("msg=match may not be exhaustive")
sealed trait RepairServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with RepairTestUtil
    with NeedsNewLfContractIds {

  protected def cantonTestsPath: String

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair)
      )

  override val defaultParticipant: String = "participant1"

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var initialized = false

  // Placing this helper in every test enables running any subset of tests in any order and provides syntactic sugar
  // providing parties Alice and Bob to every test.
  protected def withParticipantsInitialized[A](
      test: (PartyId, PartyId) => A
  )(implicit env: TestConsoleEnvironment): A = {
    import env.*

    if (!initialized) {
      // Disable reconciliation as we will issue repair commands that will result
      // in different contracts on P1 and P2 on the da and acme synchronizers.
      Seq(sequencer1 -> daId, sequencer2 -> acmeId).foreach { case (sequencer, synchronizerId) =>
        sequencer.topology.synchronizer_parameters.propose_update(
          synchronizerId = synchronizerId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )
      }

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant1.dars.upload(cantonTestsPath, synchronizerId = daId)
      participant1.dars.upload(cantonTestsPath, synchronizerId = acmeId)
      eventually()(assert(participant1.synchronizers.is_connected(daId)))

      participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
      // do not synchronise the vetting transactions:
      participant2.dars.upload(CantonExamplesPath, synchronizeVetting = false)
      eventually()(
        assert(participant2.synchronizers.is_connected(acmeId))
      )

      PartiesAllocator(Set(participant1, participant2))(
        newParties = Seq(aliceS -> participant1, bobS -> participant1),
        targetTopology = Map(
          aliceS -> Map(
            daId -> (PositiveInt.one, Set(participant1.id -> Submission)),
            acmeId -> (PositiveInt.one, Set(
              participant1.id -> Submission,
              participant2.id -> Submission,
            )),
          ),
          bobS -> Map(
            daId -> (PositiveInt.one, Set(participant1.id -> Submission)),
            acmeId -> (PositiveInt.one, Set(participant1.id -> Submission)),
          ),
        ),
      )

      // ensure all participants have observed a point after the topology changes before disconnecting them
      participants.local.foreach(_.testing.fetch_synchronizer_times())

      participant1.synchronizers.disconnect(acmeName)
      participant1.synchronizers.disconnect(daName)
      initialized = true
    }

    val Seq(alice, bob) = Seq(aliceS, bobS).map(_.toPartyId(participant1))
    test(alice, bob)
  }

  def withSynchronizerConnected[A](
      synchronizerAlias: SynchronizerAlias
  )(code: => A)(implicit env: TestConsoleEnvironment): A = {
    import env.*
    participant1.synchronizers.reconnect(synchronizerAlias)
    try {
      code
    } finally {
      participant1.synchronizers.disconnect(synchronizerAlias)
    }
  }

  def withContractFromParticipant2AndAcme[A](alice: PartyId, bob: PartyId)(
      code: RepairContract => A
  )(implicit env: TestConsoleEnvironment): A = {
    import env.*

    val contract = createContractInstance(participant2, acmeName, acmeId, alice, bob)

    try {
      code(contract)
    } finally {
      withSynchronizerConnected(acmeName) {
        withClue(
          "Failed to observe contract on acme on participant1 to ensure p1.acme reaches clean head"
        )(
          eventually() {
            participant1.testing
              .pcs_search(acmeName, contract.contract.contractId.coid)
              .nonEmpty shouldBe true
          }
        )
      }
    }
  }
}

sealed trait RepairServiceIntegrationTestStableLf
    extends RepairServiceIntegrationTest
    with SecurityTestSuite {
  override protected def cantonTestsPath: String = CantonTestsPath

  // Workaround to avoid false errors reported by IDEA.
  implicit def tagToContainer(tag: EvidenceTag): Tag = new TagContainer(tag)

  "RepairService" should {
    "prevent concurrent synchronizer reconnect" when {
      "a repair command is being processed" in { implicit env =>
        import env.*
        var runningRepair = false
        withParticipantsInitialized { (_, _) =>
          try {
            val promise = Promise[Unit]()
            // Create a fake repair command that we push to the queue
            val repairF = participant1.underlying.value.sync.repairService.executionQueue
              .execute(
                {
                  runningRepair = true
                  promise.future
                },
                "Test repair command",
              )
              .unwrap

            eventually() {
              runningRepair shouldBe true
            }

            val synchronizerReconnectF = Future(participant1.synchronizers.reconnect(daName))

            always() {
              repairF.isCompleted shouldBe false
              // Synchronizer reconnection should not run until the repair command is completed
              synchronizerReconnectF.isCompleted shouldBe false
            }

            promise.success(())

            timeouts.default.await_("repair command")(repairF)
            timeouts.default.await_("reconnect command")(synchronizerReconnectF)
          } finally {
            participant1.synchronizers.disconnect(daName)
          }
        }
      }
    }
  }

  "RepairService.add_contract" should {
    "add contract" when {
      "contract doesn't exist yet (local version)" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          withContractFromParticipant2AndAcme(alice, bob) { contractInstance =>
            import env.*
            participant1.repair.add(daId, testedProtocolVersion, Seq(contractInstance))

            withSynchronizerConnected(daName) {
              eventually() {
                val res = participant1.testing.pcs_search(
                  daName,
                  exactId = contractInstance.contract.contractId.coid,
                )
                res.headOption.map(_._1) shouldBe Some(true)
              }
            }
          }
        }
      }

      "contract doesn't exist yet (remote version)" in { implicit env =>
        import env.*
        def queryContracts(): Seq[CreatedEvent] =
          participant1.ledger_api.state.acs.of_all().collect {
            case entry if entry.synchronizerId.contains(daId.logical) => entry.event
          }

        withParticipantsInitialized { (alice, bob) =>
          val c1 = createContractInstance(participant2, acmeName, acmeId, alice, bob)
            .copy(representativePackageId = "should-not-be-used")
          val c2 = createContractInstance(participant2, acmeName, acmeId, alice, bob)

          val acsBeforeRepair = queryContracts().toSet
          participant1_.repair.add(daId, testedProtocolVersion, Seq(c1, c2))

          withSynchronizerConnected(daName) {
            eventually() {
              val acsAfterRepair = queryContracts()
              val newContracts = acsAfterRepair.filterNot(acsBeforeRepair)

              newContracts should have size 2

              newContracts.zip(Seq(c1, c2)).foreach {
                case (queriedCreatedEvent, expectedRepairContract) =>
                  queriedCreatedEvent.contractId shouldBe expectedRepairContract.contractId.coid
                  CantonTimestamp
                    .fromProtoTimestamp(queriedCreatedEvent.createdAt.value)
                    .value
                    .underlying shouldBe expectedRepairContract.contract.createdAt.time

                  // Limitation: ImportAcsOld assigns the representative package ID as the original contract package ID
                  // TODO(#24610): Adapt to assert that the representative package ID of the repair contract is used
                  queriedCreatedEvent.representativePackageId shouldBe expectedRepairContract.contract.templateId.packageId
              }
            }
          }
        }
      }

      // TODO(#24610) - Un-ignore this test part once #27325 has been re-implemented; depends on repair.add
      "contract has been unassigned" taggedAs SecurityTest(
        SecurityTest.Property.Integrity,
        "virtual shared ledger",
        Attack(
          "a participant",
          "initiates an unassignment, but the assignment cannot be completed due to concurrent topology changes",
          "resurrect the contract via repair",
        ),
      ) ignore { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contractInstance = withSynchronizerConnected(daName) {
            val created = createContract(participant1, alice, bob)
            val contract = readContractInstance(participant1, daName, daId, created)
            withSynchronizerConnected(acmeName) {
              participant1.ledger_api.commands
                .submit_unassign(
                  bob,
                  Seq(created.toLf),
                  daId,
                  acmeId,
                )
            }.discard

            eventually() {
              val res =
                participant1.testing.pcs_search(
                  daName,
                  exactId = contract.contract.contractId.coid,
                )
              res.headOption.map(_._1) shouldBe Some(false)
            }

            // Create another contract to bump clean head
            createContractInstance(participant1, daName, daId, alice, bob).discard

            // Adjust the reassignment counter such that the contract can be added again (participant1.repair.add);
            // assuming the acme synchronizer disappears after this unassignment, following a recovery of the contract
            // to the da synchronizer
            contract.copy(reassignmentCounter = ReassignmentCounter(2))
          }

          // TODO(#24610) - Note that the repair.add fails because it goes through ACS import (Old)
          participant1.repair.add(daId, testedProtocolVersion, Seq(contractInstance))

          // Ideally we should be able to query the contract as active
          withSynchronizerConnected(daName) {
            eventually() {
              val res = participant1.testing.pcs_search(
                daName,
                exactId = contractInstance.contract.contractId.coid,
              )
              res.headOption.map(_._1) shouldBe Some(true)
            }
          }
        }
      }

      "contract is purged" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val created = withSynchronizerConnected(daName) {
            createContract(participant1, alice, bob)
          }
          val repairContract = readContractInstance(participant1, daName, daId, created)

          participant1.repair.purge(daName, Seq(repairContract.contract.contractId))
          participant1.repair.add(daId, testedProtocolVersion, Seq(repairContract))
        }
      }
    }

    "not add contract" when {
      "contract is archived" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contractInstance = withSynchronizerConnected(daName) {
            createArchivedContractInstance(participant1, daName, daId, alice, bob)
          }

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair.add(daId, testedProtocolVersion, Seq(contractInstance)),
            _.commandFailureMessage should include(
              "Cannot add previously archived contract ContractId("
            ),
          )
        }
      }

      "a different contract with same contract id is active" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val (contractUsd, contractChf) = withSynchronizerConnected(daName) {
            (
              createContractInstance(participant1, daName, daId, alice, bob, "USD"),
              createContractInstance(participant1, daName, daId, alice, bob, "CHF"),
            )
          }

          val contractChfWithUsdContractId = {
            val fci = contractChf.contract
            contractChf.copy(contract =
              LfFatContractInst.fromCreateNode(
                fci.toCreateNode.copy(coid = contractUsd.contract.contractId),
                fci.createdAt,
                fci.authenticationData,
              )
            )
          }

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair.add(daId, testedProtocolVersion, Seq(contractChfWithUsdContractId)),
            _.commandFailureMessage should include(
              "Failed to authenticate contract with id"
            ),
          )
        }
      }

      // TODO(#24610): Add test cases for ContractImportMode.ACCEPT to showcase that authentication is bypassed
      "contract authentication fails" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contract = withSynchronizerConnected(daName) {
            createContractInstance(participant1, daName, daId, alice, bob, "YEN")
          }

          val modifiedContract = {
            val fci = contract.contract
            contract.copy(contract =
              LfFatContractInst.fromCreateNode(
                fci.toCreateNode,
                fci.createdAt.copy(fci.createdAt.time.add(Duration.ofSeconds(1337L))),
                fci.authenticationData,
              )
            )
          }

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair.add(
              acmeId,
              testedProtocolVersion,
              Seq(modifiedContract),
            ),
            _.commandFailureMessage should include(s"Failed to authenticate contract with id"),
          )
        }
      }
    }
  }

  "RepairService.purge_contract" should {
    "purge contract" when {
      "contract is active" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contract = withSynchronizerConnected(daName) {
            createContractInstance(participant1, daName, daId, alice, bob)
          }

          participant1.repair.purge(daName, Seq(contract.contract.contractId))

          withSynchronizerConnected(daName) {
            eventually() {
              val res =
                participant1.testing.pcs_search(
                  daName,
                  exactId = contract.contract.contractId.coid,
                )
              res.headOption.map(_._1) shouldBe Some(false)
            }
          }
        }
      }

      "contract is reassigned away" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contractId = withSynchronizerConnected(daName) {
            val cid = createContract(participant1, alice, bob)
            withSynchronizerConnected(acmeName) {
              participant1.ledger_api.commands
                .submit_unassign(bob, Seq(cid.toLf), daId, acmeId)
            }.discard
            cid
          }

          participant1.repair.purge(daName, Seq(contractId.toLf))

          withSynchronizerConnected(daName) {
            eventually() {
              val res = participant1.testing.pcs_search(daName, exactId = contractId.toLf.coid)
              res.headOption.map { case (isActive, _) => isActive } shouldBe Some(false)
            }
          }
        }
      }
    }

    "error purging contract" when {
      "no contract id to purge specified" in { implicit env =>
        withParticipantsInitialized { (_, _) =>
          import env.*
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair.purge(daName, Seq.empty, ignoreAlreadyPurged = false),
            _.commandFailureMessage should include("Missing contract ids to purge"),
          )
        }
      }

      "contract doesn't exist or is archived und ignore flag not set" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          withContractFromParticipant2AndAcme(alice, bob) { contractInstance =>
            import env.*

            loggerFactory.assertThrowsAndLogs[CommandFailure](
              participant1.repair.purge(
                daName,
                Seq(contractInstance.contract.contractId),
                ignoreAlreadyPurged = false,
              ),
              _.commandFailureMessage should include("cannot be purged: unknown contract"),
            )

            val contractIdToBeArchived =
              withSynchronizerConnected(daName) {
                createArchivedContract(participant1, alice, bob)
              }

            loggerFactory.assertThrowsAndLogs[CommandFailure](
              participant1.repair
                .purge(daName, Seq(contractIdToBeArchived.toLf), ignoreAlreadyPurged = false),
              _.commandFailureMessage should include("cannot be purged: archived contract"),
            )
          }
        }
      }
    }
  }

  "repair.change_assignation" should {
    "update contracts" when {
      "contract active at source and not existing at target" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val cidActive = withSynchronizerConnected(daName) {
            createContract(participant1, alice, bob).toLf
          }
          participant1.repair.change_assignation(
            Seq(cidActive),
            daName,
            acmeName,
            skipInactive = false,
          )

          withSynchronizerConnected(daName) {
            participant1.testing
              .pcs_search(daName, exactId = cidActive.coid)
              .headOption
              .map(_._1) shouldBe Some(false)
          }

          withSynchronizerConnected(acmeName) {
            participant1.testing
              .pcs_search(acmeName, exactId = cidActive.coid)
              .headOption
              .map(_._1) shouldBe Some(true)
          }
        }
      }

      "move back and forth a contract" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contract = withSynchronizerConnected(daName) {
            createContractInstance(participant1, daName, daId, alice, bob)
          }
          val cid = contract.contract.contractId

          val synchronizers = Seq(acmeName, daName)

          def checkContractActiveness(where: SynchronizerAlias): Assertion =
            forAll(synchronizers) { synchronizer =>
              withSynchronizerConnected(synchronizer) {
                participant1.testing
                  .pcs_search(synchronizer, exactId = cid.coid)
                  .headOption
                  .map(_._1) shouldBe Some(synchronizer == where)
              }
            }

          // Move contract there and back
          participant1.repair.change_assignation(Seq(cid), daName, acmeName, skipInactive = false)
          participant1.repair.change_assignation(Seq(cid), acmeName, daName, skipInactive = false)

          checkContractActiveness(daName)

          // Now another three rounds
          participant1.repair.change_assignation(Seq(cid), daName, acmeName, skipInactive = false)
          participant1.repair.change_assignation(Seq(cid), acmeName, daName, skipInactive = false)
          participant1.repair.change_assignation(Seq(cid), daName, acmeName, skipInactive = false)

          checkContractActiveness(acmeName)
        }
      }

      "allow changing the assignation of purged contracts" in { implicit env =>
        /*
        Ensures that in the following scenario
          da   ------- create --------- purge -----------
          acme ----------------------------------- add ---
        we can do a change_assignation from acme to da.
         */

        withParticipantsInitialized { (alice, _) =>
          import env.*

          val contract = withSynchronizerConnected(daName) {
            createContractInstance(participant1, daName, daId, alice, alice)
          }
          val cid = contract.contract.contractId

          participant1.repair.purge(daName, Seq(cid))
          participant1.repair.add(acmeId, testedProtocolVersion, Seq(contract))
          participant1.repair.change_assignation(Seq(cid), acmeName, daName)
        }
      }
    }

    "error moving contracts" when {
      "contract does not exist at source" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          withContractFromParticipant2AndAcme(alice, bob) { contractInstance =>
            import env.*
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              participant1.repair.change_assignation(
                Seq(contractInstance.contract.contractId),
                daName,
                acmeName,
                skipInactive = false,
              ),
              _.commandFailureMessage should (include(
                "Cannot change contract assignation"
              ) and include("does not exist in source synchronizer")),
            )
          }
        }
      }

      "contract is archived at source" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          withContractFromParticipant2AndAcme(alice, bob) { _ =>
            import env.*
            val cidArchived = withSynchronizerConnected(daName) {
              createArchivedContract(participant1, alice, bob).toLf
            }
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              participant1.repair
                .change_assignation(Seq(cidArchived), daName, acmeName, skipInactive = false),
              _.commandFailureMessage should (include(
                "Cannot change contract assignation"
              ) and include("has been archived")),
            )
          }
        }
      }

      "modified contract with same contract id already exists" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val (activeContract, activeContractModified) = withSynchronizerConnected(daName) {
            (
              createContractInstance(participant1, daName, daId, alice, bob, "USD"),
              createContractInstance(participant1, daName, daId, alice, bob, "CHF"),
            )
          }

          val cidActive = activeContract.contract.contractId
          val modifiedContractId = {
            val fci = activeContractModified.contract
            activeContractModified.copy(contract =
              LfFatContractInst.fromCreateNode(
                fci.toCreateNode.copy(coid = cidActive),
                fci.createdAt,
                fci.authenticationData,
              )
            )
          }

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair.add(
              acmeId,
              testedProtocolVersion,
              Seq(modifiedContractId),
            ),
            _.commandFailureMessage should include(
              s"Failed to authenticate contract with id"
            ),
          )
        }
      }

      "contract already exists at target" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val (activeContract1, activeContract2) = withSynchronizerConnected(daName) {
            (
              createContractInstance(participant1, daName, daId, alice, bob),
              createContractInstance(participant1, daName, daId, alice, bob),
            )
          }

          val cidActive1 = activeContract1.contract.contractId
          val cidActive2 = activeContract2.contract.contractId

          participant1.repair.add(acmeId, testedProtocolVersion, Seq(activeContract1))
          participant1.repair.add(acmeId, testedProtocolVersion, Seq(activeContract2))

          participant1.repair.purge(
            acmeName,
            Seq(cidActive1),
          ) // purge contract1 in target synchronizer

          withSynchronizerConnected(acmeName) {
            participant1.testing
              .pcs_search(acmeName, exactId = cidActive1.coid)
              .headOption
              .map(_._1) shouldBe Some(false)
          }

          participant1.repair.add(acmeId, testedProtocolVersion, Seq(activeContract2))

          withSynchronizerConnected(acmeName) {
            participant1.testing
              .pcs_search(acmeName, exactId = cidActive1.coid)
              .headOption
              .map(_._1) shouldBe Some(false)
          }

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair
              .change_assignation(Seq(cidActive2), daName, acmeName, skipInactive = false),
            _.commandFailureMessage should include(
              "in source synchronizer exists in target synchronizer with status Active"
            ),
          )
        }
      }

      "contract has been unassigned of source" in { implicit env =>
        withParticipantsInitialized { (alice, bob) =>
          import env.*

          val contractInstance = withSynchronizerConnected(daName) {
            val created = createContract(participant1, alice, bob)
            val contract = readContractInstance(participant1, daName, daId, created)
            withSynchronizerConnected(acmeName) {
              participant1.ledger_api.commands
                .submit_unassign(bob, Seq(created.toLf), daId, acmeId)
            }.discard
            contract
          }
          val contractId = contractInstance.contract.contractId
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair
              .change_assignation(Seq(contractId), daName, acmeName, skipInactive = false),
            _.commandFailureMessage should include("has been reassigned to "),
          )
        }
      }

      "contract stakeholders are not hosted on target synchronizer" in { implicit env =>
        withParticipantsInitialized { (_, _) =>
          import env.*

          val contractInstance = withSynchronizerConnected(daName) {
            // Create a fresh party that only exists on the source synchronizer
            // because the participant has not yet connected to the target synchronizer when we move the contract
            val charlie =
              participant1.parties.enable(
                "Charlie",
                synchronizeParticipants = Nil,
                synchronizer = daName,
              )

            val created = createContract(participant1, charlie, charlie)
            val contract = readContractInstance(participant1, daName, daId, created)
            contract
          }
          val contractId = contractInstance.contract.contractId

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.repair
              .change_assignation(Seq(contractId), daName, acmeName, skipInactive = false),
            _.commandFailureMessage should include("without at least one stakeholder of"),
          )
        }
      }
    }
  }
}

sealed trait RepairServiceIntegrationTestDevLf extends RepairServiceIntegrationTest {
  override def cantonTestsPath: String = CantonTestsDevPath

  "RepairService.addContract" should {
    "not add contract" when {
      "contract has empty maintainers" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
        implicit env =>
          withParticipantsInitialized { (alice, _) =>
            import env.*

            val pureCrypto = participant1.underlying.map(_.cryptoPureApi).value
            val authenticatedContractIdVersion = CantonContractIdVersion.maxV1
            val creationTime = CreationTime.CreatedAt(environment.clock.now.toLf)
            val contractIdSuffixer =
              new ContractIdSuffixer(pureCrypto, authenticatedContractIdVersion)
            val contractIdAbsolutizer =
              new ContractIdAbsolutizer(pureCrypto, ContractIdAbsolutizationDataV1)

            // We can't create the contract with Canton, so we have to hand-craft it.
            val module = "BasicKeys"
            val template = "NoMaintainer"
            val pkg =
              participant1.packages.find_by_module(module).headOption.map(_.packageId).value

            val lfNoMaintainerTemplateId =
              LfTemplateId(
                Ref.PackageId.assertFromString(pkg),
                Ref.QualifiedName.assertFromString(s"$module:$template"),
              )
            val lfPackageName = Ref.PackageName.assertFromString("CantonTestsDev")
            val keyWithMaintainers = ExampleTransactionFactory.globalKeyWithMaintainers(
              LfGlobalKey.build(lfNoMaintainerTemplateId, Value.ValueUnit, lfPackageName).value,
              Set.empty,
            )

            val contractInst = LfThinContractInst(
              template = lfNoMaintainerTemplateId,
              packageName = lfPackageName,
              arg = LfVersioned(
                ExampleTransactionFactory.serializationVersion,
                ValueRecord(None, ImmArray(None -> ValueParty(alice.toLf))),
              ),
            )

            val contractSalt = ContractSalt.createV1(pureCrypto)(
              transactionUuid = new UUID(1L, 1L),
              psid = daId,
              mediator = MediatorGroupRecipient(MediatorGroupIndex.one),
              viewParticipantDataSalt = TestSalt.generateSalt(1),
              createIndex = 0,
              viewPosition = ViewPosition(List.empty),
            )
            val unsuffixedContractId = LfContractId.V1(ExampleTransactionFactory.lfHash(1337))
            val unsuffixedCreateNode = LfNodeCreate(
              coid = unsuffixedContractId,
              contract = contractInst,
              signatories = Set(alice.toLf),
              stakeholders = Set(alice.toLf),
              key = Some(keyWithMaintainers.unversioned),
            )

            val contractHash = TestEngine
              .syncContractHasher(cantonTestsPath)
              .hash(
                unsuffixedCreateNode,
                contractIdSuffixer.contractHashingMethod,
              )

            val ContractIdSuffixer.RelativeSuffixResult(
              suffixedCreateNode,
              _,
              _,
              authenticationData,
            ) = contractIdSuffixer
              .relativeSuffixForLocalContract(
                contractSalt,
                creationTime,
                unsuffixedCreateNode,
                contractHash,
              )
              .valueOr(err => fail(s"Failed to generate contract suffix: $err"))

            val suffixedContractInstance = LfFatContractInst.fromCreateNode(
              suffixedCreateNode,
              creationTime,
              authenticationData.toLfBytes,
            )
            val absolutizedContractInstance =
              contractIdAbsolutizer.absolutizeFci(suffixedContractInstance).value
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              participant1.repair.add(
                daId,
                testedProtocolVersion,
                Seq(
                  RepairContract(
                    daId,
                    absolutizedContractInstance,
                    ReassignmentCounter.Genesis,
                    absolutizedContractInstance.templateId.packageId,
                  )
                ),
              ),
              _.commandFailureMessage should (
                include("InvalidIndependentOfSystemState") and include(
                  "has key without maintainers"
                )
              ),
            )
          }
      }
    }
  }
}

sealed trait RepairServiceBftSequencerPostgresTest {
  self: SharedEnvironment =>
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )
}

class RepairServiceIntegrationTestPostgresStableLf
    extends RepairServiceIntegrationTestStableLf
    with RepairServiceBftSequencerPostgresTest

class RepairServiceIntegrationTestPostgresDevLf
    extends RepairServiceIntegrationTestDevLf
    with RepairServiceBftSequencerPostgresTest
