// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{FullUnassignmentTree, ReassignmentRef, UnassignmentViewTree}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.StakeholdersMismatch
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.{
  ContractInstance,
  ContractMetadata,
  ExampleContractFactory,
  Stakeholders,
}
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.UUID

/** The goal is to check that an invalid an unassignment request is rejected during phase 3 by an
  * honest participant. This should lead to the unassignment being rejected.
  *
  * Topology:
  *   - Synchronizers: da (source) and acme (target)
  *   - signatory -> P1 (honest)
  *   - observer -> P2 (malicious)
  */
class InvalidUnassignmentRequestIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection {

  private var signatory: PartyId = _
  private var observer: PartyId = _
  private var otherParty: PartyId = _

  private var maliciousP2: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(ProgrammableSequencer.configOverride(getClass.toString, loggerFactory))
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        signatory = participant1.parties.enable("signatory", synchronizer = daName)
        participant1.parties.enable("signatory", synchronizer = acmeName)

        observer = participant2.parties.enable("observer", synchronizer = daName)
        participant2.parties.enable("observer", synchronizer = acmeName)

        otherParty = participant2.parties.enable("otherParty", synchronizer = daName)
        participant2.parties.enable("otherParty", synchronizer = acmeName)

        maliciousP2 = MaliciousParticipantNode(
          participant2,
          daId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  private def createContract()(implicit
      env: TestConsoleEnvironment
  ): ContractInstance = {
    import env.*

    val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer, 1000)
    participant1.testing
      .acs_search(daName, exactId = iou.id.contractId, limit = PositiveInt.one)
      .loneElement
  }

  private def createFullUnassignmentTree(contract: ContractInstance, index: Long)(implicit
      env: TestConsoleEnvironment
  ): FullUnassignmentTree = {
    import env.*
    val pureCrypto = participant2.underlying.value.sync.syncCrypto
      .forSynchronizer(daId, staticSynchronizerParameters1)
      .value
      .pureCrypto

    val helpers = ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = Source(daId),
      targetSynchronizer = Target(acmeId),
      pureCrypto = pureCrypto,
      targetTimestamp = Target(environment.clock.now),
    )

    val uuid = new UUID(10L, index)
    val seed = new SeedGenerator(pureCrypto).generateSaltSeed()

    helpers
      .unassignmentRequest(
        observer.toLf,
        participant2,
        MediatorGroupRecipient(NonNegativeInt.zero),
      )(Set(participant1, participant2))
      .toFullUnassignmentTree(pureCrypto, pureCrypto, seed, uuid)
  }

  private def unassign(
      contract: ContractInstance,
      commonDataStakeholders: Stakeholders,
      index: Long,
      overrideRecipients: Option[Recipients] = None,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val pureCrypto = participant2.underlying.value.sync.syncCrypto
      .forSynchronizer(daId, staticSynchronizerParameters1)
      .value
      .pureCrypto

    val unassignmentRequest = createFullUnassignmentTree(contract, index)
    val commonData = createFullUnassignmentTree(
      ExampleContractFactory.modify(
        contract,
        metadata = Some(
          ContractMetadata.tryCreate(
            commonDataStakeholders.signatories,
            commonDataStakeholders.all,
            None,
          )
        ),
      ),
      index,
    ).tree.commonData

    // If `commonDataStakeholders` != `Stakeholders(contract.metadata)`, then this one is invalid
    val updatedUnassignmentRequest = FullUnassignmentTree(
      UnassignmentViewTree(
        commonData,
        unassignmentRequest.tree.view,
        Source(testedProtocolVersion),
        pureCrypto,
      )
    )

    maliciousP2
      .submitUnassignmentRequest(
        updatedUnassignmentRequest,
        Some(environment.now),
        overrideRecipients = overrideRecipients,
      )
      .futureValueUS
      .value
  }

  "Validation of unassignment request during phase 3" should {

    "succeed in happy path" in { implicit env =>
      import env.*

      val contract = createContract()
      unassign(contract, Stakeholders(contract.metadata), index = 0)

      // Unassignment successful
      eventually() {
        assertContractState(
          participant1,
          contract.contractId,
          activeOn = Seq(),
          inactiveOn = Seq(daName),
        )
      }
    }

    "reject when unassignment has wrong recipient" in { implicit env =>
      import env.*

      val contract = createContract()

      val logAssertions: Seq[(LogEntryOptionality, LogEntry => Assertion)] =
        Seq(
          // p1
          LogEntryOptionality.Required -> { entry =>
            entry.loggerName should include regex s".*participant=participant1.*"
            entry.warningMessage should include regex
              """Received a request with id RequestId\((.*?)\) where the view at List\(\) has extra recipients Set\(MemberRecipient\(PAR::participant3.*?\)\) for the view at List\(\)\. Continue processing\.\.\.""".r

          },
          LogEntryOptionality.Required -> { entry =>
            entry.loggerName should include regex s".*participant=participant2.*"
            entry.warningMessage should include regex
              """Received a request with id RequestId\((.*?)\) where the view at List\(\) has extra recipients Set\(MemberRecipient\(PAR::participant3.*?\)\) for the view at List\(\)\. Continue processing\.\.\.""".r

          },
          LogEntryOptionality.Required -> { entry =>
            entry.loggerName should include regex s".*participant=participant3.*"
            entry.warningMessage should include(
              s"No valid root hash message in batch"
            )
          },
        )

      loggerFactory.assertLogsUnorderedOptional(
        {
          unassign(
            contract,
            Stakeholders(contract.metadata),
            index = 2,
            overrideRecipients = Recipients.ofSet(
              Set(participant1.member, participant2.member, participant3.member)
            ), // extra recipient
          )

          // Unassignment not done: contract still assigned to da
          eventually() {
            assertContractState(
              participant1,
              contract.contractId,
              activeOn = Seq(daName),
              inactiveOn = Seq(),
            )
          }

          participant2.health.ping(participant1)
        },
        logAssertions *,
      )
    }

    "reject when contract has been tampered with wrong stakeholders" in { implicit env =>
      import env.*

      val contract = createContract()
      // otherParty is added
      val correctStakeholders = Stakeholders(contract.metadata)
      val incorrectStakeholders = Stakeholders.withSignatoriesAndObservers(
        Set(signatory.toLf),
        Set(observer.toLf, otherParty.toLf),
      )

      val expectedError = StakeholdersMismatch(
        ReassignmentRef(contract.contractId),
        incorrectStakeholders,
        correctStakeholders,
      )

      def rejectedOn(participantName: String)(entry: LogEntry) = {
        entry.loggerName should include regex s".*participant=$participantName.*"
        entry.warningMessage should include(
          s"Rejected transaction due to a failed model conformance check: $expectedError"
        )
      }

      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        unassign(contract, incorrectStakeholders, index = 1),
        logs => {
          forAtLeast(1, logs)(rejectedOn("participant1"))
          forAtLeast(1, logs)(rejectedOn("participant2"))
        },
      )

      // Unassignment not done: contract still assigned to da
      assertContractState(
        participant1,
        contract.contractId,
        activeOn = Seq(daName),
        inactiveOn = Seq(),
      )

    }
  }
}
