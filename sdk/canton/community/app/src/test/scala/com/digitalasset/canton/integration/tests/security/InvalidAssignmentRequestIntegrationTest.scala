// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{ContractsReassignmentBatch, UnassignmentData}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.AcsInspection
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.protocol.{ExampleContractFactory, LfContractId, ReassignmentId}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.daml.lf.transaction.CreationTime
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicReference

/** The goal is to check that an invalid assignment requests are rejected by an honest participant
  * in phase 3 and/or an honest participant in phase 7. This should lead to the assignment being
  * rejected, which can be detected by the contract being inactive on both the source (unassignment
  * was successful) and target (assignment was rejected) synchronizers.
  *
  * Topology:
  *   - Synchronizers: da (source) and acme (target)
  *   - signatory -> P1 (honest)
  *   - observer -> P2 (malicious)
  */
final class InvalidAssignmentRequestIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with AcsInspection
    with SecurityTestHelpers {

  private var signatory: PartyId = _
  private var observer: PartyId = _

  private var maliciousP2: MaliciousParticipantNode = _

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
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

        // acme (target for reassignments)
        pureCryptoRef.set(sequencer2.crypto.pureCrypto)

        maliciousP2 = MaliciousParticipantNode(
          participant2,
          acmeId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  private def createContractAndUnassign()(implicit
      env: TestConsoleEnvironment
  ): (Iou.Contract, UnassignmentData) = {
    import env.*

    val iou = IouSyntax.createIou(participant1, Some(daId))(signatory, observer, 1000)

    val unassigned = participant2.ledger_api.commands.submit_unassign(
      observer,
      Seq(LfContractId.assertFromString(iou.id.contractId)),
      daId,
      acmeId,
    )

    val reassignmentId = ReassignmentId.tryCreate(unassigned.reassignmentId)

    val reassignmentData = participant2.underlying.value.sync.syncPersistentStateManager
      .get(acmeId)
      .value
      .reassignmentStore
      .lookup(reassignmentId)
      .failOnShutdown
      .futureValue

    (iou, reassignmentData)
  }

  private def alteredUnassignmentData(unassignmentData: UnassignmentData): UnassignmentData = {
    // Increment createdAt by 1 micro for all contracts
    val contractsUpdatedCreatedAt = ContractsReassignmentBatch.create {
      unassignmentData.contractsBatch.contracts.map { reassign =>
        val CreationTime.CreatedAt(t0) = reassign.contract.inst.createdAt
        val t1 = CreationTime.CreatedAt(t0.addMicros(1))
        (
          ExampleContractFactory.modify(reassign.contract, createdAt = Some(t1)),
          reassign.sourceValidationPackageId,
          reassign.targetValidationPackageId,
          reassign.counter,
        )
      }
    }.value

    unassignmentData.copy(contractsBatch = contractsUpdatedCreatedAt)
  }

  "Validation of assignment request during phase 3" should {
    "succeed in happy path" in { implicit env =>
      import env.*

      val (iou, reassignmentData) = createContractAndUnassign()

      maliciousP2
        .submitAssignmentRequest(observer.toLf, reassignmentData, Some(environment.now))
        .futureValueUS
        .value

      // Reassignment successful
      eventually() {
        assertContractState(
          participant2,
          LfContractId.assertFromString(iou.id.contractId),
          activeOn = Seq(acmeName),
          inactiveOn = Seq(daName),
        )
      }
    }

    "reject when assignment request has wrong recipient" in { implicit env =>
      import env.*

      val (iou, reassignmentData) = createContractAndUnassign()

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
          maliciousP2
            .submitAssignmentRequest(
              observer.toLf,
              reassignmentData,
              Some(environment.now),
              overrideRecipients = Recipients.ofSet(
                Set(
                  participant1.member,
                  participant2.member,
                  participant3.member, // extra recipient
                )
              ),
            )
            .futureValueUS
            .value

          // Unassignment done, and assignment request is rejected
          eventually() {
            assertContractState(
              participant2,
              LfContractId.assertFromString(iou.id.contractId),
              activeOn = Seq(),
              inactiveOn = Seq(daName, acmeName),
            )

            assertContractState(
              participant1,
              LfContractId.assertFromString(iou.id.contractId),
              activeOn = Seq(),
              inactiveOn = Seq(daName, acmeName),
            )
          }

          participant2.health.ping(participant1)
        },
        logAssertions *,
      )
    }

    "fail with ModelConformanceError when contract has been tampered with" in { implicit env =>
      import env.*

      val (iou, reassignmentData) = createContractAndUnassign()

      def warnFailedModelConformanceCheck(participantName: String)(entry: LogEntry): Assertion = {
        entry.loggerName should include(s"participant=$participantName")
        entry.warningMessage should (
          include("Rejected transaction due to a failed model conformance check")
            and
              include(s"contract authentication failure for ${iou.id}")
        )
      }

      def errorMediatorApprovedLocalReject(participantName: String)(entry: LogEntry): Assertion = {
        entry.loggerName should include(s"participant=$participantName")
        val errMsg = entry.throwable.fold(entry.errorMessage)(_.getMessage)
        errMsg should include("Mediator approved a request that we have locally rejected")
      }

      def oneOrMore(assertion: LogEntry => Assertion) = Seq(
        LogEntryOptionality.Required -> assertion,
        LogEntryOptionality.OptionalMany -> assertion,
      )

      val logAssertions: Seq[(LogEntryOptionality, LogEntry => Assertion)] =
        oneOrMore(warnFailedModelConformanceCheck("participant1")) ++
          oneOrMore(warnFailedModelConformanceCheck("participant2")) ++
          oneOrMore(errorMediatorApprovedLocalReject("participant1")) ++
          oneOrMore(errorMediatorApprovedLocalReject("participant2"))

      val (_, messages) = loggerFactory.assertLogsUnorderedOptional(
        replacingConfirmationResult(
          acmeId,
          sequencer2,
          mediator2,
          withMediatorVerdict(mediatorApprove),
        ) {
          maliciousP2
            .submitAssignmentRequest(
              observer.toLf,
              alteredUnassignmentData(reassignmentData),
              Some(environment.now),
            )
            .futureValueUS
            .value

          participant1.health.ping(participant2)

          // Reassignment is incomplete
          assertContractState(
            participant2,
            LfContractId.assertFromString(iou.id.contractId),
            activeOn = Seq(),
            inactiveOn = Seq(daName, acmeName),
          )

          assertContractState(
            participant1,
            LfContractId.assertFromString(iou.id.contractId),
            activeOn = Seq(),
            inactiveOn = Seq(daName, acmeName),
          )
        },
        logAssertions *,
      )

      /** So far, we know that the behavior in phase 7 is correct. Now, we also check that both
        * participants rejected the assignment.
        *
        * Notes:
        *   - P2 confirms even if it hosts only an observer because it is the submitter of the
        *     assignment request.
        */
      ProgrammableSequencer.confirmationResponsesKind(messages) shouldBe Map(
        participant1.id -> List("LocalReject"),
        participant2.id -> List("LocalReject"),
      )
    }
  }
}
