// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.tag.Security.SecurityTestSuite
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.UnassignmentData
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.protocol.messages.Verdict
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicReference

/** The intent of this test is to validate that a malicious node cannot cause other participants to
  * commit an assignment containing data that differs from data in the corresponding unassignment.
  *
  * Specfically:
  *   - a non-reassigning participant (not connected to both source and target), and
  *   - a reassigning participant that hosts a stakeholder that is not a signatory,
  *
  * should both fail to commit a doctored assignment request.
  *
  * We set up participant1 as a reassigning particpant that is malicious. It submits an unassignment
  * and then attempts to submit an assignment with the same reassignment id but different data,
  * approving it at stage 3.
  *
  * Meanwhile participant2 is also a reassigning participant. It can see that the assignment request
  * is invalid, but the party it hosts is not a signatory, so it does not get to reject at stage 3.
  *
  * Finally, participant3 is not connected to the source synchronizer, only the target, so it never
  * saw the unassignment and thus cannot compare the content of the assignment request against it.
  *
  * However participant2 and participant3 are both able to validate the contents of the assignment
  * request against the reassignment id used, and thus are able to reject as invalid at stage 7.
  */
sealed trait InvalidReassignmentIdIntegrationTest
    extends CommunityIntegrationTest
    with HasProgrammableSequencer
    with SecurityTestHelpers
    with HasCycleUtils
    with SecurityTestSuite
    with SharedEnvironment
    with EntitySyntax
    with AcsInspection {
  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var signatory: PartyId = _
  private var observer: PartyId = _

  private var maliciousP1: MaliciousParticipantNode = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .withSetup { implicit env =>
        import env.*

        val signatoryName = "signatory"
        val observerName = "observer"
        Seq(participant1, participant2).synchronizers.connect_local(sequencer1, alias = daName)
        Seq(participant1, participant2).synchronizers.connect_local(sequencer2, alias = acmeName)
        participant3.synchronizers.connect_local(sequencer2, alias = acmeName)

        Seq(participant1, participant2).dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        PartiesAllocator(participants.all.toSet)(
          Seq(signatoryName -> participant1, observerName -> participant2),
          Map(
            signatoryName -> Map(
              daId -> (PositiveInt.one, Set((participant1, Submission))),
              acmeId -> (PositiveInt.one, Set((participant1, Submission))),
            ),
            observerName -> Map(
              daId -> (PositiveInt.one, Set((participant2, Submission))),
              acmeId -> (PositiveInt.one, Set(
                (participant2, Submission),
                (participant3, Submission),
              )),
            ),
          ),
        )

        signatory = signatoryName.toPartyId(participant1)
        observer = observerName.toPartyId(participant2)

        pureCryptoRef.set(sequencer2.crypto.pureCrypto)

        maliciousP1 = MaliciousParticipantNode(
          participant1,
          acmeId,
          testedProtocolVersion,
          timeouts,
          loggerFactory,
        )
      }

  "Validation of assignment request during phase 7" should {
    "Security attack: cannot make other participants commit the contracts from unassignments with incorrect reassignmentId" in {
      implicit env =>
        import env.*
        val (sourceId, targetId) = (daId, acmeId)

        val iou1 = IouSyntax.createIou(participant1, Some(sourceId))(signatory, observer, 1000)
        val cid1 = LfContractId.assertFromString(iou1.id.contractId)

        val iou2 = IouSyntax.createIou(participant1, Some(sourceId))(signatory, observer, 2000)
        val cid2 = LfContractId.assertFromString(iou2.id.contractId)

        val unassignment1 = unassign(participant1, Seq(cid1), sourceId, targetId)
        val unassignment2 = unassign(participant1, Seq(cid2), sourceId, targetId)

        // P1 and P2 are reassigning participants. P3 is only connected to the target synchronizer.
        //
        // This means that both P1 and P2 will have seen these unassignments, so can validate the upcoming assignment
        // against the unassignment data they've seen.
        // P3 cannot do that validation, so it has to rely on the other participants to do it.
        //
        // However, the signatory party of our contracts is only allocated on P1.
        // The relevant stakeholder party on P2 is an observer only. This means that in stage 3, while P2 can validate
        // the assignment and see that the data is wrong, it cannot reject the assignment.
        // If P1 is malicious, it can submit the invalid assignment, then approve it in stage 3.
        //
        // We want to ensure that the invalid assignment is caught at stage 7, before being completed.

        // The real P1 will reject, but we want to emulate a malicious participant, so we override that reject.
        replacingConfirmationResult(
          targetId,
          sequencer2,
          mediator2,
          withMediatorVerdict(Verdict.Approve(testedProtocolVersion)),
        ) {
          loggerFactory.assertLogsUnorderedOptional(
            {
              // Submit the second unassignment, but with the reassignment id of the first.
              maliciousP1
                .submitAssignmentRequest(
                  signatory.toLf,
                  unassignment2,
                  Some(environment.now),
                  overrideReassignmentId = Some(unassignment1.reassignmentId),
                )
                .futureValueUS
                .value

              // ping for synchronization.
              participant2.health.ping(participant3, synchronizerId = Some(targetId))
            },
            LogEntryOptionality.Required -> logsReassignmentValidationFailed("participant1"),
            LogEntryOptionality.Required -> logsReassignmentValidationFailed("participant2"),
            LogEntryOptionality.Required -> logsReassignmentValidationFailed("participant3"),
            LogEntryOptionality.OptionalMany -> logsDodgyMediatorError("participant1"),
          )
        }

        // Neither contract was assigned on p2
        assertNotInLedgerAcsSync(Seq(participant2), observer.toLf, targetId, cid1)
        assertNotInLedgerAcsSync(Seq(participant2), observer.toLf, targetId, cid2)
        // Nor on p3
        assertNotInLedgerAcsSync(Seq(participant3), observer.toLf, targetId, cid1)
        assertNotInLedgerAcsSync(Seq(participant3), observer.toLf, targetId, cid2)

    }
  }

  private def unassign(
      participant: LocalParticipantReference,
      contractIds: Seq[LfContractId],
      sourceId: PhysicalSynchronizerId,
      targetId: PhysicalSynchronizerId,
  )(implicit
      env: TestConsoleEnvironment
  ): UnassignmentData = {
    import env.*

    val unassigned = participant.ledger_api.commands.submit_unassign(
      signatory,
      contractIds,
      sourceId,
      targetId,
    )

    participant.underlying.value.sync.syncPersistentStateManager
      .get(targetId)
      .value
      .reassignmentStore
      .lookup(ReassignmentId.tryCreate(unassigned.reassignmentId))
      .failOnShutdown
      .futureValue
  }

  private def logsDodgyMediatorError(participantName: String)(entry: LogEntry): Assertion = {
    entry.loggerName should include regex s"participant=$participantName"
    // We can get log lines where the error is hidden inside the message of an ApplicationHandlerException
    // rather than as an explicit throwable field on the entry.
    val errMsg = entry.throwable.fold(entry.errorMessage)(_.getMessage)
    errMsg should include("Mediator approved a request that we have locally rejected")
  }

  private def logsReassignmentValidationFailed(
      participantName: String
  )(entry: LogEntry): Assertion = {
    import com.digitalasset.canton.protocol.LocalRejectError
    entry.shouldBeCantonError(
      errorCode = LocalRejectError.ReassignmentRejects.InconsistentReassignmentId,
      loggerAssertion = _ should include(s"participant=$participantName"),
      messageAssertion = _ should include("id is inconsistent"),
    )
  }

}

class InvalidReassignmentIdIntegrationTestPostgres extends InvalidReassignmentIdIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2")).map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
