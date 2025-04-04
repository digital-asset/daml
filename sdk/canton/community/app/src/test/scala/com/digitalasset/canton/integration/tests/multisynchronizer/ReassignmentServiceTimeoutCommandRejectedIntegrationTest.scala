// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{
  DbConfig,
  NonNegativeFiniteDuration as ConfigNonNegativeFiniteDuration,
}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.InitializedSynchronizer
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.HasCommandRunnersHelpers.{
  commandId as defaultCommandId,
  submissionId as defaultSubmissionId,
  userId as defaultUserId,
}
import com.digitalasset.canton.integration.util.{
  AcsInspection,
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentData
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.store.ReassignmentStore.UnknownReassignmentId
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.{ReassignmentId, TestSynchronizerParameters}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  ProgrammableSequencerPolicies,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.ReassignmentTag.Source
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SynchronizerAlias}
import org.scalatest.Assertion

import scala.collection.mutable

sealed trait ReassignmentServiceTimeoutCommandRejectedIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers
    with HasProgrammableSequencer
    with HasExecutionContext {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)

        def disableAssignmentExclusivityTimeout(d: InitializedSynchronizer): Unit =
          d.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                d.synchronizerId,
                _.update(assignmentExclusivityTimeout = ConfigNonNegativeFiniteDuration.Zero),
              )
          )

        disableAssignmentExclusivityTimeout(getInitializedSynchronizer(daName))
        disableAssignmentExclusivityTimeout(getInitializedSynchronizer(acmeName))

        participants.all.dars.upload(BaseTest.CantonExamplesPath)
        programmableSequencers.put(daName, getProgrammableSequencer(sequencer1.name))
        programmableSequencers.put(acmeName, getProgrammableSequencer(sequencer2.name))
      }

  private val programmableSequencers: mutable.Map[SynchronizerAlias, ProgrammableSequencer] =
    mutable.Map()

  private lazy val defaultsParameters = TestSynchronizerParameters.defaultDynamic

  private def setSequencerPolicy(synchronizerName: SynchronizerAlias)(
      delayConfirmationResponse: NonNegativeFiniteDuration,
      delayMediator: NonNegativeFiniteDuration,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    programmableSequencers(synchronizerName).setPolicy("maximum delay policy")(
      ProgrammableSequencerPolicies.delay(environment = environment)(
        confirmationResponses = Map(participant1.id -> delayConfirmationResponse),
        mediatorMessages = Some(delayMediator),
      )
    )
  }

  private lazy val reassignmentFailureLogAssertions: Seq[LogEntry] => Assertion =
    LogEntry.assertLogSeq(
      Seq(
        (
          _.warningMessage should include regex "Response message for request .* timed out",
          "participant timeout",
        )
      ),
      Seq(
        _.warningMessage should include regex "has exceeded the max-sequencing-time .* of the send request",
        _.warningMessage should include("Sequencing result message timed out"),
        _.warningMessage should include(
          "Rejected transaction due to a participant determined timeout"
        ),
      ),
    )

  private def getReassignmentData(
      participant: LocalParticipantReference,
      reassignmentId: ReassignmentId,
  )(implicit
      env: TestConsoleEnvironment
  ): Either[ReassignmentStore.ReassignmentLookupError, UnassignmentData] =
    participant.underlying.value.sync.syncPersistentStateManager
      .get(env.acmeId)
      .value
      .reassignmentStore
      .lookup(reassignmentId)
      .value
      .failOnShutdown
      .futureValue

  "ReassignmentService" should {
    "generate a CommandRejected in case of timeout around unassignment" in { implicit env =>
      import env.*

      val signatory = participant1.adminParty
      val observer = participant2.adminParty

      // Create contract
      val (contract, _, _) = runCommand(
        IouSyntax.createIou(participant1, Some(daId))(signatory, observer),
        daId,
        "CantonConsole",
        signatory.toLf,
        Some(participant1),
      )
      val cid = contract.id.toLf

      setSequencerPolicy(daName)(
        delayConfirmationResponse = defaultsParameters.confirmationResponseTimeout +
          NonNegativeFiniteDuration.tryOfSeconds(1),
        delayMediator = defaultsParameters.mediatorReactionTimeout,
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          // Trying to unassignment contract
          val unassignmentCompletion =
            failingUnassignment(
              cid = cid,
              source = daId,
              target = acmeId,
              signatory.toLf,
              Some(participant1),
            )

          val unassignmentTs = CantonTimestamp
            .fromProtoTimestamp(unassignmentCompletion.synchronizerTime.value.recordTime.value)
            .value

          val reassignmentId = ReassignmentId(Source(daId), unassignmentTs)

          // Entry is deleted upon timeout
          getReassignmentData(
            participant1,
            reassignmentId,
          ).left.value shouldBe UnknownReassignmentId(reassignmentId)
          eventually() {
            getReassignmentData(
              participant2,
              reassignmentId,
            ).left.value shouldBe UnknownReassignmentId(reassignmentId)
          }

          val status = unassignmentCompletion.status.value
          status.code should not be 0
          status.message should include(
            "Rejected transaction due to a participant determined timeout"
          )

          unassignmentCompletion.userId shouldBe defaultUserId
          unassignmentCompletion.submissionId shouldBe defaultSubmissionId.getOrElse("")
          unassignmentCompletion.commandId shouldBe defaultCommandId

          // Cleaning
          setSequencerPolicy(daName)(
            delayConfirmationResponse = NonNegativeFiniteDuration.Zero,
            delayMediator = NonNegativeFiniteDuration.Zero,
          )
          IouSyntax.archive(participant1)(contract, signatory)
        },
        reassignmentFailureLogAssertions,
      )
    }

    "generate a CommandRejected in case of timeout around assignment" in { implicit env =>
      import env.*

      val signatory = participant1.adminParty
      val observer = participant2.adminParty

      // Create contract
      val (contract, _, _) = runCommand(
        IouSyntax.createIou(participant1, Some(daId))(signatory, observer),
        daId,
        "CantonConsole",
        signatory.toLf,
        Some(participant1),
      )
      val cid = contract.id.toLf

      // unassignment contract
      val (unassignedEvent, _) = unassign(
        cid = cid,
        source = daId,
        target = acmeId,
        submittingParty = signatory.toLf,
        participantOverride = Some(participant1),
      )

      // Check unassignment
      assertNotInLedgerAcsSync(
        partyId = signatory,
        participantRefs = List(participant1),
        synchronizerId = daId,
        cid = cid,
      )

      setSequencerPolicy(acmeName)(
        delayConfirmationResponse =
          defaultsParameters.confirmationResponseTimeout + NonNegativeFiniteDuration.tryOfSeconds(
            1
          ),
        delayMediator = defaultsParameters.mediatorReactionTimeout,
      )

      // Trying to assign contract
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val failedAssignmentCompletion = failingAssignment(
            unassignId = unassignedEvent.event.unassignId,
            source = daId,
            target = acmeId,
            submittingParty = signatory.toLf,
            participantOverrideO = Some(participant1),
          )

          val status = failedAssignmentCompletion.status.value
          status.code should not be 0
          status.message should include(
            "Rejected transaction due to a participant determined timeout"
          )

          failedAssignmentCompletion.userId shouldBe defaultUserId
          failedAssignmentCompletion.submissionId shouldBe defaultSubmissionId.getOrElse("")
          failedAssignmentCompletion.commandId shouldBe defaultCommandId

          // Cleaning
          setSequencerPolicy(acmeName)(
            delayConfirmationResponse = NonNegativeFiniteDuration.Zero,
            delayMediator = NonNegativeFiniteDuration.Zero,
          )

          assign(
            unassignId = unassignedEvent.event.unassignId,
            source = daId,
            target = acmeId,
            submittingParty = signatory.toLf,
            participantOverride = Some(participant1),
          ).discard

          IouSyntax.archive(participant1)(contract, signatory)
        },
        reassignmentFailureLogAssertions,
      )
    }
  }
}

//class ReassignmentServiceTimeoutCommandRejectedIntegrationTestDefault
//    extends ReassignmentServiceTimeoutCommandRejectedIntegrationTest {
//  registerPlugin(
//    new UseReferenceBlockSequencer[DbConfig.H2](
//      loggerFactory,
//      sequencerGroups = Seq(Set("sequencer1"), Set("sequencer2"))
//        .map(_.map(InstanceName.tryCreate)),
//    )
//  )
//  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
//  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
//}

class ReassignmentServiceTimeoutCommandRejectedIntegrationTestPostgres
    extends ReassignmentServiceTimeoutCommandRejectedIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
