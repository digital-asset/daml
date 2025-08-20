// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{CommandFailure, RemoteParticipantReference}
import com.digitalasset.canton.error.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseExternalProcess,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.SynchronizerRouterIntegrationTestSetup
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencer,
  SendDecision,
  SendPolicy,
}

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class AutomaticReassignmentCrashIntegrationTest
    extends SynchronizerRouterIntegrationTestSetup
    with HasProgrammableSequencer {

  import SynchronizerRouterIntegrationTestSetup.*

  protected val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set("participant1"),
      fileNameHint = this.getClass.getSimpleName,
    )

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(external)
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"), Set("sequencer3"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override val defaultParticipant: String = "participant4"

  private var remoteP1: RemoteParticipantReference = _

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory)
      )
      .withSetup { implicit env =>
        import env.*

        remoteP1 = rp("participant1")

        participants.local.dars.upload(darPath)
        remoteP1.health.wait_for_initialized()
        remoteP1.dars.upload(darPath)

        connectToCustomSynchronizers(
          Map(
            remoteP1 -> Set(synchronizer1, synchronizer2, synchronizer3),
            participant2 -> Set(synchronizer2, synchronizer3),
            participant3 -> Set(synchronizer3),
            participant4 -> Set(synchronizer1, synchronizer2, synchronizer3),
          )
        )
        synchronizeTopologyState()
      }

  "the auto reassignment transactions crash test" should {

    "reassign multiple contracts, and they get stuck" in { implicit env =>
      import env.*

      initializedSynchronizers.foreach { case (_, initializedSynchronizer) =>
        initializedSynchronizer.synchronizerOwners.foreach(
          _.topology.synchronizer_parameters
            .propose_update(
              initializedSynchronizer.synchronizerId,
              _.update(assignmentExclusivityTimeout = 5 seconds),
            )
        )
      }

      val createP2 = createSingle(party2Id, Some(synchronizer2Id), participant2, party1Id).id
      val createP3 = createSingle(party3Id, Some(synchronizer3Id), participant3, party1Id).id
      val contractIds = List(createP2, createP3).reverse

      logger.info(s"Create the aggregate contract")
      val aggregate = createAggregate(remoteP1, party1Id, contractIds, Some(synchronizer1Id))

      assertInAcsSync(List(participant2), synchronizer2, createP2.toLf)
      assertInAcsSync(List(participant3), synchronizer3, createP3.toLf)

      val seq = getProgrammableSequencer(sequencer3.name)

      seq.setPolicy_("Block assignments") {
        SendPolicy.processTimeProofs_ { r =>
          if (r.isConfirmationRequest) SendDecision.Reject else SendDecision.Process
        }
      }

      val exerciseCmd = aggregate.id.exerciseCountAll().commands.asScala.toSeq

      logger.info(s"Exercise count all")
      val autoReassignmentF =
        loggerFactory.assertThrowsAndLogsAsync[CommandFailure](
          Future {
            remoteP1.ledger_api.javaapi.commands.submit(Seq(party1Id), exerciseCmd)
          },
          _ => succeed,
          _.errorMessage should include(AutomaticReassignmentForTransactionFailure.id),
        )
      eventually() {
        val incomplete = remoteP1.ledger_api.state.acs.incomplete_unassigned_of_party(party1Id)

        incomplete.map(r => r.contractId).toSet shouldBe Set(
          createP2.toLf.coid,
          aggregate.id.toLf.coid,
        )
      }

      autoReassignmentF.futureValue

      logger.info(s"Restarting participant 1")
      external.kill(remoteP1.name)

      logger.info(s"Unblocking the target synchronizer")
      seq.resetPolicy()

      external.start(remoteP1.name)
      remoteP1.health.wait_for_initialized()

      eventually(1.minute) {
        try {
          val time = remoteP1.health.ping(remoteP1)
          logger.info(s"Ping succeeded in $time")
        } catch {
          // Translate any CommandExecutionFailedException to a TestFailedException
          // Because `eventually` will only retry on a TestFailedException
          case _: CommandFailure =>
            fail()
        }
      }

      // Contracts are assigned eventually: initially they should be in transit, but
      // after the exclusivity timeout (set to 5 seconds), it should be retried, and
      // assigned by the connected participants, in this case: participant1 should retry upon restart.
      eventually(timeUntilSuccess = 10.seconds) {
        val inTransit = remoteP1.ledger_api.state.acs
          .incomplete_unassigned_of_party(party1Id)
        inTransit.map(r => r.contractId).toSet shouldBe Set.empty
      }
    }
  }
}
