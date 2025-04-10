// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.FuncTest
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.FutureUtil
import monocle.Monocle.toAppliedFocusOps

import scala.annotation.tailrec
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt

/*
  This test does a party migration between two participants.
  It additionally checks that the ACS commitment processor indicates the discrepancies
  during the migration and that they disappear after repairs.
 */
trait OfflinePartyMigrationAcsCommitmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with RepairTestUtil {

  private val acsCommitmentInterval = PositiveSeconds.tryOfSeconds(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair)
      )
      // do no delay sending commitments
      .updateTestingConfig(
        _.focus(_.maxCommitmentSendDelayMillis).replace(Some(NonNegativeInt.zero))
      )

  "use repair to migrate a party to a different participant" taggedAs
    FuncTest(topics = Seq("party migration"), features = Seq("repair.add")) in { implicit env =>
      import env.*

      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = daId,
        _.update(reconciliationInterval = acsCommitmentInterval.toConfig),
      )

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath)

      val alice = participant1.parties.enable("Alice")

      // Create a contract for Alice
      val iouId = createContract(participant1, alice, alice)
      val iouInst = readContractInstance(participant1, daName.unwrap, daId, iouId)

      @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
      def awaitMismatches(): Unit = {
        var p1: Boolean = false
        var p2: Boolean = false

        // Wait to ensure that the first ping triggers its own, new acs commitment interval to avoid a flake such as #7930
        val promise = Promise[Unit]()
        FutureUtil.doNotAwait(
          env.environment.clock
            .scheduleAfter(_ => promise.success(()), acsCommitmentInterval.unwrap)
            .unwrap,
          "awaiting mismatches",
        )
        promise.future.futureValue

        do {
          // Trigger time progression
          participant1.health.ping(participant2).discard
          val entry = loggerFactory.pollRecordedLogEntry(5.seconds).value

          entry.message should (include(Errors.MismatchError.NoSharedContracts.id) or include(
            Errors.MismatchError.CommitmentsMismatch.id
          ))
          if (entry.loggerName.contains(participant1.name)) p1 = true
          else if (entry.loggerName.contains(participant2.name)) p2 = true
          else fail(s"Unexpected mismatch: $entry")
        } while (!(p1 && p2))
      }

      @tailrec def drainMismatches(): Unit =
        loggerFactory.pollRecordedLogEntry() match {
          case None => ()
          case Some(entry) =>
            entry.message should (include(Errors.MismatchError.NoSharedContracts.id) or include(
              Errors.MismatchError.CommitmentsMismatch.id
            ))
            entry.loggerName should (include(participant1.name) or include(participant2.name))
            drainMismatches()
        }

      loggerFactory.suppressWarnings {

        // Now bring Alice to participant2
        // This should trigger ACS mismatches
        Seq(participant1, participant2).foreach(
          _.topology.party_to_participant_mappings.propose_delta(
            party = alice,
            adds = List(participant2.id -> ParticipantPermission.Submission),
            store = daId,
          )
        )
        // TODO (#15946) In the future, utils.synchronize_topology() should be sufficient. For now, there is a small time
        //  window where it can happen that the participants receive the topology txn above, but have not started
        //  processing it yet. Still, they can see the topology status idle because the manager, dispatcher and clients
        //  are idle, which is what synchronize_topology() checks.
        //  To avoid this problem, we first ensure the participants actually started processing the topology tx by
        //  checking that they observe the party active on two participants. In the future, we should be able to remove
        //  this check.
        eventually() {
          Seq(participant1, participant2).foreach(p =>
            p.topology.party_to_participant_mappings.is_known(
              daId,
              alice,
              Seq(participant1, participant2),
            ) shouldBe true
          )
        }

        // Create another contract
        createContract(participant2, alice, alice)

        // Wait until we see a mismatch
        awaitMismatches()

        // Now manually import the contract
        participant2.synchronizers.disconnect(daName)
        participant2.repair.add(daId, testedProtocolVersion, Seq(iouInst))
        participant2.synchronizers.reconnect(daName)

        val afterMigration = env.environment.clock.now

        // We check that the ACS commitment processor recovers by looking at the safe-to-prune point
        eventually() {
          // Trigger time progression
          participant1.health.ping(participant2).discard
          val outstandingCommitment =
            participant2.underlying.value.sync.stateInspection
              .noOutstandingCommitmentsTs(daName, CantonTimestamp.MaxValue)
          assert(outstandingCommitment.exists(_ >= afterMigration))
        }

        // Now make sure that we haven't gobbled other errors
        drainMismatches()
      }

      // Use the imported contract.
      // We do this after checking the ACS commitment processor because the archival also re-syncs the ACSs.
      exerciseContract(participant2, alice, iouId)
    }
}

class OfflinePartyMigrationAcsCommitmentIntegrationTestPostgres
    extends OfflinePartyMigrationAcsCommitmentIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
}

class OfflinePartyMigrationAcsCommitmentBftOrderingIntegrationTestPostgres
    extends OfflinePartyMigrationAcsCommitmentIntegrationTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UsePostgres(loggerFactory))
}

//class OfflinePartyMigrationAcsCommitmentIntegrationTestH2
//    extends OfflinePartyMigrationAcsCommitmentIntegrationTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//  registerPlugin(new UseH2(loggerFactory))
//}

//class OfflinePartyMigrationAcsCommitmentBftOrderingIntegrationTestH2 extends OfflinePartyMigrationAcsCommitmentIntegrationTest {
//  registerPlugin(new UseBftOrderingBlockSequencer(loggerFactory))
//  registerPlugin(new UseH2(loggerFactory))
//}
