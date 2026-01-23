// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.synchronizer.sequencer.time.TimeAdvancingTopologySubscriber.TimeAdvanceBroadcastMessageIdPrefix
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.DurationConverters.*

class TopologyTicksIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  val epsilon = defaultStaticSynchronizerParameters.topologyChangeDelay

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def runAsyncAndAdvanceClockUntilFinished(f: () => Unit, clock: SimClock)(implicit
      ec: ExecutionContext
  ): Unit = {
    val future = Future(f())
    eventually(maxPollInterval = epsilon.duration.toScala) {
      clock.advance(epsilon.duration)
      future.isCompleted shouldBe true
    }
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(
          // in tests using the sim clock, epsilon is by default set to 0, but for this test we want it to be
          // the usual non-zero default so we can control when a topology transaction should become effective.
          S1M1.withTopologyChangeDelay(epsilon.toConfig)
        )
      }
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.parameters.producePostOrderingTopologyTicks).replace(true)
        )
      )

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private val sendPolicy: SendPolicy = { _ => submission =>
    // let's drop any time proof requests and time advancing messages so that only topology ticks can be used
    // to advance time across all clients
    if (TimeProof.isTimeProofSubmission(submission)) SendDecision.Drop
    else if (submission.messageId.unwrap.startsWith(TimeAdvanceBroadcastMessageIdPrefix)) {
      fail(
        "we do not expect any time advancing messages when producePostOrderingTopologyTicks is turned on"
      )
    } else SendDecision.Process
  }

  if (testedProtocolVersion >= ProtocolVersion.v35) {
    "Topology ticks" should {
      "be created to signal that topology transactions have become effective after the topology change delay has passed" in {
        implicit env =>
          import env.*
          val simClock = env.environment.simClock.value
          runAsyncAndAdvanceClockUntilFinished(
            // we need to have this command run while advancing the clock, so that topology transactions
            // (such as SynchronizerTrustCertificate) created during the participant connecting to the sequencer
            // become effective and the process can complete
            () => participant1.synchronizers.connect_local(sequencer1, alias = daName),
            simClock,
          )

          val sequencer = getProgrammableSequencer(sequencer1.name)
          sequencer.setPolicy("drop time proofs and time advancing messages")(sendPolicy)

          // create party with synchronize = None so that we don't wait on the topology transaction to become effective
          val partyId = participant1.parties.enable("testParty1", synchronize = None)

          // we see that it is not effective immediately
          participant1.ledger_api.parties.list().map(_.party) should not contain (partyId)

          // but becomes effective after advancing time enough for a topology tick to be created and signal that the
          // topology transaction is effective.
          eventually() {
            simClock.advance(epsilon.duration)
            participant1.ledger_api.parties.list().map(_.party) should contain(partyId)
            sequencer1.topology.party_to_participant_mappings
              .list(daId)
              .map(_.item.partyId) should contain(partyId)
          }
      }

      "be persisted across restarts" in { implicit env =>
        import env.*
        // doing a ping first to make create some events to make sure sequencer time is caught up to sim clock time
        participant1.health.ping(participant1)

        val simClock = env.environment.simClock.value
        val sequencer = getProgrammableSequencer(sequencer1.name)
        sequencer.setPolicy("drop time proofs and time advancing messages")(sendPolicy)

        // create a new party with synchronize = None so that we don't wait on the topology transaction to become effective
        val partyId = participant1.parties.enable("testParty2", synchronize = None)

        // wait a little bit to make sure the sequencer has registered the topology transaction,
        // before we proceed to test crash recovery
        blocking(Thread.sleep(1000))

        // we see that the topology transaction is not effective immediately
        participant1.ledger_api.parties.list().map(_.party) should not contain (partyId)

        // restart the sequencer
        sequencer1.stop()
        sequencer1.start()

        // the sequencer persisted the timestamp of the latest topology transaction and after restart it is able to tell
        // when it has become effective and emit a topology tick
        eventually() {
          simClock.advance(epsilon.duration)
          participant1.ledger_api.parties.list().map(_.party) should contain(partyId)
          sequencer1.topology.party_to_participant_mappings
            .list(daId)
            .map(_.item.partyId) should contain(partyId)
        }
      }
    }
  }
}
