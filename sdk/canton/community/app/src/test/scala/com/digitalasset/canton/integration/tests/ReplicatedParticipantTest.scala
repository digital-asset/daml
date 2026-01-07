// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.either.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  DbConfig,
  NonNegativeDuration,
  PositiveFiniteDuration,
  ReplicationConfig,
}
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalParticipantReference,
  ParticipantReference,
  RemoteParticipantReference,
}
import com.digitalasset.canton.integration.ConfigTransforms.{
  enableReplicatedParticipants,
  heavyTestDefaults,
}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetup,
  EnvironmentSetupPlugin,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceBecamePassive
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.PassiveReplica
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import scala.concurrent.duration.*

trait ReplicatedNodeHelper { self: CommunityIntegrationTest =>
  protected lazy val timeout: FiniteDuration = 180.seconds

  private def checkStatusActive[A](
      node: InstanceReference,
      checkFn: Boolean => A,
      allowNonInit: Boolean,
  ): A =
    eventually(timeout) {
      node.health.status match {
        case NodeStatus.Success(nodeStatus) => checkFn(nodeStatus.active)
        case NodeStatus.Failure(msg) => fail(s"Node ${node.name} status unavailable: $msg")
        case NodeStatus.NotInitialized(active, _) =>
          if (allowNonInit) checkFn(active) else fail(s"Node ${node.name} not initialized.")
      }
    }

  protected def getActiveAndPassive[I <: InstanceReference](
      node1: I,
      node2: I,
  ): (I, I) =
    if (isActive(node1)) (node1, node2)
    else if (isActive(node2)) (node2, node1)
    else fail(s"None of the nodes is active")

  protected def isActive(node: InstanceReference, allowNonInit: Boolean = false): Boolean =
    checkStatusActive(node, identity, allowNonInit)

  protected def waitActive(node: InstanceReference, allowNonInit: Boolean = false): Unit =
    checkStatusActive(
      node,
      active => if (active) succeed else fail(s"Node ${node.name} is not active"),
      allowNonInit,
    )

  protected def waitPassive(node: InstanceReference, allowNonInit: Boolean = false): Unit =
    checkStatusActive(
      node,
      active => if (!active) succeed else fail(s"Node ${node.name} is not passive"),
      allowNonInit,
    )

  protected def waitUntilOneActive[I <: InstanceReference](
      node1: I,
      node2: I,
      allowNonInit: Boolean = false,
  ): I =
    checkStatusActive(
      node1,
      active =>
        if (active) node1
        else
          checkStatusActive(
            node2,
            active =>
              if (active) node2
              else
                fail(
                  s"Neither node ${node1.name} nor node ${node2.name} is active"
                ),
            allowNonInit,
          ),
      allowNonInit,
    )
}

trait ReplicatedParticipantTestSetup extends ReplicatedNodeHelper {
  self: CommunityIntegrationTest with EnvironmentSetup =>

  protected def setupPlugins(
      storagePlugin: EnvironmentSetupPlugin,
      additionalPlugins: Seq[EnvironmentSetupPlugin] = Seq.empty,
  ): Unit = {
    registerPlugin(storagePlugin)
    registerPlugin(
      UseSharedStorage.forParticipants(
        activeParticipantName,
        Seq(passiveParticipantName),
        loggerFactory,
      )
    )
    // TODO(#29603): change this back to use the BFT Orderer.
    // The PerformanceReplicatedParticipantDatabaseFaultIntegrationTest was flaking a lot with the BFT Orderer
    // registerPlugin(new UseBftSequencer(loggerFactory))
    registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

    additionalPlugins.foreach(registerPlugin)

    // Enable write connection pool for the replicated participants
    // Increase max connections for the participants to have more than one connection in the write pool
    registerPlugin(
      new UseConfigTransforms(
        Seq(ConfigTransforms.setParticipantMaxConnections(PositiveInt.tryCreate(16))),
        loggerFactory,
      )
    )
  }

  protected lazy val activeParticipantName = "participant1"
  protected lazy val passiveParticipantName = "participant2"

  protected def failPassivePing(
      passiveParticipant: ParticipantReference,
      otherParticipant: ParticipantReference,
      afterFailure: Boolean = false,
  ): Assertion = {
    // Ensure passive participant is passive
    waitPassive(passiveParticipant)

    val expectedPingServiceFailureLogs = List(
      "GrpcServiceUnavailable: UNIMPLEMENTED/Method not found: com.digitalasset.canton.admin.participant.v30.PingService/Ping"
    )
    // We see additional warnings and errors if the ping is performed after a failure on the passive replica
    val expectedWarnsAfterFailover: List[String] =
      if (afterFailure)
        List(
          "Closing resilient sequencer subscription because instance became passive",
          SyncServiceBecamePassive.id,
          "disconnected because participant became passive",
          "Now retrying operation", // The failover may trigger retries. The retries may drag on when the participant is already passive.
          "DbStorage instance is not active: Connection pool is not active",
          "Locked connection was lost, trying to rebuild",
        ) ++ expectedPingServiceFailureLogs
      else expectedPingServiceFailureLogs

    val expectedWarns: List[String] = List(PassiveReplica.id) ++ expectedWarnsAfterFailover

    // The ping should fail on unavailable gRPC PingService
    loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
      passiveParticipant.health
        .maybe_ping(otherParticipant.id, timeout = NonNegativeDuration(5.seconds)),
      logs =>
        forEvery(logs)(log =>
          assert(
            expectedWarns.exists(msg => log.toString.contains(msg)),
            s"line $log contained unexpected problem",
          )
        ),
    )
  }

  protected def activePing(
      activeParticipant: ParticipantReference,
      otherParticipant: ParticipantReference,
  ): Assertion = {
    waitActive(activeParticipant)

    // The ping service may fail under retries, workaround to try multiple times
    eventually(timeout) {
      Either
        .catchOnly[CommandFailure](
          activeParticipant.health.maybe_ping(otherParticipant.id)
        )
        .toOption
        .flatten shouldBe defined
    }
  }

  protected def setupReplicas(
      env: TestConsoleEnvironment,
      external: Option[UseExternalProcess] = None,
  ): Unit = {
    import env.*

    def startParticipant(name: String): ParticipantReference = {
      val ref = p(name)
      ref match {
        case p: LocalParticipantReference => p.start()
        case p: RemoteParticipantReference =>
          external
            .valueOrFail(s"external plugin missing for remote participant $p")
            .start(name)
        case _ => fail(s"Invalid participant reference for $name")
      }

      eventually(timeUntilSuccess = 120.seconds, maxPollInterval = 10.seconds) {
        assert(ref.health.status.isInitialized)
      }

      ref
    }

    new NetworkBootstrapper(
      NetworkTopologyDescription(
        daName,
        synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator1),
      )(env)
    )(env).bootstrap()

    sequencer1.topology.synchronizer_parameters
      .propose_update(daId, _.update(reconciliationInterval = 1.second))

    // start the local active participant first to be the active replica
    val activeParticipant = startParticipant(activeParticipantName)
    activeParticipant.synchronizers.connect_local(sequencer1, alias = daName)
    waitActive(activeParticipant)

    // start passive participant second to be the passive replica
    val passiveParticipant = startParticipant(passiveParticipantName)
    waitPassive(passiveParticipant)

    // start a non-replicated participant and connect to the synchronizer
    assert(!Seq(activeParticipant, passiveParticipant).contains(participant3))
    participant3.start()
    participant3.synchronizers.connect_local(sequencer1, alias = daName)
  }

  protected lazy val baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S1M1_Manual
      .clearConfigTransforms()
      .addConfigTransforms(heavyTestDefaults*)
      .addConfigTransforms(
        enableReplicatedParticipants(activeParticipantName, passiveParticipantName),
        // Aggressive check periods to speed up the test
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.replication).some
            .andThen(GenLens[ReplicationConfig](_.connectionPool))
            .modify(
              _.focus(_.healthCheckPeriod)
                .replace(PositiveFiniteDuration.ofSeconds(1))
                .focus(_.connection.healthCheckPeriod)
                .replace(PositiveFiniteDuration.ofSeconds(1))
                .focus(_.connection.passiveCheckPeriod)
                .replace(PositiveFiniteDuration.ofSeconds(5))
            )
        ),
      )
}

/** Testing a replicated participant node without crashing the nodes, but only with a graceful
  * shutdown.
  */
trait ReplicatedParticipantTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with ReplicatedParticipantTestSetup
    with ReliabilityTestSuite {

  "A replicated participant" must {

    "accept requests on the active replica and reject on the passive" in { implicit env =>
      import env.*

      // Active participant (=participant1) must be able to ping another non-replicated participant node
      activePing(participant1, participant3)

      failPassivePing(participant2, participant3)
    }

    "fail-over on shutdown of active replica" taggedAs ReliabilityTest(
      Component(
        name = "application",
        setting = "application connecting to participant node ledger API directly",
      ),
      AdverseScenario(
        dependency = "active participant replica",
        details = "orderly shutdown (`participant.stop()`)",
      ),
      Remediation(
        remediator = "application",
        action = "fail-over to the new active participant replica",
      ),
      outcome = "transaction processing possible between fail-overs",
    ) in { implicit env =>
      import env.*

      participant1.stop()

      // Participant2 should take over from participant1
      activePing(participant2, participant3)

      // Start the former participant again, should now be passive and reject write requests
      participant1.start()
      failPassivePing(participant1, participant3)
    }

    "fail-over when the active replica is set to passive" in { implicit env =>
      import env.*

      val activeParticipantReplica = participant2
      val passiveParticipantReplica = participant1

      // Set the active replica to passive
      // user-manual-entry-begin: SetPassive
      activeParticipantReplica.replication.set_passive()
      // user-manual-entry-end: SetPassive

      // Passive participant should take over from former active participant
      activePing(passiveParticipantReplica, participant3)

      // former active replica should now be passive and reject write requests
      failPassivePing(activeParticipantReplica, participant3)

      // Fail over again
      passiveParticipantReplica.replication.set_passive()
      activePing(activeParticipantReplica, participant3)
      failPassivePing(passiveParticipantReplica, participant3)
    }

    "set passive on non-replicated participant should recover" in { implicit env =>
      import env.*

      // There is no other replica to take over as passive, so participant3 should not have become passive, but remain active
      participant3.replication.set_passive()

      activePing(participant3, participant3)
    }

    "reconnect command on passive replica should fail" in { implicit env =>
      import env.*

      val passiveParticipantReplica = participant1

      waitPassive(passiveParticipantReplica)

      assertThrowsAndLogsCommandFailures(
        passiveParticipantReplica.synchronizers.reconnect_all(),
        _.shouldBeCommandFailure(SyncServiceError.SyncServicePassiveReplica),
      )
    }

  }

  // We may get some warnings during shutdown that should not fail the test
  override def testFinished(env: TestConsoleEnvironment): Unit =
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      super.testFinished(env),
      LogEntry.assertLogSeq(
        Seq.empty,
        Seq(_.warningMessage should include("Locked connection was lost, trying to rebuild")),
      ),
    )

}

class ReplicatedParticipantTestPostgres extends ReplicatedParticipantTest {
  setupPlugins(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition.withManualStart.withSetup(setupReplicas(_))
}
