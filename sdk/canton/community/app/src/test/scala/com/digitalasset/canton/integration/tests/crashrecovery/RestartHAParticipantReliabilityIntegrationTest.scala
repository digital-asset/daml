// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{ParticipantReference, RemoteInstanceReference}
import com.digitalasset.canton.integration.plugins.*
import com.digitalasset.canton.integration.tests.ReplicatedNodeHelper
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest
import com.digitalasset.canton.integration.tests.reliability.ReliabilityPerformanceIntegrationTest
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.TracedLogger
import org.scalatest.matchers.Matcher

import scala.concurrent.duration.*

/** This class is in the crashrecovery (and not the reliability) packages because it uses the
  * external process plugin and currently using this plugin requires dumping the sbt classpath which
  * is only done in the crashrecovery tests.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class RestartHAParticipantReliabilityIntegrationTest
    extends ReliabilityPerformanceIntegrationTest
    with BasePerformanceIntegrationTest
    with LoadBalancedHAParticipantIntegrationTestBase
    with ReplicatedNodeHelper {

  private val (loadBalancedParticipant1, loadBalancedParticipant2) =
    ("participant1", "participant4")

  "kill a replicated participant while the runners are running".taggedAs(
    ReliabilityTest(
      Component("Application", "connected via load balancer to replicated participant"),
      AdverseScenario(
        dependency = "Participant node",
        details = "Forced stopping of the participant process",
      ),
      Remediation(
        remediator = "application and load balancer",
        action =
          "load balancer should direct requests at other running nodes and the application should retry requests that were dropped during fail-over",
      ),
      outcome = "transaction processing continuously possible",
    )
  ) in { implicit env =>
    runWithFault(loadBalancedParticipantName, loadBalancedParticipant.config.ledgerApi.port) {
      hasCompleted =>
        import env.*

        (1 to 10).foreach(
          remoteNodeOutage(
            external,
            rp(loadBalancedParticipant1),
            rp(loadBalancedParticipant2),
            hasCompleted,
            logger,
          )
        )
    }
  }

  private def remoteNodeOutage(
      external: UseExternalProcess,
      node1: RemoteInstanceReference,
      node2: RemoteInstanceReference,
      hasCompleted: () => Boolean,
      logger: TracedLogger,
  )(index: Int): Unit =
    if (!hasCompleted()) {
      // alternate between nodes to take down
      val (nodeToRestart, otherNode) = if (index % 2 == 1) (node1, node2) else (node2, node1)

      logger.info(s"Stopping ${nodeToRestart.name}")
      external.kill(nodeToRestart.name)

      waitActive(otherNode)

      logger.info(s"Starting ${nodeToRestart.name}")
      external.start(nodeToRestart.name)
      nodeToRestart.health.wait_for_running()
      nodeToRestart.health.wait_for_initialized()
      waitPassive(nodeToRestart)
    } else {
      checkPerfRunnerCompletion(index)
    }

  override lazy val baseEnvironmentConfig: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransform(
        ConfigTransforms
          .enableReplicatedParticipants(loadBalancedParticipant1, loadBalancedParticipant2)
      )
      .addConfigTransforms(ConfigTransforms.addMonitoringEndpointAllNodes*)
      .addConfigTransform(addRemoteLoadBalancedParticipant)

  var initialActiveParticipant: ParticipantReference = _

  override protected def baseSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participant2.health.wait_for_running()
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    // not synchronizing on `loadBalancedParticipant` here since admin-api isn't load balanced
    rp(loadBalancedParticipant1).health.wait_for_running()
    rp(loadBalancedParticipant2).health.wait_for_running()
    initialActiveParticipant =
      waitUntilOneActive(rp(loadBalancedParticipant1), rp(loadBalancedParticipant2))
    initialActiveParticipant.synchronizers.connect_local(sequencer1, alias = daName)

    // sleep a bit to give HAproxy time to recognize that a participant is active
    Threading.sleep((healthCheckInterval * 3).seconds.toMillis)
  }

  override protected def performanceTestSetup(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    participant2.dars.upload(PerformanceTestPath)
    initialActiveParticipant.dars.upload(PerformanceTestPath)
  }

  protected val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set(loadBalancedParticipant1, loadBalancedParticipant2),
      fileNameHint = this.getClass.getSimpleName,
    )

  registerPlugin(
    new UsePostgres(loggerFactory)
  ) // needs to be before the external process such that we pick up the postgres config changes
  registerPlugin(
    UseSharedStorage.forParticipants(
      loadBalancedParticipant1,
      Seq(loadBalancedParticipant2),
      loggerFactory,
    )
  )
  registerPlugin(
    new UseBftSequencer(loggerFactory)
  )

  registerPlugin(
    new UseConfigTransforms(
      Seq(
        ConfigTransforms.setStorageQueueSize(10000),
        ConfigTransforms.setParticipantMaxConnections(PositiveInt.tryCreate(16)),
      ),
      loggerFactory,
    )
  )

  registerPlugin(external)
  registerPlugin(
    new UseHAProxy(
      mkLoadBalancerSetup(
        external,
        loadBalancedParticipant1,
        loadBalancedParticipant2,
        "ledger-api",
      ),
      setRemoteParticipantToLoadBalancedLedgerApi,
      loggerFactory,
    )
  )

  override protected def matchAcceptableErrorOrWarnMessage: Matcher[String] =
    (include(
      "Subscription failed with grpc-error: INTERNAL / Received unexpected EOS on empty DATA frame from server"
    )
      or super.matchAcceptableErrorOrWarnMessage)

}
