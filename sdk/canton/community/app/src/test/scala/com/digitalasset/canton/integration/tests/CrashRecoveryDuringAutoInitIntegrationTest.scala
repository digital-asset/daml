// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.{DbConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.{CommunityStorageFactory, DbStorage}
import com.digitalasset.canton.time.SimClock
import org.scalatest.Assertion

import scala.concurrent.ExecutionContext

trait CrashRecoveryDuringAutoInitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  protected def runTest(
      node: LocalInstanceReference,
      isHealthy: => Boolean,
  )(implicit executionContext: ExecutionContext): Assertion = {
    def getTx() = node.topology.transactions
      .list()
      .result
      .map(_.mapping)

    val (generatedId, generatedKeys, generatedElements) =
      clue(s"start the node ${node.name} and initialise it") {
        node.start()
        eventually() {
          isHealthy shouldBe true
        }
        logger.debug("Node is initialised")
        val id = node.id
        val keys = node.keys.public.list().map(_.publicKey.fingerprint).toSet
        val txs = getTx()
        node.stop()
        (id, keys, txs)
      }
    val factory = new CommunityStorageFactory(node.config.storage)
    val storage = factory.tryCreate(
      connectionPoolForParticipant = false,
      None,
      new SimClock(CantonTimestamp.Epoch, loggerFactory),
      None,
      CommonMockMetrics.dbStorage,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) match {
      case jdbc: DbStorage => jdbc
      case _ => fail("should be db storage")
    }
    try {
      import storage.api.*
      storage
        .update(sqlu"DELETE FROM common_node_id", "Deleting node initialization")
        .futureValueUS
    } finally {
      storage.close()
    }
    clue(s"starting ${node.name} after we've purged the node_id") {
      node.start()
      eventually() {
        isHealthy shouldBe true
      }
      node.id shouldBe generatedId
      val keys = node.keys.public.list().map(_.publicKey.fingerprint).toSet
      keys shouldBe generatedKeys
      val txs = getTx()
      txs shouldBe generatedElements
    }
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "recovering from crash during initialization works for" should {
    "participants" in { implicit env =>
      import env.*
      runTest(participant1, participant1.health.initialized())
    }

    "sequencers" in { implicit env =>
      import env.*
      runTest(sequencer1, sequencer1.health.initialized())
    }

    "mediators" in { implicit env =>
      import env.*
      runTest(mediator1, mediator1.health.initialized())
    }

    "ping works" in { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, daName)
      participant1.health.ping(participant1)
    }
  }
}

class CrashRecoveryDuringAutoInitReferenceIntegrationTestPostgres
    extends CrashRecoveryDuringAutoInitIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

// TODO(#16761): Disabled because the BFT sequencer is not crash tolerant yet
//class CrashRecoveryDuringAutoInitBftOrderingIntegrationTestPostgres
//    extends CrashRecoveryDuringAutoInitIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseBftSequencer(loggerFactory))
//}
