// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{
  DbConfig,
  ModifiableDbConfig,
  NonNegativeDuration,
  PositiveFiniteDuration,
  ReplicationConfig,
  StorageConfig,
}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.ConfigTransforms.updateMediatorConfig
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.resource.{DbLock, DbLockCounters, DbLockedConnectionPool, DbStorage}
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeConfig
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import slick.util.AsyncExecutorWithMetrics

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

/** Tests for starting up Canton in the presence of network failures.
  */
abstract class MediatorActiveTransitionStartupTestSetup
    extends CommunityIntegrationTest
    with IsolatedEnvironments { self =>

  private val noFailFast: StorageConfig => StorageConfig = {
    case dbConfig: ModifiableDbConfig[?] =>
      dbConfig.modify(parameters = dbConfig.parameters.focus(_.failFastOnStartup).replace(false))
    case memory: Memory =>
      memory.copy(parameters = memory.parameters.focus(_.failFastOnStartup).replace(false))
  }

  private val mediator1NoFailFast: ConfigTransform = updateMediatorConfig("mediator1") {
    _.focus(_.storage)
      .modify(noFailFast)
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .addConfigTransform(mediator1NoFailFast)
      .addConfigTransforms(ConfigTransforms.enableReplicatedMediators("mediator1"))
      .addConfigTransforms(
        _.focus(_.parameters.timeouts.processing.activeInitRetryDelay)
          .replace(NonNegativeDuration.ofMillis(2000)),
        ConfigTransforms.updateAllMediatorConfigs_(
          _.focus(_.replication).some
            .andThen(GenLens[ReplicationConfig](_.connectionPool.healthCheckPeriod))
            .replace(
              PositiveFiniteDuration.ofMillis(500)
            ) // Ensure we'll transition to active as soon as we get the lock - it's important this is lower than the
            // delay between retries (set above) when waiting for the node ID to be initialized, otherwise we may not trigger the race between
            // transition to active and initialization
        ),
      )

  // Trigger a double initialization race by reaching the "wait for Id" stage with a passive DB connection, then
  // releasing the lock, triggering active transition at the same time as running the bootstrap initialization code
  "the mediator node" when {
    "transitioning from passive to active during initialization" should {
      "still allow for the mediator to start" in { implicit env =>
        import env.*

        val dbConfig = env.mediator1.config.storage.asInstanceOf[DbConfig]
        implicit val cc: CloseContext = CloseContext(FlagCloseable(logger, timeouts))

        // Acquire a lock on the mediator's connection - this will force it to go passive on startup
        val mediatorPoolExecutor = AsyncExecutorWithMetrics.createSingleThreaded(
          "MediatorPool",
          noTracingLogger,
        )
        val mediatorPool = DbLockedConnectionPool.create(
          DbLock.isSupported(DbStorage.profile(dbConfig)).value,
          dbConfig,
          env.mediator1.config
            .asInstanceOf[MediatorNodeConfig]
            .replication
            .value
            .connectionPool,
          PositiveInt.tryCreate(4),
          DbLockCounters.MEDIATOR_WRITE,
          DbLockCounters.MEDIATOR_WRITERS,
          env.environment.clock,
          timeouts,
          exitOnFatalFailures = true,
          futureSupervisor,
          loggerFactory,
          mediatorPoolExecutor,
        )

        val startF = Future(mediator1.start())

        logger.info("Waiting for mediator to be in 'wait-for-id' state")
        mediator1.health.wait_for_running()

        logger.info("Closing mediator pool to release lock")
        // Then release the lock
        mediatorPool.value.close()
        mediatorPoolExecutor.close()

        logger.info("Observe mediator starts and can be bootstrapped into a synchronizer")
        val () = Await.result(startF, 1.minute)
        mediator1.health.wait_for_running()
        eventually() {
          mediator1.health.active shouldBe true
        }
        mediator1.health.wait_for_ready_for_initialization()
        sequencer1.start()
        sequencer1.health.wait_for_running()
        // Make sure we can bootstrap a synchronizer with this mediator
        bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
          synchronizerThreshold = PositiveInt.two,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )

        participant1.start()
        participant1.health.wait_for_running()
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.health.ping(participant1)
      }
    }
  }
}

class MediatorBftActiveTransitionStartupTest extends MediatorActiveTransitionStartupTestSetup {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
