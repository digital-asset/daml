// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.StorageConfig.Memory
import com.digitalasset.canton.config.{
  DbConfig,
  ModifiableDbConfig,
  PositiveFiniteDuration,
  ReplicationConfig,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.resource.{DbLock, DbLockCounters, DbLockedConnectionPool, DbStorage}
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import slick.util.AsyncExecutorWithMetrics

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

/** Tests for starting up Canton in the presence of network failures.
  */
sealed abstract class ParticipantActiveTransitionStartupIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual.withManualStart
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") {
          _.focus(_.storage).modify {
            case db: ModifiableDbConfig[?] =>
              db.modify(parameters = db.parameters.focus(_.failFastOnStartup).replace(false))
            case mem: Memory =>
              mem.copy(parameters = mem.parameters.focus(_.failFastOnStartup).replace(false))
          }
        },
        ConfigTransforms.enableReplicatedParticipants("participant1"),
        _.focus(_.parameters.timeouts.processing.activeInitRetryDelay)
          .replace(config.NonNegativeDuration.ofMillis(5000)),

        // Ensure we'll transition to active as soon as we get the lock - it's important this is lower than the
        // delay between retries (set above) when waiting for the node ID to be initialized, otherwise we may not trigger the race between
        // transition to active and initialization
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.replication).some
            .andThen(GenLens[ReplicationConfig](_.connectionPool.healthCheckPeriod))
            .replace(PositiveFiniteDuration.ofMillis(500))
        ),
      )

  // Trigger a double initialization race by reaching the running stage with a passive DB connection, then
  // releasing the lock, triggering active transition at the same time as running the bootstrap initialization code
  "the participant node" when {
    "transitioning from passive to active during initialization" should {
      "still allow for the participant to start" in { implicit env =>
        import env.*

        val dbConfig = participant1.config.storage.asInstanceOf[DbConfig]
        implicit val cc: CloseContext = CloseContext(FlagCloseable(logger, timeouts))

        // Acquire a lock on the participant's connection - this will force it to go passive on startup
        val participantPoolExecutor = AsyncExecutorWithMetrics.createSingleThreaded(
          "ParticipantPool",
          noTracingLogger,
        )
        val participantPool = DbLockedConnectionPool.create(
          DbLock.isSupported(DbStorage.profile(dbConfig)).value,
          dbConfig,
          participant1.config
            .asInstanceOf[ParticipantNodeConfig]
            .replication
            .value
            .connectionPool,
          PositiveInt.tryCreate(4),
          DbLockCounters.PARTICIPANT_WRITE,
          DbLockCounters.PARTICIPANT_WRITERS,
          environment.clock,
          timeouts,
          exitOnFatalFailures = true,
          futureSupervisor,
          loggerFactory,
          participantPoolExecutor,
        )

        sequencer1.start()
        mediator1.start()
        bootstrap.synchronizer(
          synchronizerName = daName.toProtoPrimitive,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
        )

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            val startF = Future(participant1.start())

            logger.info("Waiting for participant to be running")
            participant1.health.wait_for_running()

            logger.info("Releasing the lock")
            participantPool.value.close()
            participantPoolExecutor.close()

            logger.info("Waiting for the situation to normalize")
            val () = Await.result(startF, 1.minute)
            participant1.health.wait_for_initialized()
            logger.info("Participant initialized")

            participant1.synchronizers.connect_local(sequencer1, alias = daName)
            participant1.synchronizers.active(daName) shouldBe true
            participant1.health.maybe_ping(participant1) shouldBe defined

          },
          LogEntry.assertLogSeq(
            Seq.empty,
            Seq(
              // The scheduler logs a warning because it's being re-activated (once in the node initialize method and
              // once in the active transition method
              _.warningMessage should include(
                "Scheduler unexpectedly already started. Stopping old executor"
              )
            ),
          ),
        )
      }
    }
  }
}

final class ParticipantActiveTransitionStartupBftOrderingIntegrationTest
    extends ParticipantActiveTransitionStartupIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
