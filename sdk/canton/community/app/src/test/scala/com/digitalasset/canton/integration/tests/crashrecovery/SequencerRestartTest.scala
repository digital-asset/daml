// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.admin.api.client.data.ParticipantStatus.SubmissionReady
import com.digitalasset.canton.admin.api.client.data.{
  ComponentHealthState,
  ComponentStatus,
  NodeStatus,
  SequencerStatus,
  TrafficControlParameters,
}
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeLong,
  NonNegativeNumeric,
  PositiveInt,
}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  CantonConfig,
  DbConfig,
  DefaultProcessingTimeouts,
  RequireTypes,
}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalMediatorReference,
  LocalParticipantReference,
  RemoteSequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.{
  InitializedSynchronizer,
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseExternalProcess.ShutdownPhase
import com.digitalasset.canton.integration.plugins.{
  PostgresDumpRestore,
  UseExternalProcess,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, HasCloseContext}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, StorageSingleFactory}
import com.digitalasset.canton.sequencing.TrafficControlParameters as InternalTrafficControlParameters
import com.digitalasset.canton.synchronizer.block.data.db.DbSequencerBlockStore
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriterConfig
import com.digitalasset.canton.synchronizer.sequencer.store.DbSequencerStore
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{DefaultTestIdentities, SequencerId}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{SynchronizerAlias, config}
import org.scalactic.source.Position
import org.scalatest.Assertion

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
abstract class BaseSynchronizerRestartTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments {

  var postgresDumpRestore: PostgresDumpRestore = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 1,
        numMediators = 1,
      )
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        mediator1.start()
        external.start(remoteSequencer1.name)
        participant1.start()
        participant2.start()

        mediator1.health.wait_for_ready_for_initialization()
        remoteSequencer1.health.wait_for_ready_for_initialization()
      }
      .withNetworkBootstrap(networkBootstrapper(_))
      .withSetup { _ =>
        postgresDumpRestore = PostgresDumpRestore(postgresPlugin, forceLocal = false)
      }

  protected def networkBootstrapper(implicit
      env: TestConsoleEnvironment
  ): NetworkBootstrapper = {
    import env.*
    new NetworkBootstrapper(
      NetworkTopologyDescription(
        synchronizerAlias = daName,
        synchronizerOwners = Seq(remoteSequencer1),
        synchronizerThreshold = PositiveInt.one,
        sequencers = Seq(remoteSequencer1),
        mediators = Seq(mediator1),
      )
    )
  }

  protected val external = new UseExternalProcess(
    loggerFactory,
    externalSequencers = Set("sequencer1"),
    shutdownPhase = ShutdownPhase.AfterEnvironment,
    fileNameHint = this.getClass.getSimpleName,
  )
  val postgresPlugin = new UsePostgres(loggerFactory)
  registerPlugin(
    postgresPlugin
  ) // needs to be before the external process such that we pick up the postgres config changes
  registerPlugin(
    new EnvironmentSetupPlugin {
      override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
        // we want the sequencers to be killed before the UseReferenceBlockSequencer cleans up the database,
        // but only after participants and mediators have been stopped (after environment destroyed)
        if (external.isRunning("sequencer1")) {
          external.kill("sequencer1")
        }

      override protected def loggerFactory: NamedLoggerFactory =
        sys.error(s"logging was used but shouldn't be")
    }
  )
  val sequencerPlugin = new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  registerPlugin(sequencerPlugin)
  registerPlugin(external)

  protected def createStorageFor(
      sequencerReference: RemoteSequencerReference
  )(implicit env: TestConsoleEnvironment, closeContext: CloseContext): DbStorage = {
    import env.*
    val storage =
      new StorageSingleFactory(external.storageConfig(sequencerReference.name))
        .create(
          connectionPoolForParticipant = false,
          None,
          new SimClock(CantonTimestamp.Epoch, loggerFactory),
          None,
          ParticipantTestMetrics.dbStorage,
          timeouts,
          loggerFactory,
        )
        .valueOrFailShutdown("Failed to create DbStorage")
    env.environment.addUserCloseable(storage)
    val dbStorage = storage match {
      case dbStorage: DbStorage => dbStorage
      case _: MemoryStorage => fail("Storage must be DbStorage")
    }
    dbStorage
  }

  protected def deleteLatestBlockCompletion(sequencerReference: RemoteSequencerReference)(implicit
      env: TestConsoleEnvironment,
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): Unit = {
    val storage = createStorageFor(sequencerReference)

    val sequencerStore = new DbSequencerStore(
      storage = storage,
      protocolVersion = testedProtocolVersion,
      bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
      bufferedEventsPreloadBatchSize = SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
      timeouts = DefaultProcessingTimeouts.testing,
      loggerFactory = loggerFactory,
      sequencerMember = SequencerId(DefaultTestIdentities.physicalSynchronizerId.uid),
      blockSequencerMode = true,
      cachingConfigs = CachingConfigs(),
      batchingConfig = BatchingConfig(),
      sequencerMetrics = SequencerMetrics.noop(getClass.getName),
    )

    val blockStore = new DbSequencerBlockStore(
      storage,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
      batchingConfig = BatchingConfig(),
      sequencerStore,
    )

    import storage.api.*

    val res = for {
      headStateO <- blockStore.readHead
      headState = headStateO.value
      query =
        sqlu"""delete from seq_block_height where height = ${headState.latestBlock.height}"""
      _ <- storage.update(query, "reset sequencer watermark")
    } yield ()
    res.futureValueUS
    storage.close()
  }

  protected def waitUntilRunning(instance: InstanceReference): Unit =
    eventually(40.seconds) {
      instance.health.status match {
        case NodeStatus.Success(status: SequencerStatus) =>
          logger.info(
            s"Instance ${instance.name} is running now ${status.uptime} ${status.connectedParticipants}"
          )
        case _ => fail(s"Instance ${instance.name} didn't start up withing expected time")
      }
    }

  protected def checkAllConnected(
      sequencer: RemoteSequencerReference,
      timeUntilSuccess: FiniteDuration = 10.seconds,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    eventually(timeUntilSuccess) {
      val status = health.status()
      val participantStatus = status.participantStatus
      val sequencerStatus = sequencer.health.status.trySuccess
      sequencerStatus.connectedParticipants should contain.allOf(
        participant1.id,
        participant2.id,
      )
      val synchronizerId = sequencerStatus.synchronizerId

      participantStatus(participant1.name).connectedSynchronizers should contain(
        synchronizerId -> SubmissionReady(true)
      )
      participantStatus(participant2.name).connectedSynchronizers should contain(
        synchronizerId -> SubmissionReady(true)
      )
    }
  }

  protected def checkMediatorConnected(
      timeUntilSuccess: FiniteDuration
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    eventually(timeUntilSuccess) {
      val mediatorStatus = health.status().mediatorStatus
      mediatorStatus(mediator1.name).components should contain(
        ComponentStatus(
          "sequencer-client",
          ComponentHealthState.Ok(),
        )
      )
    }
  }

  protected def checkAllDisconnected(
      sequencer: RemoteSequencerReference,
      timeUntilSuccess: FiniteDuration = 2.seconds,
      beforeCheck: () => Unit = () => (),
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    eventually(timeUntilSuccess) {
      beforeCheck()
      val status = health.status()
      val participantStatus = status.participantStatus
      val sequencerStatus = sequencer.health.status.trySuccess
      participantStatus(participant1.name).connectedSynchronizers shouldBe empty
      participantStatus(participant2.name).connectedSynchronizers shouldBe empty
      sequencerStatus.connectedParticipants shouldBe empty
    }
  }

  protected def checkSequencerIsDown(sequencer: RemoteSequencerReference): Unit =
    sequencer.health.status match {
      case NodeStatus.Failure(x)
          if x.contains(s"Request failed for ${sequencer.name}. Is the server running") =>
        ()
      case x => fail(s"Unexpected $x")
    }

  protected def portOfSequencer(sequencer: RemoteSequencerReference): RequireTypes.Port =
    sequencer.health.status match {
      case NodeStatus.Success(status: SequencerStatus) =>
        status.ports.values
          .find(_ == sequencer.config.publicApi.port)
          .value
      case _ => fail("can not determine remote port")
    }

  protected def connectSynchronizer(
      synchronizerAlias: SynchronizerAlias,
      sequencer: RemoteSequencerReference,
      participant: LocalParticipantReference,
  ): Unit = {
    val port = portOfSequencer(sequencer)
    participant.synchronizers.connect(
      synchronizerAlias = synchronizerAlias,
      connection = s"http://127.0.0.1:$port",
    )
  }

  protected def restart(
      sequencer: RemoteSequencerReference,
      mediatorO: Option[LocalMediatorReference] = None,
      afterKill: => Unit = (),
      afterStart: => Unit = (),
  ): Unit = {
    mediatorO.foreach { mediator =>
      logger.info(s"Stopping mediator ${mediator.name}")
      mediator.stop()
    }
    logger.info(s"Killing sequencer ${sequencer.name}")
    external.kill(sequencer.name)
    afterKill
    logger.info(s"Starting sequencer ${sequencer.name}")
    external.start(sequencer.name)
    mediatorO.foreach { mediator =>
      logger.info(s"Starting mediator ${mediator.name}")
      mediator.start()
    }
    afterStart
    waitUntilRunning(sequencer)
    logger.info(s"Sequencer ${sequencer.name} is healthy after restart")
  }

  protected def startAndConnectAllParticipants(
      sequencer: RemoteSequencerReference
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    Seq(participant1, participant2).foreach { p =>
      p.start()
      connectSynchronizer(daName, sequencer, p)
    }
    checkAllConnected(sequencer)
  }

}

class SequencerRestartTest
    extends BaseSynchronizerRestartTest
    with FlagCloseable
    with HasCloseContext {

  // The max time between retries at the participant's sequencer client
  private val maxParticipantRetryInterval = 1.minute
  // Time taken for participant reconnection after it has woken up for a retry
  private val maxRecogniseParticipantReconnection = 20.seconds

  private val maxReconnectWait: FiniteDuration =
    maxParticipantRetryInterval.plus(maxRecogniseParticipantReconnection)

  private val baseEventCost = 500L
  private val trafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeNumeric.tryCreate(20 * 1000L),
    readVsWriteScalingFactor = InternalTrafficControlParameters.DefaultReadVsWriteScalingFactor,
    maxBaseTrafficAccumulationDuration = config.PositiveFiniteDuration.ofSeconds(10L),
    setBalanceRequestSubmissionWindowSize = config.PositiveFiniteDuration.ofMinutes(5L),
    enforceRateLimiting = true,
    baseEventCost = NonNegativeLong.tryCreate(baseEventCost),
  )

  "sequencer operates normally after restarting and participants reconnect to it automatically" in {
    implicit env =>
      import env.*

      enableTrafficManagement(initializedSynchronizers)

      startAndConnectAllParticipants(remoteSequencer1)

      participant1.health.ping(participant2.id)

      loggerFactory.suppressWarningsAndErrors {
        restart(
          remoteSequencer1,
          afterKill = {
            checkSequencerIsDown(remoteSequencer1)
            // Note: with the next line we test the crash recovery interaction with traffic control.
            // We reset the state of the BlockSequencer one block back, that will trigger
            // ErrorUtil.invalidState in the EnterpriseSequencerRateLimitManager.
            // Sequencer will then exit and the checks below will fail.
            // When done correctly, crash recovery will clean up the traffic control state
            // to one extra block back and no invalid state will be thrown.
            deleteLatestBlockCompletion(remoteSequencer1)
          },
        )
        checkAllConnected(remoteSequencer1, maxReconnectWait)
        checkMediatorConnected(maxReconnectWait)

        participant1.health.ping(participant2.id)
      }
  }

  private def enableTrafficManagement(
      initializedSynchronizers: mutable.Map[SynchronizerAlias, InitializedSynchronizer]
  ): Unit =
    initializedSynchronizers.foreach { case (_, synchronizer) =>
      synchronizer.synchronizerOwners.foreach { owner =>
        owner.topology.synchronizer_parameters.propose_update(
          synchronizerId = synchronizer.synchronizerId,
          _.update(trafficControl = Some(trafficControlParameters)),
        )
      }
    }

  "sequencer can be restarted before participants connecting" in { implicit env =>
    import env.*

    enableTrafficManagement(initializedSynchronizers)

    // the stopping of the synchronizer immediately after starting can potentially cause problems, so it can be good to have
    // a test in place for that
    loggerFactory.suppressWarningsAndErrors {
      restart(remoteSequencer1)

      startAndConnectAllParticipants(remoteSequencer1)
      checkMediatorConnected(maxReconnectWait)

      participant1.health.ping(participant2.id)
    }
  }

  "participants can reconnect manually after sequencer restarts" in { implicit env =>
    import env.*

    enableTrafficManagement(initializedSynchronizers)

    startAndConnectAllParticipants(remoteSequencer1)

    participant1.health.ping(participant2.id)

    participant1.synchronizers.disconnect(daName)
    participant2.synchronizers.disconnect(daName)

    checkAllDisconnected(remoteSequencer1)

    loggerFactory.suppressWarningsAndErrors {
      restart(remoteSequencer1, afterKill = checkSequencerIsDown(remoteSequencer1))

      participant1.synchronizers.reconnect(daName)
      participant2.synchronizers.reconnect(daName)

      checkAllConnected(remoteSequencer1)
      checkMediatorConnected(maxReconnectWait)

      participant1.health.ping(participant2.id, timeout = 40.seconds)
    }
  }

  "successfully restart during a bong and complete bong" taggedAs (
    ReliabilityTest(
      Component(
        "Participant",
        "connected to embedded non-replicated synchronizer and sending bong",
      ),
      AdverseScenario(
        dependency = "Sequencer",
        details =
          "Sequencer is forcefully stopped and then restarted while running a bong over participant",
      ),
      Remediation(
        remediator = "participant",
        action = "participants automatically reconnect to sequencer once it has restarted",
      ),
      outcome = "participant can process transactions whenever connected to the sequencer",
    ),
  ) in { implicit env =>
    import env.*

    console.set_command_timeout(200.seconds)

    startAndConnectAllParticipants(remoteSequencer1)

    val beforeBong = participant1.testing.pcs_search(daName).length
    logger.info(s"Performing a bong with 6 levels")

    loggerFactory.suppressWarningsAndErrors {

      val bongF = Future {
        participant2.testing.bong(
          Set(participant1.id, participant2.id),
          levels = 4,
          timeout = 120.seconds,
        )
      }

      // wait until bong started
      eventually(10.seconds) {
        assert(participant1.testing.pcs_search(daName).sizeIs > beforeBong + 10)
      }

      restart(remoteSequencer1)

      logger.info(s"Participants should reconnect to the sequencer of the synchronizer")
      checkAllConnected(remoteSequencer1, maxReconnectWait)
      checkMediatorConnected(maxReconnectWait)

      logger.info(s"The bong should complete successfully")
      val patience = defaultPatience.copy(timeout = 160.seconds)
      val duration = bongF
        .thereafter {
          case Failure(exception) =>
            logger.debug("Error in bong:", exception)
          case _ =>
        }
        .futureValue(patience, Position.here)
      logger.info(s"Completed a bong in $duration")
    }
  }
}
