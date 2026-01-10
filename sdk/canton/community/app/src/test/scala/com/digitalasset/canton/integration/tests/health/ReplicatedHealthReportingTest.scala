// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.health

import com.digitalasset.canton.admin.api.client.data.{
  ComponentHealthState,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{Port, PositiveInt}
import com.digitalasset.canton.console.{
  InstanceReference,
  LocalInstanceReference,
  LocalMediatorReference,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseReferenceBlockSequencer,
  UseSharedStorage,
}
import com.digitalasset.canton.integration.tests.*
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.sync.{ConnectedSynchronizer, SyncEphemeralState}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.client.{DelayedSequencerClient, SequencerClient}
import com.digitalasset.canton.synchronizer.block.*
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.ReferenceSequencerDriverFactory
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SynchronizerAlias}
import com.google.protobuf.ByteString
import io.grpc.health.v1.health.HealthCheckResponse.ServingStatus
import io.grpc.health.v1.health.{HealthCheckRequest, HealthGrpc}
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.{ManagedChannel, ServerServiceDefinition}
import monocle.macros.syntax.lens.*
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.{KillSwitch, Materializer}
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

trait HealthReportingTestHelper
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with HealthMonitoringTestUtils
    with ReplicatedNodeHelper {

  case class ActivePassiveStubs(
      activeStub: HealthGrpc.HealthStub,
      passiveStub: HealthGrpc.HealthStub,
  )

  case class HealthStubs(
      healthEndpointStubs: ActivePassiveStubs,
      adminEndpointStubs: ActivePassiveStubs,
  )

  protected lazy val baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M2_Config
      .addConfigTransforms(
        ConfigTransforms.enableReplicatedAllNodes*
      )
      .addConfigTransforms(
        ConfigTransforms.addMonitoringEndpointAllNodes*
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.health.backendCheckPeriod).replace(NonNegativeFiniteDuration.ofSeconds(1))
        ),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.sequencerClient.warnDisconnectDelay)
            .replace(NonNegativeFiniteDuration.ofMillis(1))
        ),
        ConfigTransforms.updateAllMediatorConfigs_(
          _.focus(_.sequencerClient.warnDisconnectDelay)
            .replace(NonNegativeFiniteDuration.ofMillis(1))
        ),
      )
      .updateTestingConfig(
        _.focus(_.testSequencerClientFor).replace(
          Set(
            TestSequencerClientFor("test", "mediator1", "health-test-synchronizer"),
            TestSequencerClientFor("test", "mediator2", "health-test-synchronizer"),
          )
        )
      )

  protected val synchronizerAlias = SynchronizerAlias.tryCreate("health-test-synchronizer")
  protected var synchronizerId: PhysicalSynchronizerId = _

  protected def setupPlugins(storagePlugin: EnvironmentSetupPlugin): Unit = {
    registerPlugin(storagePlugin)
    registerPlugin(
      UseSharedStorage.forMediators("mediator1", Seq("mediator2"), loggerFactory)
    )
    registerPlugin(
      UseSharedStorage.forParticipants("participant1", Seq("participant2"), loggerFactory)
    )
    // TODO(#16089): Should this test also test sequencer HA when it is available?
  }

  protected def bootstrapSynchronizer(
      mediator: LocalMediatorReference
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    synchronizerId = bootstrap
      .synchronizer(
        synchronizerName = "health-test-synchronizer",
        sequencers = Seq(sequencer1),
        mediators = Seq(mediator),
        synchronizerOwners = Seq[InstanceReference](sequencer1, mediator),
        synchronizerThreshold = PositiveInt.two,
        staticSynchronizerParameters =
          StaticSynchronizerParameters.defaults(sequencer1.config.crypto, testedProtocolVersion),
      )
    sequencer1.health.wait_for_initialized()
    sequencer1.topology.synchronisation.await_idle()
  }

  protected def bootstrapSynchronizer(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    bootstrapSynchronizer(waitUntilOneActive(mediator1, mediator2, allowNonInit = true))
  }
}

trait HealthMonitoringTestUtils { this: BaseTest =>
  private val healthCheckTimeout = 2.seconds

  // Create stubs from a seq of address / port pairs
  protected def withHealthStubs[T](
      healthEndpoints: Seq[ServerConfig]
  )(
      f: PartialFunction[Seq[HealthGrpc.HealthStub], T]
  )(implicit env: TestConsoleEnvironment): T =
    withHealthStubPairs(healthEndpoints.map(he => (he.address, he.port)))(f)

  protected def withHealthStubPairs[T](
      healthEndpoints: Seq[(String, Port)]
  )(
      f: PartialFunction[Seq[HealthGrpc.HealthStub], T]
  )(implicit env: TestConsoleEnvironment): T = {
    val channelsAndStubs = healthEndpoints.map { case (address, port) =>
      val channel: ManagedChannel = NettyChannelBuilder
        .forAddress(address, port.unwrap)
        .usePlaintext()
        .executor(env.executionContext)
        .build()

      val stub: HealthGrpc.HealthStub = HealthGrpc.stub(channel)
      (channel, stub)
    }

    try {
      f(channelsAndStubs.map(_._2))
    } finally {
      channelsAndStubs.map(_._1).foreach(_.shutdownNow())
    }
  }

  protected def checkHealth(
      expected: ServingStatus,
      stub: HealthGrpc.HealthStub,
      service: String = "",
  ): Assertion = {
    logger.info(s"Checking health of '$service', expected '$expected'")
    val res = Await
      .result(stub.check(HealthCheckRequest(service)), healthCheckTimeout)
      .status
    logger.info(
      s"Response for checking health of '$service', expected '$expected', received: '$res'"
    )
    res shouldBe expected
  }

  protected def checkLivenessServing(stub: HealthGrpc.HealthStub): Assertion =
    checkHealth(ServingStatus.SERVING, stub, "liveness")

  protected def checkLivenessNotServing(stub: HealthGrpc.HealthStub): Assertion =
    checkHealth(ServingStatus.NOT_SERVING, stub, "liveness")

  protected def checkContainsState[S <: ComponentHealthState: ClassTag](
      node: LocalInstanceReference,
      name: String,
  ): Assertion =
    node.health.status.trySuccess.components.exists {
      case dep if dep.name == name =>
        dep.state match {
          case _: S => true
          case _ => false
        }
      case _ => false
    } shouldBe true

  protected def checkServing(stub: HealthGrpc.HealthStub, service: String = ""): Assertion =
    checkHealth(ServingStatus.SERVING, stub, service)
  protected def checkNotServing(stub: HealthGrpc.HealthStub, service: String = ""): Assertion =
    checkHealth(ServingStatus.NOT_SERVING, stub, service)
}

trait HealthReportingIndividualNodeTest extends HealthReportingTestHelper {

  "report health on the health endpoint on a participant" in { implicit env =>
    import env.*

    val activeParticipant = waitUntilOneActive(participant1, participant2)
    val passiveParticipant =
      if (activeParticipant.name == participant1.name) participant2 else participant1

    val activeConfig = env.actualConfig.participantsByString(activeParticipant.name)
    val passiveConfig = env.actualConfig.participantsByString(passiveParticipant.name)
    withHealthStubs(
      Seq(
        activeConfig.monitoring.grpcHealthServer.value,
        passiveConfig.monitoring.grpcHealthServer.value,
        activeConfig.ledgerApi,
      )
    ) {
      case Seq(
            healthActive,
            healthPassive,
            ledgerActive,
          ) =>
        checkLivenessServing(healthActive)

        checkContainsState[ComponentHealthState.Failed](passiveParticipant, DbStorage.healthName)
        checkContainsState[ComponentHealthState.Ok](activeParticipant, DbStorage.healthName)
        checkContainsState[ComponentHealthState.Failed](
          activeParticipant,
          ConnectedSynchronizer.healthName,
        )
        checkContainsState[ComponentHealthState.Failed](
          activeParticipant,
          SyncEphemeralState.healthName,
        )
        checkContainsState[ComponentHealthState.Failed](
          activeParticipant,
          SequencerClient.healthName,
        )
        checkServing(healthActive)
        checkNotServing(ledgerActive)

        // Passive participant is not serving
        checkNotServing(healthPassive)

        // Bootstrap the synchronizer and connect the participant
        bootstrapSynchronizer

        // Connect to synchronizer and ping to make sure it's working
        activeParticipant.synchronizers.connect_local(sequencer1, synchronizerAlias)
        activeParticipant.health.wait_for_initialized()
        activeParticipant.health.ping(activeParticipant)

        checkContainsState[ComponentHealthState.Ok](activeParticipant, DbStorage.healthName)
        checkContainsState[ComponentHealthState.Ok](
          activeParticipant,
          ConnectedSynchronizer.healthName,
        )
        checkContainsState[ComponentHealthState.Ok](
          activeParticipant,
          SyncEphemeralState.healthName,
        )
        checkContainsState[ComponentHealthState.Ok](
          activeParticipant,
          SequencerClient.healthName,
        )

        eventually() {
          // Active should be serving
          checkLivenessServing(healthActive)
          checkServing(healthActive)
          checkServing(ledgerActive)

          // But not passive
          checkNotServing(healthPassive)
          checkLivenessServing(healthPassive)
        }
    }
  }

  "report health on the health endpoint of a mediator" in { implicit env =>
    import env.*

    val activeMediator = waitUntilOneActive(mediator1, mediator2, allowNonInit = true)
    val passiveMediator =
      if (activeMediator.name == mediator1.name) mediator2 else mediator1

    val activeConfig = env.actualConfig.mediatorsByString(activeMediator.name)
    val passiveConfig = env.actualConfig.mediatorsByString(passiveMediator.name)

    withHealthStubs(
      Seq(
        activeConfig.monitoring.grpcHealthServer.value,
        passiveConfig.monitoring.grpcHealthServer.value,
      )
    ) { case Seq(healthActive, healthPassive) =>
      eventually() {
        checkServing(healthActive)
        checkLivenessServing(healthActive)
        checkNotServing(healthPassive)
        checkLivenessServing(healthPassive)
      }

      bootstrapSynchronizer

      eventually() {
        checkContainsState[ComponentHealthState.Ok](activeMediator, DbStorage.healthName)
      }
    }
  }

  "report mediator unhealthy when its sequencer connection is lost" in { implicit env =>
    import env.*

    val activeMediator = waitUntilOneActive(mediator1, mediator2, allowNonInit = true)
    val passiveMediator =
      if (activeMediator.name == mediator1.name) mediator2 else mediator1

    val activeConfig = env.actualConfig.mediatorsByString(activeMediator.name)
    val passiveConfig = env.actualConfig.mediatorsByString(passiveMediator.name)

    withHealthStubs(
      Seq(
        activeConfig.monitoring.grpcHealthServer.value,
        passiveConfig.monitoring.grpcHealthServer.value,
      )
    ) { case Seq(healthActive, healthPassive) =>
      eventually() {
        checkServing(healthActive)
        checkLivenessServing(healthActive)
        checkNotServing(healthPassive)
        checkLivenessServing(healthPassive)
      }

      bootstrapSynchronizer

      DelayedSequencerClient
        .delayedSequencerClient(
          "test",
          synchronizerId,
          activeMediator.id.uid.toString,
        )
        .value
        .setDelayPolicy { (_: SequencedSerializedEvent) =>
          throw new RuntimeException("Testing the liveness of mediator")
        }

      val activeParticipant = waitUntilOneActive(participant1, participant2)
      // connecting participant will send topology transaction to mediator which will trigger the error
      loggerFactory.assertLogs(SuppressionRule.Level(Level.ERROR))(
        activeParticipant.synchronizers.connect_local(sequencer1, synchronizerAlias),
        _.errorMessage should include("Testing the liveness of mediator"),
      )

      eventually() {
        checkNotServing(healthActive)
        checkLivenessNotServing(healthActive)
        checkContainsState[ComponentHealthState.Failed](activeMediator, SequencerClient.healthName)
      }
    }
  }

  "report health for a sequencer" in { implicit env =>
    import env.*

    val seq1Config = env.actualConfig.sequencersByString(sequencer1.name)

    withHealthStubs(
      Seq(
        seq1Config.monitoring.grpcHealthServer.value,
        seq1Config.publicApi,
      )
    ) { case Seq(health, seq) =>
      eventually() {
        // When not initialized the sequencer API should expose the health service but return not serving
        checkNotServing(seq)
        checkNotServing(seq, CantonGrpcUtil.sequencerHealthCheckServiceName)
        checkServing(health)
        checkLivenessServing(health)
      }

      bootstrapSynchronizer

      eventually() {
        // Once initialized we should show serving
        checkServing(seq)
        checkServing(seq, CantonGrpcUtil.sequencerHealthCheckServiceName)
        checkContainsState[ComponentHealthState.Ok](sequencer1, Sequencer.healthName)
        checkContainsState[ComponentHealthState.Ok](sequencer1, DbStorage.healthName)
      }
    }
  }
}

class HealthReportingNodeReferenceIntegrationTestPostgres
    extends HealthReportingIndividualNodeTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory
    ) {
      override protected val driverFactory: ReferenceSequencerDriverFactory =
        new SequencerHealthDriverTestFactory
    }
  )
  setupPlugins(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition

  "participant should detect unhealthy sequencer when driver is unhealthy" in { implicit env =>
    import env.*

    val seq1Config = env.actualConfig.sequencersByString(sequencer1.name)
    val activeParticipant = waitUntilOneActive(participant1, participant2)
    val activeConfig = env.actualConfig.participantsByString(activeParticipant.name)

    bootstrapSynchronizer

    withHealthStubs(
      Seq(
        activeConfig.monitoring.grpcHealthServer.value,
        seq1Config.publicApi,
      )
    ) {
      case Seq(
            par,
            seq,
          ) =>
        // Bootstrap the synchronizer and connect the participant
        bootstrapSynchronizer

        // Make sure the sequencer is serving
        checkServing(seq)
        checkServing(seq, CantonGrpcUtil.sequencerHealthCheckServiceName)

        // Connect to synchronizer and ping to make sure it's working
        activeParticipant.synchronizers.connect_local(sequencer1, synchronizerAlias)
        activeParticipant.health.wait_for_initialized()
        activeParticipant.health.ping(activeParticipant)

        checkContainsState[ComponentHealthState.Ok](activeParticipant, DbStorage.healthName)
        checkContainsState[ComponentHealthState.Ok](
          activeParticipant,
          ConnectedSynchronizer.healthName,
        )
        checkContainsState[ComponentHealthState.Ok](
          activeParticipant,
          SyncEphemeralState.healthName,
        )
        checkContainsState[ComponentHealthState.Ok](
          activeParticipant,
          SequencerClient.healthName,
        )

        eventually() {
          checkLivenessServing(par)
          checkServing(par)
        }

        val usingConnectionPool = participant1.config.sequencerClient.useNewConnectionPool
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            // Set the driver health to unhealthy
            SequencerHealthDriverTestFactory.healthOverride = Some(
              Future.successful(
                SequencerDriverHealthStatus(isActive = false, Some("Testing driver unhealthy"))
              )
            )

            clue("sequencer is not serving") {
              eventually() {
                checkNotServing(seq)
                checkNotServing(seq, CantonGrpcUtil.sequencerHealthCheckServiceName)
              }
            }

            clue("participant sequencer client is in failed state") {
              eventually() {
                checkContainsState[ComponentHealthState.Failed](
                  activeParticipant,
                  SequencerClient.healthName,
                )
              }
            }
          },
          LogEntry.assertLogSeq(
            mustContainWithClue =
              if (usingConnectionPool) {
                // TODO(i28761): Subscription should warn after it is lost
                Seq(
                  (
                    _.warningMessage should include("Sequencer is unhealthy"),
                    "expected sequencer is not health",
                  )
                )
              } else
                Seq(
                  (
                    _.warningMessage should include("Sequencer is unhealthy"),
                    "expected sequencer is not health",
                  ),
                  (
                    _.warningMessage should include("SEQUENCER_SUBSCRIPTION_LOST"),
                    "expected sequencer subscription lost",
                  ),
                ),
            mayContain = Seq.empty,
          ),
        )

    }
  }
}

class HealthReportingNodeBftOrderingIntegrationTestPostgres
    extends HealthReportingIndividualNodeTest {
  registerPlugin(new UseBftSequencer(loggerFactory))
  setupPlugins(new UsePostgres(loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition
}

object SequencerHealthDriverTestFactory {
  @volatile
  var healthOverride: Option[Future[SequencerDriverHealthStatus]] = None
}

// NOTE: This driver factory derives its health state from a singleton mutable variable.
// It is used to toggle off the health signal at will during the test.
// DO NOT USE this class in any other test. It is public because its loaded reflectively by the `ServiceLoader`.
class SequencerHealthDriverTestFactory extends ReferenceSequencerDriverFactory {

  private val delegate = new ReferenceSequencerDriverFactory

  override def name: String = "reference-health-test" // Avoids clashes with other tests

  override def create(
      config: ConfigType,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      synchronizerId: String,
      sequencerId: String,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): SequencerDriver = {
    val driver = delegate.create(
      config,
      nonStandardConfig,
      timeProvider,
      firstBlockHeight,
      synchronizerId,
      sequencerId,
      loggerFactory,
    )

    new SequencerDriver {
      override def firstBlockHeight: Long = SequencerDriver.DefaultInitialBlockHeight

      override def adminServices: Seq[ServerServiceDefinition] = driver.adminServices
      override def acknowledge(acknowledgement: ByteString)(implicit
          traceContext: TraceContext
      ): Future[Unit] = driver.acknowledge(acknowledgement)
      override def send(request: ByteString, submissionId: String, senderId: String)(implicit
          traceContext: TraceContext
      ): Future[Unit] =
        driver.send(request, submissionId, senderId)
      override def subscribe()(implicit
          traceContext: TraceContext
      ): Source[RawLedgerBlock, KillSwitch] = driver.subscribe()
      override def sequencingTime(implicit
          traceContext: TraceContext
      ): Future[Option[Long]] =
        driver.sequencingTime
      override def health(implicit
          traceContext: TraceContext
      ): Future[SequencerDriverHealthStatus] =
        SequencerHealthDriverTestFactory.healthOverride.getOrElse(driver.health)
      override def close(): Unit = driver.close()
    }
  }
}
