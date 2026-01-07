// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import cats.syntax.option.*
import com.daml.tracing.NoOpTelemetry
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{AdminServerConfig, ApiLoggingConfig, FullClientConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseExternalProcess
import com.digitalasset.canton.integration.tests.crashrecovery.ClockResetIntegrationTest.startTimeServer
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.LifeCycle.toCloseableServer
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencer
import com.digitalasset.canton.time.{Clock, SimClock}
import com.digitalasset.canton.topology.admin.v30

import scala.concurrent.Future

class ClockResetIntegrationTest extends ParticipantRestartTest with HasCycleUtils {

  private lazy val separateClockPort = UniquePortGenerator.next

  override lazy val external = new UseExternalProcess(
    loggerFactory,
    externalParticipants = Set("participant1"),
    fileNameHint = this.getClass.getSimpleName,
    clockServer = FullClientConfig(port = separateClockPort).some,
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ProgrammableSequencer.configOverride(this.getClass.toString, loggerFactory),
        ConfigTransforms.updateMaxDeduplicationDurations(java.time.Duration.ofMinutes(1)),
      )

  "clock may be reset during a crash" in { implicit env =>
    import env.*

    val commonSimClock = environment.simClock.value
    // Create a separate sim clock for participant1
    val separateSimClock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
    startTimeServer(separateSimClock, separateClockPort, loggerFactory)

    NetworkBootstrapper(Seq(EnvironmentDefinition.S1M1)).bootstrap()

    val participant1 = startAndGet("participant1")
    connectToDa(participant1)

    val commandId = "cycle-command"
    runCycle(participant1.adminParty, participant1, participant1, commandId)
    val pruningOffset = participant1.ledger_api.state.end()

    val tsReset = CantonTimestamp.ofEpochSecond(40)
    val ts1 = CantonTimestamp.ofEpochSecond(61)
    separateSimClock.advanceTo(ts1)
    commonSimClock.advanceTo(ts1)

    // Run another cycle to make sure that the participant has observed the time
    runCycle(participant1.adminParty, participant1, participant1)

    pruneParticipant(participant1, pruningOffset)

    restart(
      participant1, {
        logger.debug("Resetting the sim clock of participant 1")
        separateSimClock.reset()
        separateSimClock.advanceTo(tsReset)
      },
    )

    participant1.synchronizers.reconnect_all()
    withClue("id should be set after restart") {
      participant1.health.initialized() shouldBe true
    }
    logger.debug(s"Restarted participant1")
    eventually() {
      withClue("synchronizer should be connected after restart") {
        participant1.synchronizers.list_connected() should have length 1
      }
    }
    logger.info(s"Participant1 is connected again after restart")

    // Run the same cycle with the same command ID again.
    // This should not cause a deduplication failure because the participant should realize that the clock has been reset.
    runCycle(participant1.adminParty, participant1, participant1, commandId)
  }
}

object ClockResetIntegrationTest {
  def startTimeServer(clock: Clock, port: Port, loggerFactory: NamedLoggerFactory)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val clockServerConfig = AdminServerConfig(internalPort = port.some)
    val timeService = new v30.IdentityInitializationServiceGrpc.IdentityInitializationService {
      override def initId(request: v30.InitIdRequest): Future[v30.InitIdResponse] = ???
      override def getId(request: v30.GetIdRequest): Future[v30.GetIdResponse] = ???
      override def currentTime(request: v30.CurrentTimeRequest): Future[v30.CurrentTimeResponse] =
        Future.successful(v30.CurrentTimeResponse(clock.now.toProtoPrimitive))
    }
    val separateTimeServer = {
      val serverBuilder = CantonServerBuilder
        .forConfig(
          clockServerConfig,
          None,
          executionContext,
          loggerFactory,
          apiLoggingConfig = ApiLoggingConfig(messagePayloads = false),
          environment.config.monitoring.tracing,
          ParticipantTestMetrics.grpcMetrics,
          NoOpTelemetry,
        )
        .addService(
          v30.IdentityInitializationServiceGrpc.bindService(timeService, executionContext)
        )
      val server = serverBuilder.build.start()
      toCloseableServer(server, loggerFactory.getTracedLogger(this.getClass), "TimeServer")
    }
    environment.addUserCloseable(separateTimeServer)
  }
}
