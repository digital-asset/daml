// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package simulation

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Materializer
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.sandbox.SandboxOnXForTest.ParticipantId
import com.daml.lf.engine.trigger.simulation.process.{LedgerProcess, TriggerProcessFactory}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

abstract class TriggerMultiProcessSimulation
    extends AsyncWordSpec
    with SuiteResourceManagementAroundAll
    with AbstractTriggerTest {

  import TriggerMultiProcessSimulation._

  // FIXME: changed default value for test debugging
  protected implicit lazy val simulationConfig: TriggerSimulationConfig =
    TriggerSimulationConfig(simulationDuration = 30.seconds)

  protected implicit lazy val simulation: ActorSystem[Unit] =
    ActorSystem(triggerMultiProcessSimulationWithTimeout, "cat-and-food-simulation")

  override implicit lazy val materializer: Materializer = Materializer(simulation)

  override implicit lazy val executionContext: ExecutionContext = materializer.executionContext

  override protected implicit val applicationId: ApplicationId = ApplicationId(
    "trigger-multi-process-simulation"
  )

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  "Multi process trigger simulation" in {
    for {
      _ <- simulation.whenTerminated
    } yield succeed
  }

  /** User simulation need to (at least) override this method in order to define a trigger multi-process simulation. If
    * the user implementation continues as the `super.triggerMultiProcessSimulation` behavior, then simulations will be
    * bounded using time durations from the simulation configuration. If they do not continue with this behaviour, then
    * their runtime will be bounded by the duration of the test (i.e. configured using bazel).
    *
    * @return trigger multi-process actor system
    */
  protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    Behaviors.receive { (context, _) =>
      context.log.info(s"Simulation timed out after: ${simulationConfig.simulationDuration}")
      Behaviors.stopped
    }
  }

  private[this] def triggerMultiProcessSimulationWithTimeout: Behavior[Unit] = {
    Behaviors.withTimers[Unit] { timer =>
      timer.startSingleTimer((), simulationConfig.simulationDuration)

      Behaviors
        .supervise(triggerMultiProcessSimulation)
        .onFailure[Throwable](SupervisorStrategy.stop)
    }
  }

  protected def triggerProcessFactory(
      client: LedgerClient,
      ledger: ActorRef[LedgerProcess.LedgerManagement],
      name: String,
      actAs: Party,
  ): TriggerProcessFactory = {
    new TriggerProcessFactory(
      client,
      ledger,
      name,
      packageId,
      applicationId,
      compiledPackages,
      config.participants(ParticipantId).apiServer.timeProviderType,
      triggerRunnerConfiguration,
      actAs,
    )
  }
}

object TriggerMultiProcessSimulation {

  final case class TriggerSimulationConfig(
      simulationSetupTimeout: FiniteDuration = 30.seconds,
      simulationDuration: FiniteDuration = 5.minutes,
      ledgerSubmissionTimeout: FiniteDuration = 30.seconds,
      ledgerRegistrationTimeout: FiniteDuration = 30.seconds,
      ledgerWorkloadTimeout: FiniteDuration = 1.second,
      // FIXME:
      triggerDataFile: String = "/tmp/trigger-simulation-data.csv",
      acsDataFile: String = "/tmp/trigger-simulation-acs-data.csv",
  )

  final case class TriggerSimulationFailure(cause: Throwable) extends Exception

  object TriggerSimulationFailure {
    def apply(reason: String): TriggerSimulationFailure =
      TriggerSimulationFailure(new RuntimeException(reason))
  }
}
