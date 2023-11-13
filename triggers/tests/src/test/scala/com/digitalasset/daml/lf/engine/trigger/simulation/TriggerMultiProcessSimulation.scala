// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package simulation

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, ChildFailed, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.client.LedgerClient
import com.daml.lf.engine.trigger.simulation.process.TriggerProcessFactory
import com.daml.lf.engine.trigger.simulation.process.ledger.LedgerProcess
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.engine.trigger.test.AbstractTriggerTest.allocateParty
import com.daml.lf.speedy.{SValue, Speedy}
import com.daml.lf.testing.parser
import com.daml.lf.testing.parser.parseExpr
import com.daml.logging.LoggingContext
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Files, Path}
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

abstract class TriggerMultiProcessSimulation extends AsyncWordSpec with AbstractTriggerTest {

  import TriggerMultiProcessSimulation._

  protected implicit lazy val simulationConfig: TriggerSimulationConfig =
    TriggerSimulationConfig()

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  private val simulationTerminatedNormally = Promise[Unit]()

  "Multi process trigger simulation" in {
    val simulation: ActorSystem[Message] =
      ActorSystem(triggerMultiProcessSimulationWithTimeout, "simulation")

    for {
      _ <- simulation.whenTerminated
      _ <- simulationTerminatedNormally.future
    } yield succeed
  }

  /** User simulations need to (at least) override this method in order to define a trigger multi-process simulation.
    *
    * @return trigger multi-process actor system
    */
  protected def triggerMultiProcessSimulation: Behavior[Unit]

  private[this] def triggerMultiProcessSimulationWithTimeout: Behavior[Message] = {
    Behaviors.withTimers[Message] { timer =>
      timer.startSingleTimer(StopSimulation, simulationConfig.simulationDuration)

      Behaviors
        .supervise[Message] {
          Behaviors.setup { rootContext =>
            var simulationController: Option[ActorRef[Unit]] = None

            rootContext.log.info(s"Simulation will run for ${simulationConfig.simulationDuration}")
            rootContext.self ! StartSimulation

            Behaviors.logMessages {
              Behaviors
                .receiveMessage[Message] {
                  case StartSimulation =>
                    simulationController = Some(
                      rootContext.spawn(triggerMultiProcessSimulation, "simulation-controller")
                    )
                    simulationController.foreach(rootContext.watch)
                    Behaviors.same

                  case StopSimulation =>
                    rootContext.log.info(
                      s"Simulation stopped after ${simulationConfig.simulationDuration}"
                    )
                    simulationController.foreach(rootContext.stop)
                    simulationController = None
                    simulationTerminatedNormally.success(())
                    Behaviors.stopped
                }
                .receiveSignal { case (_, ChildFailed(_, cause)) =>
                  simulationTerminatedNormally.failure(cause)
                  Behaviors.stopped
                }
            }
          }
        }
        .onFailure[Throwable](SupervisorStrategy.stop)
    }
  }

  protected def triggerProcessFactory(
      client: LedgerClient,
      ledger: ActorRef[LedgerProcess.Message],
      name: String,
      actAs: ApiTypes.Party,
  ): TriggerProcessFactory = {
    new TriggerProcessFactory(
      client,
      ledger,
      name,
      packageId,
      applicationId,
      compiledPackages,
      timeProviderType,
      triggerRunnerConfiguration,
      actAs,
    )
  }

  protected def withLedger(
      spawnTriggers: (
          LedgerClient,
          ActorRef[LedgerProcess.Message],
          ApiTypes.Party,
          ActorContext[Unit],
      ) => Behavior[Unit]
  )(implicit applicationId: ApiTypes.ApplicationId): Behavior[Unit] = {
    Behaviors.setup { controllerContext =>
      val setup = for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
      } yield (client, party)
      val (client, actAs) = Await.result(setup, simulationConfig.simulationSetupTimeout)
      val ledger = controllerContext.spawn(LedgerProcess.create(client), "ledger")

      controllerContext.watch(ledger)

      spawnTriggers(client, ledger, actAs, controllerContext)
    }
  }

  protected def safeSValueFromLf(lfValue: String): Either[String, SValue] = {
    val parserParameters: parser.ParserParameters[this.type] =
      parser.ParserParameters.defaultFor(majorLanguageVersion).copy(defaultPackageId = packageId)

    parseExpr(lfValue)(parserParameters).flatMap(expr =>
      Speedy.Machine
        .runPureExpr(expr, compiledPackages)(LoggingContext.ForTesting)
        .left
        .map(_.toString)
    )
  }

  protected def unsafeSValueFromLf(lfValue: String): SValue = {
    safeSValueFromLf(lfValue).left.map(cause => throw new RuntimeException(cause)).toOption.get
  }
}

object TriggerMultiProcessSimulation {

  // If simulation CSV data is to be kept, then we need to run our bazel tests with "--test_tmpdir=/tmp/" or similar
  // (otherwise bazel will remove the directory holding the saved CSV data)
  private val tmpDir = Files.createTempDirectory("TriggerSimulation")

  println(s"Trigger simulation reporting data is located in $tmpDir")

  final case class TriggerSimulationConfig(
      simulationSetupTimeout: FiniteDuration = 30.seconds,
      simulationDuration: FiniteDuration = 5.minutes,
      ledgerSubmissionTimeout: FiniteDuration = 30.seconds,
      ledgerRegistrationTimeout: FiniteDuration = 30.seconds,
      ledgerWorkloadTimeout: FiniteDuration = 5.seconds,
      triggerDataFile: Path = tmpDir.resolve("trigger-simulation-metrics-data.csv"),
      acsDataFile: Path = tmpDir.resolve("trigger-simulation-acs-data.csv"),
      submissionDataFile: Path = tmpDir.resolve("trigger-simulation-submission-data.csv"),
  )

  final case class TriggerSimulationFailure(cause: Throwable) extends Exception

  object TriggerSimulationFailure {
    def apply(reason: String): TriggerSimulationFailure =
      TriggerSimulationFailure(new RuntimeException(reason))
  }

  abstract class Message extends Product with Serializable
  private case object StartSimulation extends Message
  private case object StopSimulation extends Message
}
