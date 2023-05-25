// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation
import com.daml.lf.engine.trigger.simulation.process.ledger.LedgerProcess
import com.daml.lf.engine.trigger.simulation.process.wrapper.TriggerTimer
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue

import java.nio.file.{Path, Paths}
import scala.concurrent.Await
import scala.concurrent.duration._

class LaunchTriggerSimulation extends TriggerMultiProcessSimulation {

  import AbstractTriggerTest._

  override protected lazy val darFile: Either[Path, Path] = Option(System.getenv("DAR")) match {
    case Some(darFileName) =>
      Right(Paths.get(darFileName))

    case None =>
      throw new RuntimeException("Need to define the Dar file containing the trigger code by setting the environment variable: DAR")
  }

  override protected val cantonFixtureDebugMode: Boolean = true

  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit def applicationId: ApiTypes.ApplicationId = this.applicationId

    Behaviors.setup { context =>
      Option(System.getenv("TRIGGER")) match {
        case Some(name) =>
          context.log.info(s"Simulation will launch trigger: $name")
          val setup = for {
            client <- defaultLedgerClient()
            party <- allocateParty(client)
          } yield (client, party)
          context.log.info("DEBUGGY: here")
          val (client, actAs) = Await.result(setup, simulationConfig.simulationSetupTimeout)
          context.log.info("DEBUGGY: here-0")
          val ledger = context.spawn(LedgerProcess.create(client), "ledger")
          context.log.info("DEBUGGY: here-1")
          val triggerFactory = triggerProcessFactory(client, ledger, name, actAs)
          context.log.info("DEBUGGY: here-2")
          val trigger = context.spawn(triggerFactory.create(SValue.SInt64(100L), Seq.empty), "trigger")
          context.log.info("DEBUGGY: here-3")
          val ref = context.spawn(TriggerTimer.messageWithFixedDelay(1.second, 1.second)(trigger), "triggerWithHeartbeat")
          context.log.info(s"DEBUGGY: $ref")

          Behaviors.empty

        case None =>
          context.log.error("Need to define the trigger to run by setting the environment variable: TRIGGER")
          Behaviors.stopped
      }
    }
  }
}
