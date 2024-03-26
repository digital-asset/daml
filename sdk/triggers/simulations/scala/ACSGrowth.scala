// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.lf.data.Ref
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.process.wrapper.TriggerTimer

import java.nio.file.{Path, Paths}
import scala.concurrent.duration._

class GenericACSGrowth(triggerName: String) extends TriggerMultiProcessSimulation {

  override protected lazy val darFile: Either[Path, Path] =
    Right(
      Paths.get(
        Option(System.getenv("DAR")).getOrElse(
          throw new RuntimeException(
            "Trigger simulation needs a Dar file specified using the environment variable: DAR"
          )
        )
      )
    )

  // For demonstration purposes, we only run the simulation for 30 seconds
  override protected implicit lazy val simulationConfig: TriggerSimulationConfig =
    TriggerSimulationConfig(simulationDuration = 30.seconds)

  override protected val cantonFixtureDebugMode = CantonFixtureDebugKeepTmpFiles

  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit val applicationId: Option[Ref.ApplicationId] = this.applicationId

    withLedger { (client, ledger, actAs, controllerContext) =>
      val triggerFactory = triggerProcessFactory(client, ledger, s"Cats:$triggerName", actAs)
      val startState = unsafeSValueFromLf("Types:Tuple2 { _1 = False, _2 = 0 }")
      val trigger =
        controllerContext.spawn(triggerFactory.create(startState, Seq.empty), triggerName)

      controllerContext.watch(trigger)
      controllerContext.spawn(
        TriggerTimer.messageWithFixedDelay(1.second, 1.second)(trigger),
        s"timed-$triggerName",
      )

      Behaviors.empty
    }
  }
}

class SlowACSGrowth extends GenericACSGrowth("slowBreedingTrigger")

class MediumACSGrowth extends GenericACSGrowth("mediumBreedingTrigger")

class FastACSGrowth extends GenericACSGrowth("fastBreedingTrigger")
