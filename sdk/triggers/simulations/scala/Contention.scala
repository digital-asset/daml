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

class GenericContention(delay: FiniteDuration) extends TriggerMultiProcessSimulation {

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
      val breedingTriggerFactory =
        triggerProcessFactory(client, ledger, "Cats:slowBreedingTrigger", actAs)
      val pettingTriggerFactory =
        triggerProcessFactory(client, ledger, "Cats:pettingTrigger", actAs)
      val cullingTriggerFactory =
        triggerProcessFactory(client, ledger, "Cats:cullingTrigger", actAs)
      val breedingStartState = unsafeSValueFromLf("Types:Tuple2 { _1 = False, _2 = 0 }")
      val startState = unsafeSValueFromLf(
        "Types:Tuple2 { _1 = Nil @(ContractId Cats:Cat), _2 = None @(ContractId Cats:Cat) }"
      )
      val breedingTrigger = controllerContext.spawn(
        breedingTriggerFactory.create(breedingStartState, Seq.empty),
        "slowBreedingTrigger",
      )
      val pettingTrigger = controllerContext.spawn(
        pettingTriggerFactory.create(startState, Seq.empty),
        "pettingTrigger",
      )
      val cullingTrigger = controllerContext.spawn(
        cullingTriggerFactory.create(startState, Seq.empty),
        "cullingTrigger",
      )

      controllerContext.watch(breedingTrigger)
      controllerContext.watch(pettingTrigger)
      controllerContext.watch(cullingTrigger)
      controllerContext.spawn(
        TriggerTimer.regularMessage(1.second)(breedingTrigger),
        "timed-slowBreedingTrigger",
      )
      controllerContext.spawn(
        TriggerTimer.regularMessage(delay)(pettingTrigger),
        "timed-pettingTrigger",
      )
      controllerContext.spawn(
        TriggerTimer.regularMessage(1.second)(cullingTrigger),
        "timed-cullingTrigger",
      )

      Behaviors.empty
    }
  }
}

class SlowContention extends GenericContention(1.second)

class MediumContention extends GenericContention(500.milliseconds)

class FastContention extends GenericContention(250.milliseconds)
