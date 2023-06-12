// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.process.wrapper.TriggerTimer

import java.nio.file.{Path, Paths}
import scala.concurrent.duration._

class LimitedContention extends TriggerMultiProcessSimulation {

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

  override protected val cantonFixtureDebugMode: Boolean = true

  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit val applicationId: ApiTypes.ApplicationId = this.applicationId

    withLedger { (client, ledger, actAs, controllerContext) =>
      val breedingTriggerFactory =
        triggerProcessFactory(client, ledger, "Cats:slowBreedingTrigger", actAs)
      val cullingTriggerFactory =
        triggerProcessFactory(client, ledger, "Cats:cullingTrigger", actAs)
      val breedingStartState = unsafeSValueFromLf("Types:Tuple2 { _1 = False, _2 = 0 }")
      val cullingStartState = unsafeSValueFromLf(
        "Types:Tuple2 { _1 = Nil @(ContractId Cats:Cat), _2 = None @(ContractId Cats:Cat) }"
      )
      val breedingTrigger = controllerContext.spawn(
        breedingTriggerFactory.create(breedingStartState, Seq.empty),
        "slowBreedingTrigger",
      )
      val cullingTrigger = controllerContext.spawn(
        cullingTriggerFactory.create(cullingStartState, Seq.empty),
        "cullingTrigger",
      )

      controllerContext.watch(breedingTrigger)
      controllerContext.watch(cullingTrigger)
      controllerContext.spawn(
        TriggerTimer.messageWithFixedDelay(500.milliseconds, 500.milliseconds)(breedingTrigger),
        "timed-slowBreedingTrigger",
      )
      controllerContext.spawn(
        TriggerTimer.messageWithFixedDelay(500.milliseconds, 500.milliseconds)(cullingTrigger),
        "timed-cullingTrigger",
      )

      Behaviors.empty
    }
  }
}

class MultipleTriggerContention extends TriggerMultiProcessSimulation {

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

  override protected val cantonFixtureDebugMode: Boolean = true

  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit val applicationId: ApiTypes.ApplicationId = this.applicationId

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
        TriggerTimer.messageWithFixedDelay(1.second, 1.second)(breedingTrigger),
        "timed-slowBreedingTrigger",
      )
      controllerContext.spawn(
        TriggerTimer.messageWithFixedDelay(1.second, 1.second)(pettingTrigger),
        "timed-pettingTrigger",
      )
      controllerContext.spawn(
        TriggerTimer.messageWithFixedDelay(1.second, 1.second)(cullingTrigger),
        "timed-cullingTrigger",
      )

      Behaviors.empty
    }
  }
}
