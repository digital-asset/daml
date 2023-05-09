// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.engine.trigger.simulation.TriggerMultiProcessSimulation.TriggerSimulationConfig
import com.daml.lf.engine.trigger.simulation.process.ledger.{LedgerExternalAction, LedgerProcess}
import com.daml.lf.engine.trigger.simulation.process.TriggerProcessFactory
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import org.scalacheck.Gen
import scalaz.syntax.tag._

import scala.concurrent.Await
import scala.concurrent.duration._

class CatAndFoodTriggerSimulation
    extends TriggerMultiProcessSimulation
    with CatTriggerResourceUsageTestGenerators {

  import AbstractTriggerTest._
  import CatAndFoodTriggerSimulation._

  // For demonstration purposes, we only run the simulation for 30 seconds
  override protected implicit lazy val simulationConfig: TriggerSimulationConfig =
    TriggerSimulationConfig(simulationDuration = 30.seconds)
  // For demonstration purposes, we enable saving Canton logging
  override protected val cantonFixtureDebugMode: Boolean = true

  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    implicit def applicationId: ApiTypes.ApplicationId = config.applicationId
    Behaviors.setup { context =>
      val setup = for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
      } yield (client, party)
      val (client, actAs) = Await.result(setup, simulationConfig.simulationSetupTimeout)
      val ledger = context.spawn(LedgerProcess.create(client), "ledger")
      val triggerFactory: TriggerProcessFactory =
        triggerProcessFactory(client, ledger, "Cats:feedingTrigger", actAs)
      // With a negative start state, Cats:feedingTrigger will have a behaviour that is dependent on Cat and Food contract generators
      val trigger1 = context.spawn(triggerFactory.create(SValue.SInt64(-1)), "trigger1")
      val trigger2 = context.spawn(triggerFactory.create(SValue.SInt64(-1)), "trigger2")
      val workload =
        context.spawn(
          workloadProcess(ledger, actAs)(
            batchSize = 2000,
            maxNumOfCats = 1000L,
            workloadFrequency = 1.second,
            catDelay = 2.seconds,
            foodDelay = 2.seconds,
            jitter = 2.seconds,
          ),
          "workload",
        )
      context.watch(ledger)
      context.watch(trigger1)
      context.watch(trigger2)
      context.watch(workload)

      Behaviors.empty
    }
  }

  def workloadProcess(ledger: ActorRef[LedgerProcess.Message], owner: Party)(
      batchSize: Int,
      maxNumOfCats: Long,
      workloadFrequency: FiniteDuration,
      catDelay: FiniteDuration,
      foodDelay: FiniteDuration,
      jitter: FiniteDuration,
  ): Behavior[WorkloadProcess.Message] = {
    Behaviors.withTimers[WorkloadProcess.Message] { timer =>
      timer.startTimerAtFixedRate(WorkloadProcess.ScheduleWorkload, workloadFrequency)

      Behaviors.receiveMessage {
        case WorkloadProcess.ScheduleWorkload =>
          for (_ <- 1 to batchSize) {
            Gen
              .zip(
                Gen.chooseNum(1L, maxNumOfCats),
                Gen.choose(catDelay - jitter, catDelay + jitter),
                Gen.choose(foodDelay - jitter, foodDelay + jitter),
              )
              .sample
              .foreach { case (isin, catCreateDelay, foodCreateDelay) =>
                timer.startSingleTimer(
                  WorkloadProcess.CreateContract(createCat(owner.unwrap, isin)),
                  catCreateDelay,
                )
                timer.startSingleTimer(
                  WorkloadProcess.CreateContract(createFood(owner.unwrap, isin)),
                  foodCreateDelay,
                )
              }
          }
          Behaviors.same

        case WorkloadProcess.CreateContract(event) =>
          ledger ! LedgerProcess.ExternalAction(LedgerExternalAction.CreateContract(event, owner))
          Behaviors.same
      }
    }
  }
}

object CatAndFoodTriggerSimulation {
  object WorkloadProcess {
    sealed abstract class Message extends Product with Serializable
    case object ScheduleWorkload extends Message
    final case class CreateContract(event: CreatedEvent) extends Message
  }
}
