// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.speedy.SValue
import org.scalacheck.Gen
import scalaz.syntax.tag._

import scala.concurrent.Await
import scala.concurrent.duration._

class CatAndFoodTriggerSimulation
    extends TriggerMultiProcessSimulation
    with CatTriggerResourceUsageTestGenerators {

  import TriggerMultiProcessSimulation._
  import CatAndFoodTriggerSimulation._

  override protected def triggerMultiProcessSimulation: Behavior[Unit] = {
    Behaviors.setup { context =>
      val setup = for {
        client <- ledgerClient()
        party <- allocateParty(client)
      } yield (client, Party(party))
      val (client, actAs) = Await.result(setup, simulationConfig.simulationSetupTimeout)
      val ledger = context.spawn(LedgerProcess.create(client, this), "ledger")
      val triggerFactory: TriggerProcessFactory =
        triggerProcessFactory(client, ledger, "Cats:feedingTrigger", actAs)
      // With a negative start state, Cats:feedingTrigger will have a behaviour that is dependent on Cat and Food contract generators
      val trigger = context.spawn(triggerFactory.create(SValue.SInt64(-1)), "trigger")
      val workload =
        context.spawn(
          workloadProcess(ledger, actAs)(
            batchSize = 2000,
            maxNumOfCats = 1000L,
            workloadFrequency = 1.second,
            catDelay = 0.seconds,
            foodDelay = 10.seconds,
            jitter = 5.seconds,
          ),
          "contracts",
        )
      context.watch(ledger)
      context.watch(trigger)
      context.watch(workload)

      super.triggerMultiProcessSimulation
    }
  }

  def workloadProcess(ledger: ActorRef[LedgerProcess.LedgerManagement], owner: Party)(
      batchSize: Int,
      maxNumOfCats: Long,
      workloadFrequency: FiniteDuration,
      catDelay: FiniteDuration,
      foodDelay: FiniteDuration,
      jitter: FiniteDuration,
  ): Behavior[ContractProcess.Message] = {
    Behaviors.withTimers[ContractProcess.Message] { timer =>
      timer.startTimerAtFixedRate(ContractProcess.ScheduleWorkload, workloadFrequency)

      Behaviors.receiveMessage {
        case ContractProcess.ScheduleWorkload =>
          for (_ <- 1 to batchSize) {
            Gen
              .zip(
                Gen.chooseNum(1L, maxNumOfCats),
                Gen.choose(-jitter, jitter),
                Gen.choose(-jitter, jitter),
              )
              .sample
              .foreach { case (isin, catJitter, foodJitter) =>
                timer.startSingleTimer(
                  ContractProcess.CreateContract(createCat(owner.unwrap, isin)),
                  catDelay + catJitter,
                )
                timer.startSingleTimer(
                  ContractProcess.CreateContract(createFood(owner.unwrap, isin)),
                  foodDelay + foodJitter,
                )
              }
          }
          Behaviors.same

        case ContractProcess.CreateContract(event) =>
          ledger ! LedgerProcess.CreateContract(event, owner)
          Behaviors.same
      }
    }
  }
}

object CatAndFoodTriggerSimulation {
  object ContractProcess {
    sealed abstract class Message extends Product with Serializable
    case object ScheduleWorkload extends Message
    final case class CreateContract(event: CreatedEvent) extends Message
  }
}
