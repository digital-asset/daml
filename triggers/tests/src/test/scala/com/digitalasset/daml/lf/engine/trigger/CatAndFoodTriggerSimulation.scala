// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.lf.speedy.SValue
import org.scalacheck.Gen
import scalaz.syntax.tag._

import scala.concurrent.Await

class CatAndFoodTriggerSimulation
    extends TriggerMultiProcessSimulation
    with CatTriggerResourceUsageTestGenerators {

  import TriggerMultiProcessSimulation._

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
      val cat =
        context.spawn(CreateContractProcess.create(catGen(actAs.unwrap), ledger, actAs), "cat")
      val food =
        context.spawn(CreateContractProcess.create(foodGen(actAs.unwrap), ledger, actAs), "food")

      context.watch(ledger)
      context.watch(trigger)
      context.watch(cat)
      context.watch(food)

      super.triggerMultiProcessSimulation
    }
  }

  private def catGen(owner: String): Gen[CreatedEvent] =
    for {
      isin <- Gen.chooseNum(0L, 1000L)
    } yield createCat(owner, isin)

  private def foodGen(owner: String): Gen[CreatedEvent] =
    for {
      isin <- Gen.chooseNum(0L, 1000L)
    } yield createFood(owner, isin)
}
