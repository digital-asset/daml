// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.lf.engine.trigger.TriggerMsg

object AllowFiltering {
  def apply(
      filter: TriggerMsg.Transaction => Boolean
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    Behaviors.setup { _ =>
      Behaviors.receiveMessage {
        case TriggerProcess.MessageWrapper(msg: TriggerMsg.Transaction) if !filter(msg) =>
          Behaviors.same

        case msg =>
          consumer ! msg
          Behaviors.same
      }
    }
  }
}
