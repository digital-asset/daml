// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package wrapper

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.lf.engine.trigger.TriggerMsg

object TriggerFilter {
  def apply(
      filter: TriggerMsg.Transaction => Boolean
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    Behaviors.setup { _ =>
      Behaviors.receiveMessage {
        case msg @ TriggerProcess.MessageWrapper(transaction: TriggerMsg.Transaction)
            if filter(transaction) =>
          consumer ! msg
          Behaviors.same

        case _ =>
          Behaviors.same
      }
    }
  }
}
