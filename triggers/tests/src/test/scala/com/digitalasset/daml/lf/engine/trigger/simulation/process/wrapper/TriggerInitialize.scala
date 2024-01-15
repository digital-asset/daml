// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package wrapper

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.lf.speedy.SValue

object TriggerInitialize {
  def create(
      userState: SValue
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    Behaviors.setup { _ =>
      consumer ! TriggerProcess.Initialize(userState)

      Behaviors.receiveMessage { msg =>
        consumer ! msg
        Behaviors.same
      }
    }
  }
}
