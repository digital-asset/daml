// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.daml.lf.engine.trigger.TriggerMsg

import scala.concurrent.duration.FiniteDuration

object AllowHeartbeats {
  def apply(
      duration: FiniteDuration
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    Behaviors.withTimers[TriggerProcess.Message] { timer =>
      timer.startTimerAtFixedRate(TriggerProcess.MessageWrapper(TriggerMsg.Heartbeat), duration)

      Behaviors.receiveMessage { msg =>
        consumer ! msg
        Behaviors.same
      }
    }
  }
}
