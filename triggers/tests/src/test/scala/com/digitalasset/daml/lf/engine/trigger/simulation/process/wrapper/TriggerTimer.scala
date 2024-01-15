// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.simulation.process
package wrapper

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.daml.lf.engine.trigger.TriggerMsg

import scala.concurrent.duration.FiniteDuration

object TriggerTimer {
  def singleMessage(
      duration: FiniteDuration
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    Behaviors.withTimers[TriggerProcess.Message] { timer =>
      timer.startSingleTimer(TriggerProcess.MessageWrapper(TriggerMsg.Heartbeat), duration)

      Behaviors.receiveMessage { msg =>
        consumer ! msg
        Behaviors.same
      }
    }
  }

  def regularMessage(
      interval: FiniteDuration
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    messageWithFixedDelay(interval, interval)(consumer)
  }

  def messageWithFixedDelay(
      initialDelay: FiniteDuration,
      interval: FiniteDuration,
  )(consumer: ActorRef[TriggerProcess.Message]): Behavior[TriggerProcess.Message] = {
    Behaviors.withTimers[TriggerProcess.Message] { timer =>
      timer.startTimerWithFixedDelay(
        TriggerProcess.MessageWrapper(TriggerMsg.Heartbeat),
        initialDelay,
        interval,
      )

      Behaviors.receiveMessage { msg =>
        consumer ! msg
        Behaviors.same
      }
    }
  }
}
