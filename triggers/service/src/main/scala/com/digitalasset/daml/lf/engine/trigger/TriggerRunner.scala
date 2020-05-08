// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.SupervisorStrategy._
import akka.actor.typed.Signal
import akka.actor.typed.PostStop
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.ActorContext
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import com.daml.grpc.adapter.ExecutionSequencerFactory

object TriggerRunner {
  type Config = TriggerRunnerImpl.Config

  trait Message
  final case object Stop extends Message

  def apply(config: Config, name: String)(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): Behavior[TriggerRunner.Message] =
    Behaviors.setup(ctx => new TriggerRunner(ctx, config, name))
}

class TriggerRunner(
    ctx: ActorContext[TriggerRunner.Message],
    config: TriggerRunner.Config,
    name: String)(implicit esf: ExecutionSequencerFactory, mat: Materializer)
    extends AbstractBehavior[TriggerRunner.Message](ctx)
    with StrictLogging {

  import TriggerRunner.{Message, Stop}

  private val child =
    ctx.spawn(Behaviors.supervise(TriggerRunnerImpl(config)).onFailure(restart), name)

  override def onMessage(msg: Message): Behavior[Message] =
    Behaviors.receiveMessagePartial[Message] {
      case Stop =>
        child ! Stop
        Behaviors.stopped
    }

  override def onSignal: PartialFunction[Signal, Behavior[Message]] = {
    case PostStop =>
      logger.info(s"Trigger ${name} stopped")
      this
  }

}
