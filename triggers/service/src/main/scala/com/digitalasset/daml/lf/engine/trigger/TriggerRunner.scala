// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.{ActorRef, Behavior, PostStop, Signal}
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.SupervisorStrategy._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.ActorContext
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.logging.LoggingContextOf
import LoggingContextOf.{label, newLoggingContext}
import com.daml.scalautil.Statement.discard

class InitializationHalted(s: String) extends Exception(s) {}
class InitializationException(s: String) extends Exception(s) {}

object TriggerRunner {
  type Config = TriggerRunnerImpl.Config

  trait Message
  final case object Stop extends Message

  def apply(config: Config, name: String)(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): Behavior[TriggerRunner.Message] =
    newLoggingContext(label[Config], config.loggingExtension) { implicit loggingContext =>
      Behaviors.setup(ctx => new TriggerRunner(ctx, config, name))
    }
}

final class TriggerRunner private (
    ctx: ActorContext[TriggerRunner.Message],
    config: TriggerRunner.Config,
    name: String)(
    implicit esf: ExecutionSequencerFactory,
    mat: Materializer,
    loggingContext: LoggingContextOf[TriggerRunner.Config])
    extends AbstractBehavior[TriggerRunner.Message](ctx)
    with StrictLogging {

  import TriggerRunner.{Message, Stop}

  // Spawn a trigger runner impl. Supervise it. Stop immediately on
  // initialization halted exceptions, retry any initialization or
  // execution failure exceptions.
  discard[ActorRef[Message]] {
    ctx.spawn(
      Behaviors
        .supervise(
          Behaviors
            .supervise(TriggerRunnerImpl(config))
            .onFailure[InitializationHalted](stop)
        )
        .onFailure(
          restartWithBackoff(
            config.restartConfig.minRestartInterval,
            config.restartConfig.maxRestartInterval,
            config.restartConfig.restartIntervalRandomFactor)),
      name
    )
  }

  override def onMessage(msg: Message): Behavior[Message] =
    Behaviors.receiveMessagePartial[Message] {
      case Stop =>
        Behaviors.stopped // Automatically stops the child actor if running.
    }

  override def onSignal: PartialFunction[Signal, Behavior[Message]] = {
    case PostStop =>
      logger.info(s"Trigger $name stopped")
      this
  }

}
