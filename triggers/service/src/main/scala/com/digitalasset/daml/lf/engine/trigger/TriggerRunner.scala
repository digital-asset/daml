// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.SupervisorStrategy.restartWithBackoff
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{
  ActorRef,
  Behavior,
  BehaviorInterceptor,
  PostStop,
  Signal,
  TypedActorContext
}
import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.logging.LoggingContextOf.{label, newLoggingContext}
import com.daml.logging.ContextualizedLogger
import spray.json._

import scala.util.control.Exception.Catcher

class InitializationHalted(s: String) extends Exception(s) {}
class InitializationException(s: String) extends Exception(s) {}
case class UnauthenticatedException(s: String) extends Exception(s) {}

object TriggerRunner {
  private val logger = ContextualizedLogger.get(this.getClass)

  type Config = TriggerRunnerImpl.Config

  sealed trait Message
  final case object Stop extends Message
  final case class Status(replyTo: ActorRef[TriggerStatus]) extends Message
  private final case class Unauthenticated(cause: UnauthenticatedException) extends Message

  sealed trait TriggerStatus
  final case object QueryingACS extends TriggerStatus
  final case object Running extends TriggerStatus
  final case object Stopped extends TriggerStatus

  implicit val triggerStatusFormat: JsonFormat[TriggerStatus] = new JsonFormat[TriggerStatus] {
    override def read(json: JsValue): TriggerStatus = json match {
      case JsString("running") => Running
      case JsString("stopped") => Stopped
      case JsString("querying ACS") => QueryingACS
      case _ => deserializationError(s"TriggerStatus expected")
    }
    override def write(obj: TriggerStatus): JsValue = obj match {
      case QueryingACS => JsString("querying ACS")
      case Running => JsString("running")
      case Stopped => JsString("stopped")
    }
  }

  // TODO[AH] Workaround for https://github.com/akka/akka/issues/29841.
  //   Remove once fixed upstream.
  private class Interceptor(parent: ActorRef[TriggerRunner.Message])
      extends BehaviorInterceptor[TriggerRunnerImpl.Message, TriggerRunnerImpl.Message] {
    private def handleException(ctx: TypedActorContext[TriggerRunnerImpl.Message])
      : Catcher[Behavior[TriggerRunnerImpl.Message]] = {
      case e: InitializationHalted => {
        // This should be a stop supervisor nested under the restart supervisor.
        ctx.asScala.log.info(s"Supervisor saw failure ${e.getMessage} - stopping")
        Behaviors.stopped
      }
      case e: UnauthenticatedException => {
        // This should be a stop supervisor nested under the restart supervisor.
        // The TriggerRunner should receive a ChildFailed signal when watching TriggerRunnerImpl.
        // This cannot be emulated outside the akka-actor-typed implementation, so we use a dedicated message instead.
        ctx.asScala.log.info(s"Supervisor saw failure ${e.getMessage} - stopping")
        parent ! Unauthenticated(e)
        Behaviors.stopped
      }
    }
    override def aroundStart(
        ctx: TypedActorContext[TriggerRunnerImpl.Message],
        target: BehaviorInterceptor.PreStartTarget[TriggerRunnerImpl.Message])
      : Behavior[TriggerRunnerImpl.Message] = {
      try {
        target.start(ctx)
      } catch handleException(ctx)
    }
    override def aroundReceive(
        ctx: TypedActorContext[TriggerRunnerImpl.Message],
        msg: TriggerRunnerImpl.Message,
        target: BehaviorInterceptor.ReceiveTarget[TriggerRunnerImpl.Message])
      : Behavior[TriggerRunnerImpl.Message] = {
      try {
        target(ctx, msg)
      } catch handleException(ctx)
    }
    override def aroundSignal(
        ctx: TypedActorContext[TriggerRunnerImpl.Message],
        signal: Signal,
        target: BehaviorInterceptor.SignalTarget[TriggerRunnerImpl.Message])
      : Behavior[TriggerRunnerImpl.Message] = {
      try {
        target(ctx, signal)
      } catch handleException(ctx)
    }
  }

  def apply(config: Config, name: String)(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): Behavior[TriggerRunner.Message] =
    newLoggingContext(label[Config with Trigger], config.loggingExtension) {
      implicit loggingContext =>
        Behaviors.setup { ctx =>
          // Spawn a trigger runner impl. Supervise it. Stop immediately on
          // initialization halted exceptions, retry any initialization or
          // execution failure exceptions.
          val runner =
            ctx.spawn(
              Behaviors
                .supervise(
                  Behaviors
                    .intercept(() => new Interceptor(ctx.self))(TriggerRunnerImpl(config))
                )
                .onFailure(
                  restartWithBackoff(
                    config.restartConfig.minRestartInterval,
                    config.restartConfig.maxRestartInterval,
                    config.restartConfig.restartIntervalRandomFactor)),
              name
            )
          Behaviors
            .receiveMessagePartial[Message] {
              case Status(replyTo) =>
                // pass through
                runner ! TriggerRunnerImpl.Status(replyTo)
                Behaviors.same
              case Stop =>
                Behaviors.stopped // Automatically stops the child actor if running.
              case Unauthenticated(cause) =>
                logger.warn(
                  s"Trigger was unauthenticated - requesting token refresh: ${cause.getMessage}")
                config.server ! Server.TriggerTokenExpired(
                  config.triggerInstance,
                  config.trigger,
                  config.compiledPackages)
                Behaviors.stopped
            }
            .receiveSignal {
              case (_, PostStop) =>
                logger.info(s"Trigger $name stopped")
                Behaviors.same
            }
        }
    }
}
