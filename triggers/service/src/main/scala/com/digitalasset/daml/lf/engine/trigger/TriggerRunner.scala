// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.typed.SupervisorStrategy._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.logging.LoggingContextOf.{label, newLoggingContext}
import com.daml.logging.{ContextualizedLogger}
import spray.json._

class InitializationHalted(s: String) extends Exception(s) {}
class InitializationException(s: String) extends Exception(s) {}

object TriggerRunner {
  private val logger = ContextualizedLogger.get(this.getClass)

  type Config = TriggerRunnerImpl.Config

  sealed trait Message
  final case object Stop extends Message
  final case class Status(replyTo: ActorRef[TriggerStatus]) extends Message

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
          Behaviors
            .receiveMessagePartial[Message] {
              case Status(replyTo) =>
                // pass through
                runner ! TriggerRunnerImpl.Status(replyTo)
                Behaviors.same
              case Stop =>
                Behaviors.stopped // Automatically stops the child actor if running.
            }
            .receiveSignal {
              case (_, PostStop) =>
                logger.info(s"Trigger $name stopped")
                Behaviors.same
            }
        }
    }
}
