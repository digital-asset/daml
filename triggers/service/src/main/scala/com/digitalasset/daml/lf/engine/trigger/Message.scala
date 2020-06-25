package com.daml.lf.engine.trigger

import akka.actor.typed.ActorRef
import akka.http.scaladsl.Http.ServerBinding

sealed trait Message

final case class GetServerBinding(replyTo: ActorRef[ServerBinding]) extends Message

final case class StartFailed(cause: Throwable) extends Message

final case class Started(binding: ServerBinding) extends Message

final case class TriggerStarting(runningTrigger: RunningTrigger) extends Message

final case class TriggerStarted(runningTrigger: RunningTrigger) extends Message

final case class TriggerInitializationFailure(
    runningTrigger: RunningTrigger,
    cause: String
) extends Message

final case class TriggerRuntimeFailure(
    runningTrigger: RunningTrigger,
    cause: String
) extends Message

case object Stop extends Message
