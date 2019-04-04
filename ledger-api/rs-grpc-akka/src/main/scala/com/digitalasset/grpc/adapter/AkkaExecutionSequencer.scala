// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.grpc.adapter

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import com.digitalasset.grpc.adapter.RunnableSequencingActor.ShutdownRequest

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext

/**
  * Implements serial execution semantics by forwarding the Runnables it receives to an underlying actor.
  */
class AkkaExecutionSequencer private (private val actorRef: ActorRef)(
    implicit terminationTimeout: Timeout)
    extends ExecutionSequencer {

  override def sequence(runnable: Runnable): Unit = actorRef ! runnable

  override def close(): Unit = {
    closeAsync(DirectExecutionContext)
    ()
  }

  /**
    * Completes Future when all scheduled Runnables that were sequenced so far have been completed,
    * and the Actor was ordered to terminate.
    */
  def closeAsync(implicit ec: ExecutionContext): Future[Done] =
    (actorRef ? ShutdownRequest).mapTo[Done].recover {
      case askTimeoutException: AskTimeoutException if actorIsTerminated(askTimeoutException) =>
        Done
    }

  private def actorIsTerminated(askTimeoutException: AskTimeoutException) = {
    AkkaExecutionSequencer.actorTerminatedRegex.findFirstIn(askTimeoutException.getMessage).nonEmpty
  }
}

object AkkaExecutionSequencer {
  def apply(name: String, terminationTimeout: FiniteDuration)(
      implicit system: ActorSystem): AkkaExecutionSequencer = {
    new AkkaExecutionSequencer(system.actorOf(Props[RunnableSequencingActor], name))(
      Timeout.durationToTimeout(terminationTimeout))
  }

  private val actorTerminatedRegex = """Recipient\[.*]\] had already been terminated.""".r
}

private[grpc] class RunnableSequencingActor extends Actor with ActorLogging {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  override val receive: Receive = {
    case runnable: Runnable =>
      try {
        runnable.run()
      } catch {
        case NonFatal(t) => log.error("Unexpected exception while executing Runnable", t)
      }
    case ShutdownRequest =>
      context.stop(self) // processing of the current message will continue
      sender() ! Done
  }
}

private[grpc] object RunnableSequencingActor {
  case object ShutdownRequest
}
