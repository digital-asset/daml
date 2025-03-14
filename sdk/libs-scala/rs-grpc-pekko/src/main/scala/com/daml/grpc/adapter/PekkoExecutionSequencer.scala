// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.grpc.adapter

import org.apache.pekko.Done
import org.apache.pekko.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  ActorSystem,
  ExtendedActorSystem,
  Props,
}
import org.apache.pekko.pattern.{AskTimeoutException, ask}
import org.apache.pekko.util.Timeout
import com.daml.grpc.adapter.RunnableSequencingActor.ShutdownRequest

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Implements serial execution semantics by forwarding the Runnables it receives to an underlying actor.
  */
class PekkoExecutionSequencer private (private val actorRef: ActorRef)(implicit
    terminationTimeout: Timeout
) extends ExecutionSequencer {

  override def sequence(runnable: Runnable): Unit = actorRef ! runnable

  override def close(): Unit = {
    closeAsync(ExecutionContext.parasitic)
    ()
  }

  /** Completes Future when all scheduled Runnables that were sequenced so far have been completed,
    * and the Actor was ordered to terminate.
    */
  def closeAsync(implicit ec: ExecutionContext): Future[Done] =
    (actorRef ? ShutdownRequest).mapTo[Done].recover {
      case askTimeoutException: AskTimeoutException if actorIsTerminated(askTimeoutException) =>
        Done
    }

  private def actorIsTerminated(askTimeoutException: AskTimeoutException) = {
    PekkoExecutionSequencer.actorTerminatedRegex
      .findFirstIn(askTimeoutException.getMessage)
      .nonEmpty
  }
}

object PekkoExecutionSequencer {
  def apply(name: String, terminationTimeout: FiniteDuration)(implicit
      system: ActorSystem
  ): PekkoExecutionSequencer = {
    system match {
      case extendedSystem: ExtendedActorSystem =>
        new PekkoExecutionSequencer(
          extendedSystem.systemActorOf(Props[RunnableSequencingActor](), name)
        )(Timeout.durationToTimeout(terminationTimeout))
      case _ =>
        new PekkoExecutionSequencer(system.actorOf(Props[RunnableSequencingActor](), name))(
          Timeout.durationToTimeout(terminationTimeout)
        )

    }
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
        case NonFatal(t) =>
          log.error(t, s"Unexpected exception while executing Runnable ($runnable): {}", t)
      }
    case ShutdownRequest =>
      context.stop(self) // processing of the current message will continue
      sender() ! Done
  }
}

private[grpc] object RunnableSequencingActor {
  case object ShutdownRequest
}
