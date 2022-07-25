// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.DispatcherState.{
  DispatcherNotRunning,
  DispatcherRunning,
  DispatcherStateShutdown,
}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.timer.Timeout._

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Life-cycle manager for the Ledger API streams offset dispatcher. */
class DispatcherState(dispatcherShutdownTimeout: Duration)(implicit
    loggingContext: LoggingContext
) {
  private val logger = ContextualizedLogger.get(getClass)
  @volatile private var dispatcherStateRef: DispatcherState.State =
    DispatcherNotRunning

  def getDispatcher: Dispatcher[Offset] = dispatcherStateRef match {
    case DispatcherNotRunning => throw dispatcherNotRunning()
    case DispatcherStateShutdown => throw dispatcherStateShutdown()
    case DispatcherRunning(dispatcher) => dispatcher
  }

  def startDispatcher(ledgerEnd: LedgerEnd): Unit = synchronized {
    dispatcherStateRef match {
      case DispatcherNotRunning =>
        val activeDispatcher = buildDispatcher(ledgerEnd)
        dispatcherStateRef = DispatcherRunning(activeDispatcher)
      case DispatcherStateShutdown => throw dispatcherStateShutdown()
      case DispatcherRunning(_) =>
        throw new IllegalStateException(
          "Dispatcher startup triggered while an existing dispatcher is still active."
        )
    }
  }

  def stopDispatcher(): Future[Unit] = synchronized {
    dispatcherStateRef match {
      case DispatcherNotRunning | DispatcherStateShutdown => Future.unit
      case DispatcherRunning(dispatcher) =>
        dispatcherStateRef = DispatcherNotRunning
        logger.info("Stopping active Ledger API offset dispatcher.")
        dispatcher
          // TODO LLP: Fail sources with exception instead of graceful shutdown
          .shutdown()
          .withTimeout(dispatcherShutdownTimeout)(
            logger.warn(
              s"Shutdown of existing Ledger API streams did not finish in ${dispatcherShutdownTimeout.toSeconds} seconds."
            )
          )
    }
  }

  private[platform] def shutdown(): Future[Unit] = synchronized {
    logger.info("Shutting down Ledger API offset dispatcher state.")
    val shutdownF = dispatcherStateRef match {
      case DispatcherNotRunning | DispatcherStateShutdown => Future.unit
      case DispatcherRunning(_) => stopDispatcher()
    }

    dispatcherStateRef = DispatcherStateShutdown
    shutdownF
  }

  private def buildDispatcher(ledgerEnd: LedgerEnd): Dispatcher[Offset] =
    Dispatcher(
      name = "ledger-api",
      zeroIndex = Offset.beforeBegin,
      headAtInitialization = ledgerEnd.lastOffset,
    )

  private def dispatcherNotRunning() =
    new IllegalStateException("Ledger API offset dispatcher not running.")

  private def dispatcherStateShutdown() =
    new IllegalStateException("Ledger API offset dispatcher state has already shut down.")
}

object DispatcherState {
  private sealed trait State extends Product with Serializable

  private final case object DispatcherNotRunning extends State
  private final case object DispatcherStateShutdown extends State
  private final case class DispatcherRunning(dispatcher: Dispatcher[Offset]) extends State

  def owner(apiStreamShutdownTimeout: Duration)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[DispatcherState] = ResourceOwner.forReleasable(() =>
    new DispatcherState(apiStreamShutdownTimeout)(loggingContext)
  )(_.shutdown())
}
