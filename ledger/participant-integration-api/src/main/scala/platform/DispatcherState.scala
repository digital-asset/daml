// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.DispatcherState.{
  DispatcherNotInitialized,
  DispatcherOperational,
  DispatcherShutdown,
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
    DispatcherNotInitialized

  def initialized: Boolean = dispatcherStateRef match {
    case _: DispatcherState.DispatcherOperational => true
    case DispatcherState.DispatcherNotInitialized | DispatcherState.DispatcherShutdown => false
  }

  def getDispatcher: Dispatcher[Offset] = dispatcherStateRef match {
    case DispatcherState.DispatcherNotInitialized => throw dispatcherNotInitializedException()
    case DispatcherState.DispatcherOperational(dispatcher) => dispatcher
    case DispatcherState.DispatcherShutdown => throw dispatcherShutdownException()
  }

  def reset(ledgerEnd: LedgerEnd): Future[Unit] = synchronized {
    dispatcherStateRef match {
      case DispatcherState.DispatcherNotInitialized =>
        dispatcherStateRef = DispatcherOperational(buildDispatcher(ledgerEnd))
        Future.unit
      case DispatcherOperational(dispatcher) =>
        dispatcherStateRef = DispatcherOperational(buildDispatcher(ledgerEnd))
        shutdownDispatcher(dispatcher)
      case DispatcherState.DispatcherShutdown =>
        Future.failed(dispatcherShutdownException())
    }
  }

  private[platform] def shutdown(): Future[Unit] = synchronized {
    val currentState = dispatcherStateRef
    dispatcherStateRef = DispatcherShutdown

    currentState match {
      case DispatcherState.DispatcherNotInitialized | DispatcherState.DispatcherShutdown =>
        Future.unit
      case DispatcherOperational(dispatcher) =>
        shutdownDispatcher(dispatcher)
    }
  }

  private def shutdownDispatcher(dispatcher: Dispatcher[Offset]): Future[Unit] =
    dispatcher
      // TODO LLP: Fail sources with exception instead of graceful shutdown
      .shutdown()
      .withTimeout(dispatcherShutdownTimeout)(
        logger.warn(
          s"Shutdown of existing Ledger API streams did not finish in ${dispatcherShutdownTimeout.toSeconds} seconds."
        )
      )

  private def buildDispatcher(ledgerEnd: LedgerEnd): Dispatcher[Offset] =
    Dispatcher(
      name = "ledger-api",
      zeroIndex = Offset.beforeBegin,
      headAtInitialization = ledgerEnd.lastOffset,
    )

  private def dispatcherNotInitializedException() =
    new IllegalStateException("Uninitialized Ledger API offset dispatcher.")

  private def dispatcherShutdownException() =
    new IllegalStateException("Ledger API offset dispatcher has already shut down.")
}

object DispatcherState {
  private sealed trait State extends Product with Serializable

  private final case object DispatcherNotInitialized extends State
  private final case object DispatcherShutdown extends State
  private final case class DispatcherOperational(dispatcher: Dispatcher[Offset]) extends State

  def owner(apiStreamShutdownTimeout: Duration)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[DispatcherState] = ResourceOwner.forReleasable(() =>
    new DispatcherState(apiStreamShutdownTimeout)(loggingContext)
  )(_.shutdown())
}
