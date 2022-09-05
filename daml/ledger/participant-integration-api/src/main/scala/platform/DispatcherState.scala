// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.DispatcherState.{
  DispatcherNotRunning,
  DispatcherRunning,
  DispatcherStateShutdown,
}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.timer.Timeout._
import io.grpc.StatusRuntimeException

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Life-cycle manager for the Ledger API streams offset dispatcher. */
class DispatcherState(dispatcherShutdownTimeout: Duration)(implicit
    loggingContext: LoggingContext
) {
  private val ServiceName = "Ledger API offset dispatcher"
  private val logger = ContextualizedLogger.get(getClass)
  private var dispatcherStateRef: DispatcherState.State = DispatcherNotRunning

  def isRunning: Boolean = synchronized {
    dispatcherStateRef match {
      case DispatcherRunning(_) => true
      case DispatcherNotRunning | DispatcherStateShutdown => false
    }
  }

  def getDispatcher: Dispatcher[Offset] = synchronized {
    dispatcherStateRef match {
      case DispatcherStateShutdown | DispatcherNotRunning => throw dispatcherNotRunning()
      case DispatcherRunning(dispatcher) => dispatcher
    }
  }

  def startDispatcher(initializationOffset: Offset): Unit = synchronized {
    dispatcherStateRef match {
      case DispatcherNotRunning =>
        val activeDispatcher = buildDispatcher(initializationOffset)
        dispatcherStateRef = DispatcherRunning(activeDispatcher)
        logger.info(
          s"Started a $ServiceName at initialization offset: $initializationOffset."
        )
      case DispatcherStateShutdown =>
        throw new IllegalStateException(s"$ServiceName state has already shut down.")
      case DispatcherRunning(_) =>
        throw new IllegalStateException(
          "Dispatcher startup triggered while an existing dispatcher is still active."
        )
    }
  }

  def stopDispatcher(): Future[Unit] = synchronized {
    logger.info(s"Stopping active $ServiceName.")
    dispatcherStateRef match {
      case DispatcherNotRunning | DispatcherStateShutdown =>
        logger.info(s"$ServiceName already stopped or shutdown.")
        Future.unit
      case DispatcherRunning(dispatcher) =>
        dispatcherStateRef = DispatcherNotRunning
        dispatcher
          .cancel(dispatcherNotRunning())
          .transform {
            case Success(_) =>
              logger.info(s"Active $ServiceName stopped.")
              Success(())
            case f @ Failure(failure) =>
              logger.warn(s"Failed stopping active $ServiceName", failure)
              f
          }(ExecutionContext.parasitic)
    }
  }

  private[platform] def shutdown(): Future[Unit] = synchronized {
    logger.info(s"Shutting down $ServiceName state.")

    val currentDispatcherState = dispatcherStateRef
    dispatcherStateRef = DispatcherStateShutdown

    currentDispatcherState match {
      case DispatcherNotRunning =>
        logger.info(s"$ServiceName not running. Transitioned to shutdown.")
        Future.unit
      case DispatcherStateShutdown =>
        logger.info(s"$ServiceName already shutdown.")
        Future.unit
      case DispatcherRunning(dispatcher) =>
        dispatcher
          .shutdown()
          .withTimeout(dispatcherShutdownTimeout)(
            logger.warn(
              s"Shutdown of existing Ledger API streams did not finish in ${dispatcherShutdownTimeout.toSeconds} seconds."
            )
          )
          .transform {
            case Success(()) =>
              logger.info(s"$ServiceName shutdown.")
              Success(())
            case f @ Failure(failure) =>
              logger.warn(s"Error during $ServiceName shutdown", failure)
              f
          }(ExecutionContext.parasitic)
    }
  }

  private def buildDispatcher(initializationOffset: Offset): Dispatcher[Offset] =
    Dispatcher(
      name = ServiceName,
      zeroIndex = Offset.beforeBegin,
      headAtInitialization = initializationOffset,
    )

  private def dispatcherNotRunning(): StatusRuntimeException = {
    val contextualizedErrorLogger = new DamlContextualizedErrorLogger(
      logger = logger,
      // TODO: Use request contextual LoggingContext once a correlation id can be provided
      loggingContext = loggingContext,
      None,
    )
    LedgerApiErrors.ServiceNotRunning.Reject(ServiceName)(contextualizedErrorLogger).asGrpcError
  }
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
