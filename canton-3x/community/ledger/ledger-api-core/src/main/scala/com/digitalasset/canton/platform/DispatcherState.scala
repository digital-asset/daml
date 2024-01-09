// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.ResourceOwner
import com.daml.timer.Timeout.*
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.DispatcherState.{
  DispatcherNotRunning,
  DispatcherRunning,
  DispatcherStateShutdown,
}
import com.digitalasset.canton.platform.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.StatusRuntimeException

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Success}

/** Life-cycle manager for the Ledger API streams offset dispatcher. */
class DispatcherState(
    dispatcherShutdownTimeout: Duration,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    traceContext: TraceContext
) extends NamedLogging {

  private val ServiceName = "Ledger API offset dispatcher"
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var dispatcherStateRef: DispatcherState.State = DispatcherNotRunning

  private val directEc = DirectExecutionContext(noTracingLogger)

  def isRunning: Boolean = blocking(synchronized {
    dispatcherStateRef match {
      case DispatcherRunning(_) => true
      case DispatcherNotRunning | DispatcherStateShutdown => false
    }
  })

  def getDispatcher: Dispatcher[Offset] = blocking(synchronized {
    dispatcherStateRef match {
      case DispatcherStateShutdown | DispatcherNotRunning => throw dispatcherNotRunning
      case DispatcherRunning(dispatcher) => dispatcher
    }
  })

  def startDispatcher(initializationOffset: Offset): Unit = blocking(synchronized {
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
  })

  def stopDispatcher(): Future[Unit] = blocking(synchronized {

    dispatcherStateRef match {
      case DispatcherNotRunning | DispatcherStateShutdown =>
        logger.debug(s"$ServiceName already stopped, shutdown or never started.")
        Future.unit
      case DispatcherRunning(dispatcher) =>
        logger.info(s"Stopping active $ServiceName.")
        dispatcherStateRef = DispatcherNotRunning
        dispatcher
          .cancel(() => dispatcherNotRunning)
          .transform {
            case Success(_) =>
              logger.debug(s"Active $ServiceName stopped.")
              Success(())
            case f @ Failure(failure) =>
              logger.warn(s"Failed stopping active $ServiceName", failure)
              f
          }(directEc)
    }
  })

  private[platform] def shutdown(): Future[Unit] = blocking(synchronized {
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
          }(directEc)
    }
  })

  private def buildDispatcher(initializationOffset: Offset): Dispatcher[Offset] =
    Dispatcher(
      name = ServiceName,
      zeroIndex = Offset.beforeBegin,
      headAtInitialization = initializationOffset,
    )

  private def dispatcherNotRunning: StatusRuntimeException = {
    val contextualizedErrorLogger = ErrorLoggingContext(
      logger = logger,
      loggerFactory.properties,
      traceContext,
    )
    CommonErrors.ServiceNotRunning.Reject(ServiceName)(contextualizedErrorLogger).asGrpcError
  }
}

object DispatcherState {
  private sealed trait State extends Product with Serializable

  private final case object DispatcherNotRunning extends State
  private final case object DispatcherStateShutdown extends State
  private final case class DispatcherRunning(dispatcher: Dispatcher[Offset]) extends State

  def owner(apiStreamShutdownTimeout: Duration, loggerFactory: NamedLoggerFactory)(implicit
      traceContext: TraceContext
  ): ResourceOwner[DispatcherState] = ResourceOwner.forReleasable(() =>
    new DispatcherState(apiStreamShutdownTimeout, loggerFactory)
  )(_.shutdown())
}
