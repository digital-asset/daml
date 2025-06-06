// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

/** Small state machine to handle potential bursts of validations caused by rapidly flipping states
  * of the underlying connection. If a validation is requested while one is being run, we schedule a
  * new validation to start when the current one completes. Extra requests are collapsed into this
  * single scheduling.
  *
  * {{{
  *                   request                       request
  *  ┌───────────┐   validation   ┌───────────┐    validation    ┌───────────┐
  *  │   IDLE    ├────────────────►VALIDATING ├──────────────────►VALIDATION ├────┐
  *  ├───────────┤                ├───────────┤                  │  PENDING  │    │ request
  *  │           │                │ on entry/ │                  ├───────────┤    │validation
  *  │           │                │   start   │                  │           │    │
  *  │           ◄────────────────┤validation ◄──────────────────┤           ◄────┘
  *  └───────────┘   validation   └───────────┘    validation    └───────────┘
  *                   complete                      complete
  * }}}
  */
class ConnectionValidationLimiter(
    validate: TraceContext => FutureUnlessShutdown[Unit],
    futureSupervisor: FutureSupervisor,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends AutoCloseable
    with NamedLogging {
  import ConnectionValidationLimiter.*

  private val validationState: AtomicReference[ValidationState] =
    new AtomicReference[ValidationState](ValidationState.Idle)

  def maybeValidate()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val newPromise =
      PromiseUnlessShutdown.supervised[Unit]("connection validation", futureSupervisor)

    val newState = validationState.updateAndGet {
      case ValidationState.Idle => ValidationState.Validating(traceContext, newPromise)

      case ValidationState.Validating(_tc, promise) =>
        ValidationState.ValidationPending(traceContext, runningP = promise, scheduledP = newPromise)

      // No need to schedule more than one validation.
      // We keep the same promise but store the new trace context. Essentially, when the scheduled
      // validation runs, it does so on behalf of the last request that was scheduled, but will fulfill
      // all the ones in between as well.
      case ValidationState.ValidationPending(_tc, runningP, scheduledP) =>
        ValidationState.ValidationPending(traceContext, runningP, scheduledP)
    }

    // The future to return is not always `newPromise` because burst requests are collapsed into a single one scheduled
    val future = newState match {
      case ValidationState.Idle =>
        ErrorUtil.invalidState("Cannot be Idle after state change in maybeValidate()")

      case ValidationState.Validating(_tc, runningP) => runningP.futureUS

      case ValidationState.ValidationPending(_tc, _runningP, `newPromise`) =>
        logger.debug(s"Scheduling a new validation to take place once the current one finishes")
        newPromise.futureUS

      case ValidationState.ValidationPending(_tc, _runningP, scheduledP) =>
        logger.debug(s"Superseding scheduled validation with new validation request")
        scheduledP.futureUS
    }

    enterNewState(newState)

    future
  }

  private def enterNewState(
      newState: ValidationState
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Entering state $newState")

    newState match {
      case ValidationState.Validating(tc, promise) =>
        implicit val traceContext: TraceContext = tc

        logger.debug("Starting validation")
        val validationF =
          validate(traceContext).thereafter { result =>
            logger.debug(s"Validation completed with $result")

            // We don't want to run a possible scheduled validation if the current one is shutdown or throws.
            // If the current validation:
            //   - is aborted with a shutdown, we abort the scheduled one (if any) as well
            //   - throws, we throw in the scheduled one (if any) as well with the same throwable.
            result match {
              case Success(UnlessShutdown.Outcome(())) => validationComplete()
              case Success(UnlessShutdown.AbortedDueToShutdown) => abortValidation(None)
              case Failure(t) => abortValidation(Some(t))
            }
          }
        promise.completeWithUS(validationF).discard

      case ValidationState.Idle | ValidationState.ValidationPending(_, _, _) => // No action
    }
  }

  /** Normal completion of a validation
    */
  private def validationComplete()(implicit traceContext: TraceContext): Unit = {
    val newState = validationState.updateAndGet {
      case ValidationState.Idle => ErrorUtil.invalidState("Cannot be Idle while validating")

      case ValidationState.Validating(_, _) => ValidationState.Idle

      case ValidationState.ValidationPending(tc, _currentP, scheduledP) =>
        // Retrieve the trace context and promise for the scheduled validation
        ValidationState.Validating(tc, scheduledP)
    }

    enterNewState(newState) // (tc)
  }

  /** Abnormal completion of a validation
    * @param throwableO
    *   if defined, the exception that the validation threw; if undefined, the validation completed
    *   with a shutdown
    */
  private def abortValidation(
      throwableO: Option[Throwable]
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug("Aborting connection validation")

    validationState.getAndSet(ValidationState.Idle) match {
      case ValidationState.Idle => ErrorUtil.invalidState("Cannot be Idle while validating")

      case ValidationState.Validating(_, _) =>

      case ValidationState.ValidationPending(tc, _currentP, scheduledP) =>
        throwableO match {
          case None =>
            logger.debug("Shutting down scheduled validation")(tc)
            scheduledP.shutdown_()
          case Some(t) =>
            logger.debug("Failing scheduled validation")(tc)
            scheduledP.failure(t)
        }
    }
  }

  private def shutdown()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Shutting down connection validations")

    validationState.getAndSet(ValidationState.Idle) match {
      case ValidationState.Idle =>

      case ValidationState.Validating(_tc, promise) =>
        logger.debug("Shutting down current validation")
        promise.shutdown_()

      case ValidationState.ValidationPending(_tc, currentP, scheduledP) =>
        logger.debug("Shutting down current and scheduled validations")
        currentP.shutdown_()
        scheduledP.shutdown_()
    }
  }

  override def close(): Unit = {
    import TraceContext.Implicits.Empty.*
    shutdown()
  }
}

object ConnectionValidationLimiter {
  private sealed trait ValidationState extends PrettyPrinting with Product with Serializable
  private object ValidationState {
    case object Idle extends ValidationState {
      override protected def pretty: Pretty[Idle.type] = prettyOfObject[Idle.type]
    }

    /** @param traceContext
      *   trace context of the currently running validation
      * @param promise
      *   promise for the currently running validation
      */
    final case class Validating(traceContext: TraceContext, promise: PromiseUnlessShutdown[Unit])
        extends ValidationState {
      override protected def pretty: Pretty[this.type] = prettyOfObject[this.type]
    }

    /** @param traceContext
      *   trace context of the scheduled validation
      * @param runningP
      *   promise for the currently running validation, needed for shutdown
      * @param scheduledP
      *   promise for the scheduled validation
      */
    final case class ValidationPending(
        traceContext: TraceContext,
        runningP: PromiseUnlessShutdown[Unit],
        scheduledP: PromiseUnlessShutdown[Unit],
    ) extends ValidationState {
      override protected def pretty: Pretty[this.type] = prettyOfObject[this.type]
    }
  }
}
