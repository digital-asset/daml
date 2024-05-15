// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.functor.*
import cats.syntax.option.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerSubscriptionErrorGroup
import com.digitalasset.canton.health.{CloseableAtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  ApplicationHandlerPassive,
  ApplicationHandlerShutdown,
}
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.HandlerException
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.handlers.{CounterCapture, HasReceivedEvent}
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.topology.{DomainId, Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.{DelayUtil, FutureUtil, LoggerUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerCounter}
import org.apache.pekko.stream.AbruptStageTerminationException

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Attempts to create a resilient [[SequencerSubscription]] for the [[SequencerClient]] by
  * creating underlying subscriptions using the [[com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport]]
  * and then recreating them if they fail with a reason that is deemed retryable.
  * If a subscription is closed or fails with a reason that is not retryable the failure will be passed upstream
  * from this subscription.
  * We determine whether an error is retryable by calling the supplied [[SubscriptionErrorRetryPolicy]].
  * We also will delay recreating subscriptions by an interval determined by the
  * [[com.digitalasset.canton.sequencing.client.SubscriptionRetryDelayRule]].
  * As we have to know where to restart a subscription from when it is recreated
  * we use a [[com.digitalasset.canton.sequencing.handlers.CounterCapture]] handler
  * wrapper to keep track of the last event that was successfully provided by the provided handler, and use this value
  * to restart new subscriptions from.
  * For this subscription [[ResilientSequencerSubscription.start]] must be called for the underlying subscriptions to begin.
  */
class ResilientSequencerSubscription[HandlerError](
    domainId: DomainId,
    startingFrom: SequencerCounter,
    handler: SerializedEventHandler[HandlerError],
    subscriptionFactory: SequencerSubscriptionFactory[HandlerError],
    retryDelayRule: SubscriptionRetryDelayRule,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerSubscription[HandlerError]
    with NamedLogging
    with CloseableAtomicHealthComponent
    with FlagCloseableAsync {
  override val name: String = SequencerClient.healthName
  override val initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
  override def closingState: ComponentHealthState =
    ComponentHealthState.failed("Disconnected from domain")
  private val nextSubscriptionRef =
    new AtomicReference[Option[SequencerSubscription[HandlerError]]](None)
  private val counterCapture = new CounterCapture(startingFrom, loggerFactory)

  /** Start running the resilient sequencer subscription */
  def start(implicit traceContext: TraceContext): Unit = setupNewSubscription()

  /** Start a new subscription to the sequencer.
    * @param delayOnRestart If this subscription fails with an error that can be retried, how long should we wait before starting a new subscription?
    */
  private def setupNewSubscription(
      delayOnRestart: FiniteDuration = retryDelayRule.initialDelay
  )(implicit traceContext: TraceContext): Unit =
    performUnlessClosing(functionFullName) {
      def started(
          hasReceivedEvent: HasReceivedEvent,
          newSubscription: SequencerSubscription[HandlerError],
          retryPolicy: SubscriptionErrorRetryPolicy,
      ): Unit = {
        logger.debug(
          s"The sequencer subscription has been successfully started"
        )

        // register resolution
        FutureUtil.doNotAwait(
          hasReceivedEvent.awaitEvent.map { _ =>
            resolveUnhealthy()
          },
          "has received event failed",
        )

        // setup handling when it is complete
        newSubscription.closeReason onComplete {
          case Success(SubscriptionCloseReason.TransportChange) =>
            // Create a new subscription and reset the retry delay
            // It is the responsibility of the subscription factory to use the changed transport
            setupNewSubscription(retryDelayRule.initialDelay)

          case Success(_: SubscriptionCloseReason.SubscriptionError) if isClosing =>
            giveUp(Success(SubscriptionCloseReason.Shutdown))

          case error @ Success(subscriptionError: SubscriptionCloseReason.SubscriptionError) =>
            val canRetry =
              retryPolicy.retryOnError(subscriptionError, hasReceivedEvent.hasReceivedEvent)
            if (canRetry) {
              // retry subscription. the retry rule logs at an appropriate level for the given error so we just note
              // that we are retrying at debug level here.
              logger.debug(
                s"The sequencer subscription encountered an error and will be restarted: $subscriptionError"
              )
              delayAndRestartSubscription(hasReceivedEvent.hasReceivedEvent, delayOnRestart)
            } else {
              // we decided we shouldn't attempt to restart a subscription after this error
              giveUp(error)
            }

          case Failure(_: AbruptStageTerminationException) if isClosing =>
            giveUp(Success(SubscriptionCloseReason.Shutdown))

          case Failure(exn) =>
            val canRetry = retryPolicy.retryOnException(exn, logger)

            if (canRetry) {
              // retry subscription
              logger.warn(
                s"The sequencer subscription encountered an exception and will be restarted: $exn",
                exn,
              )
              delayAndRestartSubscription(hasReceivedEvent.hasReceivedEvent, delayOnRestart)
            } else {
              // we decided we shouldn't attempt to restart a subscription after this error
              giveUp(Failure(exn))
            }

          case unrecoverableReason =>
            // for all other reasons assume we can't retry and shut ourselves down
            giveUp(unrecoverableReason)

        }
      }

      createSubscription.map((started _).tupled)
    }.map(
      // the inner UnlessShutdown is the signal from the SequencerSubscriptionFactory that it is being shut down.
      // (really it is about the SequencerTransportState being shutdown)
      // if we call this inside within the performUnlessClosing block above, it will call close on this ResilientSequencerSubscription,
      // which in turn will not be able to proceed, because it will wait for that same performUnlessClosing task to complete, which can't happen,
      // because it contains the call to close
      _.onShutdown(giveUp(Success(SubscriptionCloseReason.Shutdown)))
    )
      // the outer UnlessShutdown is about detecting that ResilientSequencerSubscription is being closed
      .onShutdown(())

  private def delayAndRestartSubscription(hasReceivedEvent: Boolean, delay: FiniteDuration)(implicit
      traceContext: TraceContext
  ): Unit = {
    val logMessage = s"Waiting ${LoggerUtil.roundDurationForHumans(delay)} before reconnecting"
    if (delay < retryDelayRule.warnDelayDuration) {
      logger.debug(logMessage)
    } else if (isFailed) {
      logger.info(logMessage)
    } else if (!isClosing) {
      TraceContext.withNewTraceContext { tx =>
        this.failureOccurred(
          LostSequencerSubscription.Warn(SequencerId(domainId))(this.errorLoggingContext(tx))
        )
      }
    }

    // delay and then restart a subscription with an updated delay duration
    // we effectively throwing away the future here so add some logging in case it fails
    FutureUtil.doNotAwait(
      DelayUtil.delay(functionFullName, delay, this) map { _ =>
        val newDelay = retryDelayRule.nextDelay(delay, hasReceivedEvent)
        setupNewSubscription(newDelay)
      },
      "Delaying setup of new sequencer subscription failed",
    )
  }

  private def createSubscription(implicit traceContext: TraceContext): UnlessShutdown[
    (HasReceivedEvent, SequencerSubscription[HandlerError], SubscriptionErrorRetryPolicy)
  ] = {
    // we are subscribing from the last event we've already received (this way we are sure that we
    // successfully resubscribed). the event will subsequently be ignored by the sequencer client.
    // even more, the event will be compared with the previous event received and we'll complain
    // if we observed a fork
    val nextCounter = counterCapture.counter
    val (hasReceivedEvent, wrappedHandler) = HasReceivedEvent(counterCapture(handler))
    logger.debug(s"Starting new sequencer subscription from $nextCounter")

    val subscriptionE = subscriptionFactory.create(nextCounter, wrappedHandler)(traceContext)
    nextSubscriptionRef.set(subscriptionE.map(_._1.some).onShutdown(None))

    subscriptionE.map { case (subscription, retryPolicy) =>
      (hasReceivedEvent, subscription, retryPolicy)
    }
  }

  // stop the current subscription, do not retry, and propagate the failure upstream
  private def giveUp(
      reason: Try[SubscriptionCloseReason[HandlerError]]
  )(implicit tc: TraceContext): Unit = {
    reason match {
      case Success(SubscriptionCloseReason.Closed) =>
        logger.trace("Sequencer subscription is being closed")
      case Success(SubscriptionCloseReason.Shutdown) =>
        logger.info("Sequencer subscription is being closed due to an ongoing shutdown")
      case Success(SubscriptionCloseReason.HandlerError(_: ApplicationHandlerShutdown.type)) =>
        logger.info("Sequencer subscription is being closed due to handler shutdown")
      case Success(SubscriptionCloseReason.HandlerError(exception: ApplicationHandlerException)) =>
        logger.error(
          s"Sequencer subscription is being closed due to handler exception (this indicates a bug): $exception"
        )
        fatalOccurred(exception.toString)
      case Success(SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(reason))) =>
        logger.warn(
          s"Closing resilient sequencer subscription because instance became passive: $reason"
        )
      case Success(Fatal(reason)) if isClosing =>
        logger.info(
          s"Closing resilient sequencer subscription after an error due to an ongoing shutdown: $reason"
        )
      case Success(ex: HandlerException) =>
        logger.error(s"Closing resilient sequencer subscription due to handler exception: $ex")
        fatalOccurred(ex.toString)
      case Success(error) =>
        logger.warn(s"Closing resilient sequencer subscription due to error: $error")
      case Failure(exception) =>
        logger.error(s"Closing resilient sequencer subscription due to exception", exception)
        fatalOccurred(exception.toString)
    }
    closeReasonPromise.tryComplete(reason).discard
    close()
  }

  /** Closes the current subscription with [[SubscriptionCloseReason.TransportChange]] and resubscribes
    * using the `subscriptionFactory`, provided that there is currently a subscription.
    *
    * @return The future completes after the old subscription has been closed.
    */
  def resubscribeOnTransportChange()(implicit traceContext: TraceContext): Future[Unit] = {
    nextSubscriptionRef.get() match {
      case None => Future.unit
      case Some(subscription) =>
        subscription.complete(SubscriptionCloseReason.TransportChange)
        subscription.closeReason.void
    }
  }

  override private[canton] def complete(
      reason: SubscriptionCloseReason[HandlerError]
  )(implicit traceContext: TraceContext): Unit =
    giveUp(Success(reason))

  private def closeSubscription(
      subscription: SequencerSubscription[HandlerError]
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Closing subscription")
    subscription.close()

    val reason = Try(
      timeouts.shutdownNetwork.await("wait for the running sequencer subscription to close")(
        subscription.closeReason
      )
    )

    reason match {
      case Success(reason) =>
        logger.debug(s"Underlying subscription closed with reason: $reason")
      case Failure(ex) =>
        logger.warn(s"Underlying subscription failed to close", ex)
    }

    val _ = closeReasonPromise.tryComplete(reason)
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = withNewTraceContext {
    implicit traceContext =>
      Seq(
        SyncCloseable(
          "underlying-subscription",
          nextSubscriptionRef.get().foreach(closeSubscription),
        ),
        SyncCloseable(
          "close-reason", {
            // ensure that it is always completed even if there is no running subscription
            closeReasonPromise.tryComplete(Success(SubscriptionCloseReason.Closed)).discard[Boolean]
          },
        ),
      )
  }
}

object ResilientSequencerSubscription extends SequencerSubscriptionErrorGroup {
  def apply[E](
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      member: Member,
      getTransport: => UnlessShutdown[SequencerClientTransport],
      handler: SerializedEventHandler[E],
      startingFrom: SequencerCounter,
      initialDelay: FiniteDuration,
      warnDelay: FiniteDuration,
      maxRetryDelay: FiniteDuration,
      timeouts: ProcessingTimeout,
      requiresAuthentication: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): ResilientSequencerSubscription[E] = {
    new ResilientSequencerSubscription[E](
      domainId,
      startingFrom,
      handler,
      createSubscription(member, getTransport, requiresAuthentication, protocolVersion),
      SubscriptionRetryDelayRule(
        initialDelay,
        warnDelay,
        maxRetryDelay,
      ),
      timeouts,
      loggerFactory,
    )
  }

  /** Creates a simpler handler subscription function for the underlying class */
  private def createSubscription[E](
      member: Member,
      getTransport: => UnlessShutdown[SequencerClientTransport],
      requiresAuthentication: Boolean,
      protocolVersion: ProtocolVersion,
  ): SequencerSubscriptionFactory[E] =
    new SequencerSubscriptionFactory[E] {
      override def create(startingCounter: SequencerCounter, handler: SerializedEventHandler[E])(
          implicit traceContext: TraceContext
      ): UnlessShutdown[(SequencerSubscription[E], SubscriptionErrorRetryPolicy)] = {
        val request = SubscriptionRequest(member, startingCounter, protocolVersion)
        getTransport
          .map { transport =>
            val subscription =
              if (requiresAuthentication) transport.subscribe(request, handler)(traceContext)
              else transport.subscribeUnauthenticated(request, handler)(traceContext)
            (subscription, transport.subscriptionRetryPolicy)
          }
      }
    }

  @Explanation(
    """This warning is logged when a sequencer subscription is interrupted. The system will keep on retrying to reconnect indefinitely."""
  )
  @Resolution(
    "Monitor the situation and contact the server operator if the issues does not resolve itself automatically."
  )
  object LostSequencerSubscription
      extends ErrorCode(
        "SEQUENCER_SUBSCRIPTION_LOST",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {

    final case class Warn(sequencer: SequencerId, _logOnCreation: Boolean = true)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"Lost subscription to sequencer ${sequencer.toString}. Will try to recover automatically."
        ) {
      override def logOnCreation: Boolean = _logOnCreation
    }
  }

  @Explanation(
    """This error is logged when a sequencer client determined a ledger fork, where a sequencer node
      |responded with different events for the same timestamp / counter.
      |
      |Whenever a client reconnects to a domain, it will start with the last message received and compare
      |whether that last message matches the one it received previously. If not, it will report with this error.
      |
      |A ledger fork should not happen in normal operation. It can happen if the backups have been taken
      |in a wrong order and e.g. the participant was more advanced than the sequencer.
      |"""
  )
  @Resolution(
    """You can recover by restoring the system with a correctly ordered backup. Please consult the
      |respective sections in the manual."""
  )
  object ForkHappened
      extends ErrorCode(
        "SEQUENCER_FORK_DETECTED",
        ErrorCategory.SystemInternalAssumptionViolated,
      )

}

/** Errors that may occur on the creation of a sequencer subscription
  */
sealed trait SequencerSubscriptionCreationError extends SubscriptionCloseReason.SubscriptionError

/** When a fatal error occurs on the creation of a sequencer subscription, the [[com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription]]
  * will not retry the subscription creation. Instead, the subscription will fail.
  */
final case class Fatal(msg: String) extends SequencerSubscriptionCreationError

trait SequencerSubscriptionFactory[HandlerError] {
  def create(
      startingCounter: SequencerCounter,
      handler: SerializedEventHandler[HandlerError],
  )(implicit
      traceContext: TraceContext
  ): UnlessShutdown[(SequencerSubscription[HandlerError], SubscriptionErrorRetryPolicy)]
}
