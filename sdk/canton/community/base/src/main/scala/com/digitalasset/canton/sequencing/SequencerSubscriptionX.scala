// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  ApplicationHandlerPassive,
  ApplicationHandlerShutdown,
}
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.HandlerException
import com.digitalasset.canton.sequencing.client.{
  Fatal,
  InternallyCompletedSequencerSubscription,
  SequencedEventValidatorFactory,
  SequencerClientSubscriptionError,
  SubscriptionCloseReason,
}
import com.digitalasset.canton.sequencing.handlers.HasReceivedEvent
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import org.apache.pekko.stream.AbruptStageTerminationException

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** A subscription to a sequencer.
  *
  * @param connection
  *   the underlying connection to the sequencer
  * @param member
  *   the member for whom we request a subscription
  * @param startingTimestampO
  *   timestamp at which to start the subscription; if undefined, we subscribe from the beginning
  * @param handler
  *   handler to process the events received from the subscription
  */
class SequencerSubscriptionX[HandlerError] private[sequencing] (
    val connection: SequencerConnectionX,
    member: Member,
    startingTimestampO: Option[CantonTimestamp],
    handler: SequencedEventHandler[HandlerError],
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends InternallyCompletedSequencerSubscription[HandlerError]
    with NamedLogging {
  private val retryPolicy = connection.subscriptionRetryPolicy

  def start()(implicit traceContext: TraceContext): Either[String, Unit] = {
    val startingTimestampStringO = startingTimestampO
      .map(timestamp => s"the timestamp $timestamp")
      .getOrElse("the beginning")
    logger.info(s"Starting subscription at $startingTimestampStringO")

    val protocolVersion = connection.attributes.staticParameters.protocolVersion
    val request = SubscriptionRequest(member, startingTimestampO, protocolVersion)

    val (hasReceivedEvent, wrappedHandler) = HasReceivedEvent(handler)

    connection
      .subscribe(request, wrappedHandler, timeouts.network.duration)
      .map(newSubscription =>
        newSubscription.closeReason.onComplete {
          case Success(SubscriptionCloseReason.TransportChange) =>
            ErrorUtil
              .invalidState(s"Close reason 'TransportChange' cannot happen on a pool connection")

          case Success(_: SubscriptionCloseReason.SubscriptionError) if isClosing =>
            giveUp(Success(SubscriptionCloseReason.Shutdown))

          case error @ Success(subscriptionError: SubscriptionCloseReason.SubscriptionError) =>
            val canRetry =
              retryPolicy.retryOnError(
                subscriptionError,
                receivedItems = hasReceivedEvent.hasReceivedEvent,
              )
            if (canRetry) {
              logger.debug(s"Closing sequencer subscription due to error: $subscriptionError.")
              restartConnection(connection, subscriptionError.toString)
            } else {
              // Permanently close the connection to this sequencer
              giveUp(error)
            }

          case Failure(_: AbruptStageTerminationException) if isClosing =>
            giveUp(Success(SubscriptionCloseReason.Shutdown))

          case Failure(exn) =>
            val canRetry = retryPolicy.retryOnException(exn, logger)

            if (canRetry) {
              logger.warn(s"Closing sequencer subscription due to exception: $exn.", exn)
              restartConnection(connection, exn.toString)
            } else {
              // Permanently close the connection to this sequencer
              giveUp(Failure(exn))
            }

          case unrecoverableReason =>
            // Permanently close the connection to this sequencer
            giveUp(unrecoverableReason)
        }
      )
  }

  private def restartConnection(connection: SequencerConnectionX, reason: String)(implicit
      traceContext: TraceContext
  ): Unit =
    // Stop the connection non-fatally and let the subscription pool start a new subscription.
    connection.fail(reason)
  // TODO(i27278): Warn after some delay or number of failures?
  // LostSequencerSubscription.Warn(connection.attributes.sequencerId).discard

  // stop the current subscription, do not retry, and propagate the failure upstream
  private def giveUp(
      reason: Try[SubscriptionCloseReason[HandlerError]]
  )(implicit traceContext: TraceContext): Unit = {
    // We need to complete the promise first, otherwise the `fatal()` will result in the close reason being
    // completed with 'Closed'.
    closeReasonPromise.tryComplete(reason).discard

    reason match {
      case Success(SubscriptionCloseReason.Closed) =>
        logger.trace("Closing sequencer subscription")
      // Normal closing of this subscription can be triggered either by:
      // - closing of the subscription pool, which closed all the subscriptions; in this case, the connection pool
      //   will also be closed and will take care of the connections
      // - failure of a connection, and the subscription pool closed the associated subscription; in this case, the
      //   connection will already be closed if need be
      // We therefore don't need to explicitly close the connection.

      case Success(SubscriptionCloseReason.Shutdown) =>
        logger.info("Closing sequencer subscription due to an ongoing shutdown")
      // If we reach here, it is due to a concurrent closing of the subscription (see previous case) and a subscription
      // error. Again, we don't need to explicitly close the connection.

      case Success(SubscriptionCloseReason.HandlerError(_: ApplicationHandlerShutdown.type)) =>
        logger.info("Closing sequencer subscription due to handler shutdown")
        connection.fatal("Subscription handler shutdown")

      case Success(SubscriptionCloseReason.HandlerError(exception: ApplicationHandlerException)) =>
        logger.error(
          s"Permanently closing sequencer subscription due to handler exception (this indicates a bug): $exception"
        )
        connection.fatal(exception.toString)

      case Success(SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(reason))) =>
        logger.info(
          s"Permanently closing sequencer subscription because instance became passive: $reason"
        )
        connection.fatal("Instance became passive")

      case Success(Fatal(reason)) if isClosing =>
        logger.info(
          s"Permanently closing sequencer subscription after an error due to an ongoing shutdown: $reason"
        )
        connection.fatal("Error during shutdown")

      case Success(ex: HandlerException) =>
        logger.error(s"Permanently closing sequencer subscription due to handler exception: $ex")
        connection.fatal(ex.toString)

      case Success(error) =>
        logger.warn(s"Permanently closing sequencer subscription due to error: $error")
        connection.fatal(error.toString)

      case Failure(exception) =>
        logger.error(s"Permanently closing sequencer subscription due to exception", exception)
        connection.fatal(exception.toString)
    }
  }

  override def toString: String = s"Subscription over ${connection.name}"
}

trait SequencerSubscriptionXFactory {
  def create(
      connection: SequencerConnectionX,
      member: Member,
      preSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionHandlerFactory: SubscriptionHandlerXFactory,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscriptionX[SequencerClientSubscriptionError]
}

class SequencerSubscriptionXFactoryImpl(
    eventValidatorFactory: SequencedEventValidatorFactory,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionXFactory {

  override def create(
      connection: SequencerConnectionX,
      member: Member,
      preSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionHandlerFactory: SubscriptionHandlerXFactory,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscriptionX[SequencerClientSubscriptionError] = {
    val loggerWithConnection = loggerFactory.append("connection", connection.name)
    val startingTimestampO = preSubscriptionEventO.map(_.timestamp)

    val eventValidator = eventValidatorFactory.create(loggerWithConnection)

    val sequencerAlias = SequencerAlias.tryCreate(connection.name)
    val sequencerId = connection.attributes.sequencerId

    val subscriptionHandler = subscriptionHandlerFactory.create(
      eventValidator,
      preSubscriptionEventO,
      sequencerAlias,
      sequencerId,
      loggerWithConnection,
    )

    new SequencerSubscriptionX(
      connection,
      member,
      startingTimestampO,
      subscriptionHandler.handleEvent,
      timeouts,
      loggerWithConnection,
    )
  }
}
