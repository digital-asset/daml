// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.implicits.catsSyntaxFlatten
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.{FlagCloseable, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClient.{
  SequencerTransportContainer,
  SequencerTransports,
}
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerPassive,
  ApplicationHandlerShutdown,
}
import com.digitalasset.canton.sequencing.client.transports.{
  SequencerClientTransport,
  SequencerClientTransportCommon,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil, SeqUtil}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Random, Success, Try}

trait SequencerTransportLookup {

  /** Returns an arbitrary [[com.digitalasset.canton.sequencing.client.transports.SequencerClientTransportCommon]].
    * Prefers healthy subscriptions to unhealthy ones.
    *
    * @throws java.lang.IllegalStateException if there are currently no
    *                                         [[com.digitalasset.canton.sequencing.client.transports.SequencerClientTransportCommon]]s at all
    */
  def transport(implicit traceContext: TraceContext): SequencerClientTransportCommon

  /** Similar to `transport` except that it returns as many [[com.digitalasset.canton.sequencing.client.transports.SequencerClientTransportCommon]]
    * as currently configured in [[com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports.submissionRequestAmplification]],
    * capped by the number of healthy subscriptions.
    */
  def amplifiedTransports(implicit
      traceContext: TraceContext
  ): NonEmpty[Seq[SequencerClientTransportCommon]]

  /** Returns the transport for the given [[com.digitalasset.canton.topology.SequencerId]].
    *
    * @throws java.lang.IllegalArgumentException if the [[com.digitalasset.canton.topology.SequencerId]] currently has not transport
    */
  // TODO(#13789) remove after having switched over to Pekko everywhere
  def transport(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[SequencerClientTransport]
}

class SequencersTransportState(
    initialSequencerTransports: SequencerTransports[?],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerTransportLookup
    with NamedLogging
    with FlagCloseable {

  private val random: Random = new Random(1L)

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()

  def completion: Future[SequencerClient.CloseReason] = closeReasonPromise.future

  private val lock = new Object()

  private val state = new mutable.HashMap[SequencerId, SequencerTransportState]()

  private val sequencerTrustThreshold =
    new AtomicReference[PositiveInt](initialSequencerTransports.sequencerTrustThreshold)

  def getSequencerTrustThreshold: PositiveInt = sequencerTrustThreshold.get()

  private val submissionRequestAmplification =
    new AtomicReference[PositiveInt](initialSequencerTransports.submissionRequestAmplification)

  def getSubmissionRequestAmplification: PositiveInt = submissionRequestAmplification.get()

  blocking(lock.synchronized {
    val sequencerIdToTransportStateMap = initialSequencerTransports.sequencerIdToTransportMap.map {
      case (sequencerId, transport) =>
        (sequencerId, SequencerTransportState(transport))
    }
    state.addAll(sequencerIdToTransportStateMap).discard
  })

  private def transportState(
      sequencerId: SequencerId
  )(implicit traceContext: TraceContext): UnlessShutdown[SequencerTransportState] =
    performUnlessClosing(functionFullName)(blocking(lock.synchronized {
      state.getOrElse(
        sequencerId,
        ErrorUtil.internalError(
          new IllegalArgumentException(s"sequencerId=$sequencerId is unknown")
        ),
      )
    }))

  private def updateTransport(
      sequencerId: SequencerId,
      updatedTransport: SequencerTransportContainer[?],
  )(implicit traceContext: TraceContext): UnlessShutdown[SequencerTransportState] =
    performUnlessClosing(functionFullName) {
      blocking(lock.synchronized {
        transportState(sequencerId).map { transportStateBefore =>
          state
            .put(sequencerId, transportStateBefore.withTransport(updatedTransport))
            .discard
          transportStateBefore
        }
      })
    }.flatten

  override def transport(implicit traceContext: TraceContext): SequencerClientTransportCommon =
    blocking(lock.synchronized {
      transportInternal(PositiveInt.one).head1
    })

  override def amplifiedTransports(implicit
      traceContext: TraceContext
  ): NonEmpty[Seq[SequencerClientTransportCommon]] =
    blocking(lock.synchronized {
      transportInternal(submissionRequestAmplification.get())
    })

  /** Pick `amount` many random healthy sequencer connections.
    * If are no healthy sequencers, returns a single unhealthy sequencer connection.
    * Must only be called inside a `lock.synchronized` block.
    */
  private[this] def transportInternal(
      amount: PositiveInt
  )(implicit traceContext: TraceContext): NonEmpty[Seq[SequencerClientTransportCommon]] = {
    // We can use a plain Random instance across all threads calling this method,
    // because this method anyway uses locking on its own.
    // (In general, ThreadLocalRandom would void contention on the random number generation, but
    // the plain Random has the advantage that we can hard-code the seed so that the chosen sequencers
    // are easier to reproduce for debugging and tests.)
    val healthySequencers = state.view
      .collect { case (_sequencerId, state) if state.isSubscriptionHealthy => state }
      // Use a `Vector` so that we get fast updates when picking the random subset
      .to(Vector)
    val chosen = SeqUtil
      .randomSubsetShuffle(healthySequencers, amount.unwrap, random)
      .map(_.transport.clientTransport)
    NonEmpty.from(chosen) match {
      case Some(ne) => ne
      case None => NonEmpty(Seq, pickUnhealthySequencer)
    }
  }

  private[this] def pickUnhealthySequencer(implicit
      traceContext: TraceContext
  ): SequencerClientTransportCommon = {
    // TODO(i12377): Can we fallback to first sequencer transport here or should we
    //               introduce EitherT and propagate error handling?
    state.values.headOption
      .getOrElse(
        // TODO(i12377): Error handling
        ErrorUtil.invalidState("No sequencer subscription at the moment. Try again later.")
      )
      .transport
      .clientTransport
  }

  override def transport(sequencerId: SequencerId)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[SequencerClientTransport] =
    transportState(sequencerId).map(_.transport.clientTransport)

  def addSubscription(
      sequencerId: SequencerId,
      subscription: ResilientSequencerSubscription[
        SequencerClientSubscriptionError
      ],
      eventValidator: SequencedEventValidator,
  )(implicit traceContext: TraceContext): Unit =
    performUnlessClosing(functionFullName) {
      blocking(lock.synchronized {
        transportState(sequencerId)
          .map { currentSequencerTransportStateForAlias =>
            if (currentSequencerTransportStateForAlias.subscription.nonEmpty) {
              // there's an existing subscription!
              logger.warn(
                "Cannot create additional subscriptions to the sequencer from the same client"
              )
              sys.error(
                s"The sequencer client already has a running subscription for sequencerAlias=$sequencerId"
              )
            }
            subscription.closeReason.onComplete(closeWithSubscriptionReason(sequencerId))

            state
              .put(
                sequencerId,
                currentSequencerTransportStateForAlias.withSubscription(
                  subscription,
                  eventValidator,
                ),
              )
              .discard
          }
          .onShutdown(())
      })
    }.onShutdown(())

  def changeTransport(
      sequencerTransports: SequencerTransports[?]
  )(implicit traceContext: TraceContext): Future[Unit] = blocking(lock.synchronized {
    sequencerTrustThreshold.set(sequencerTransports.sequencerTrustThreshold)
    submissionRequestAmplification.set(sequencerTransports.submissionRequestAmplification)
    val oldSequencerIds = state.keySet.toSet
    val newSequencerIds = sequencerTransports.sequencerIdToTransportMap.keySet

    val newValues: Set[SequencerId] = newSequencerIds.diff(oldSequencerIds)
    val removedValues: Set[SequencerId] = oldSequencerIds.diff(newSequencerIds)
    val keptValues: Set[SequencerId] = oldSequencerIds.intersect(newSequencerIds)

    if (newValues.nonEmpty || removedValues.nonEmpty) {
      ErrorUtil.internalErrorAsync(
        new IllegalArgumentException(
          "Adding or removing sequencer subscriptions is not supported at the moment"
        )
      )
    } else
      MonadUtil
        .sequentialTraverse_(keptValues.toSeq) { sequencerId =>
          updateTransport(sequencerId, sequencerTransports.sequencerIdToTransportMap(sequencerId))
            .map { transportStateBefore =>
              transportStateBefore.subscription
                .map(_.resilientSequencerSubscription.resubscribeOnTransportChange())
                .getOrElse(Future.unit)
                .thereafter { _ => transportStateBefore.transport.clientTransport.close() }
            }
            .onShutdown(Future.unit)
        }
  })

  private def closeSubscription(
      sequencerId: SequencerId,
      sequencerState: SequencerTransportState,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Closing sequencer subscription $sequencerId...")
    sequencerState.subscription.foreach(_.close())
    sequencerState.transport.clientTransport.close()
    val closeReason = sequencerState.subscription
      .map(_.resilientSequencerSubscription.closeReason)
      .getOrElse(Future.unit)
    logger.trace(s"Wait for the subscription $sequencerId to complete")
    timeouts.shutdownNetwork
      .await_(s"closing resilient sequencer client subscription $sequencerId")(closeReason)
  }

  def closeAllSubscriptions(): Unit = blocking(lock.synchronized {
    import TraceContext.Implicits.Empty.*

    state.toList.foreach { case (sequencerId, subscription) =>
      closeSubscription(sequencerId, subscription)
    }

    closeReasonPromise
      .tryComplete(Success(SequencerClient.CloseReason.ClientShutdown))
      .discard
  })

  private def isEnoughSequencersToOperateWithoutSequencer: Boolean =
    state.size > sequencerTrustThreshold.get().unwrap

  private def closeWithSubscriptionReason(sequencerId: SequencerId)(
      subscriptionCloseReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
  )(implicit traceContext: TraceContext): Unit = {
    // TODO(i12076): Consider aggregating the current situation about other sequencers and
    //               close the sequencer client only in case of not enough healthy sequencers

    val maybeCloseReason: Try[Either[SequencerClient.CloseReason, Unit]] =
      subscriptionCloseReason.map[Either[SequencerClient.CloseReason, Unit]] {
        case SubscriptionCloseReason.HandlerException(ex) =>
          Left(SequencerClient.CloseReason.UnrecoverableException(ex))
        case SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(_reason)) =>
          Left(SequencerClient.CloseReason.BecamePassive)
        case SubscriptionCloseReason.HandlerError(ApplicationHandlerShutdown) =>
          Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.HandlerError(err) =>
          Left(SequencerClient.CloseReason.UnrecoverableError(s"handler returned error: $err"))
        case permissionDenied: SubscriptionCloseReason.PermissionDeniedError =>
          blocking(lock.synchronized {
            if (!isEnoughSequencersToOperateWithoutSequencer)
              Left(SequencerClient.CloseReason.PermissionDenied(s"$permissionDenied"))
            else {
              state.remove(sequencerId).foreach(closeSubscription(sequencerId, _))
              Right(())
            }
          })
        case subscriptionError: SubscriptionCloseReason.SubscriptionError =>
          blocking(lock.synchronized {
            if (!isEnoughSequencersToOperateWithoutSequencer)
              Left(
                SequencerClient.CloseReason.UnrecoverableError(
                  s"subscription implementation failed: $subscriptionError"
                )
              )
            else {
              state.remove(sequencerId).foreach(closeSubscription(sequencerId, _))
              Right(())
            }
          })
        case SubscriptionCloseReason.Closed =>
          blocking(lock.synchronized {
            if (!isEnoughSequencersToOperateWithoutSequencer)
              Left(SequencerClient.CloseReason.ClientShutdown)
            else {
              state.remove(sequencerId).foreach(closeSubscription(sequencerId, _))
              Right(())
            }
          })
        case SubscriptionCloseReason.Shutdown => Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.TransportChange =>
          Right(()) // we don't want to close the sequencer client when changing transport
      }

    def complete(reason: Try[SequencerClient.CloseReason]): Unit =
      closeReasonPromise.tryComplete(reason).discard

    lazy val closeReason: Try[SequencerClient.CloseReason] = maybeCloseReason.collect {
      case Left(error) =>
        error
    }
    maybeCloseReason match {
      case Failure(_) => complete(closeReason)
      case Success(Left(_)) => complete(closeReason)
      case Success(Right(())) =>
    }
  }

  override protected def onClosed(): Unit = {
    closeAllSubscriptions()
  }
}

final case class SequencerTransportState(
    transport: SequencerTransportContainer[?],
    subscription: Option[SequencerTransportState.Subscription] = None,
) {

  def withTransport(
      newTransport: SequencerTransportContainer[?]
  ): SequencerTransportState = {
    require(
      newTransport.sequencerId == transport.sequencerId,
      "SequencerId of the new transport must match",
    )
    copy(transport = newTransport)
  }

  def withSubscription(
      resilientSequencerSubscription: ResilientSequencerSubscription[
        SequencerClientSubscriptionError
      ],
      eventValidator: SequencedEventValidator,
  ): SequencerTransportState = {
    require(
      subscription.isEmpty,
      "Cannot create additional subscriptions to the sequencer from the same client",
    )
    copy(subscription =
      Some(
        SequencerTransportState.Subscription(
          resilientSequencerSubscription,
          eventValidator,
        )
      )
    )
  }

  def isSubscriptionHealthy: Boolean = subscription.exists { subscription =>
    !subscription.resilientSequencerSubscription.isFailed && !subscription.resilientSequencerSubscription.isClosing
  }

}

object SequencerTransportState {
  final case class Subscription(
      resilientSequencerSubscription: ResilientSequencerSubscription[
        SequencerClientSubscriptionError
      ],
      eventValidator: SequencedEventValidator,
  ) {
    def close(): Unit = {
      eventValidator.close()
      resilientSequencerSubscription.close()
    }
  }
}
