// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.flatMap.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SubmissionRequestAmplification
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
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{SequencerAlias, config}
import io.grpc.Status

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

  /** Chooses a sequencer to try next.
    * Currently picks a random healthy sequencer (not chosen so far) or a fixed sequencer if there are no healthy sequencers.
    *
    * @param previous The sequencers the client has already tried to send the submission request to
    * @return The chosen sequencer, its transport, and the configured patience duration to wait before trying again (or None if the amplification factor has been exhausted with the chosen sequencer).
    */
  // TODO(#12377) Be more intelligent about choosing a sequencer
  def nextAmplifiedTransport(previous: Seq[SequencerId])(implicit
      traceContext: TraceContext
  ): (
      SequencerAlias,
      SequencerId,
      SequencerClientTransportCommon,
      Option[config.NonNegativeFiniteDuration],
  )

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
    sequencerTransportSeed: Option[Long],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerTransportLookup
    with NamedLogging
    with FlagCloseable {

  private val random: Random = sequencerTransportSeed.fold(new Random)(new Random(_))

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()

  def completion: Future[SequencerClient.CloseReason] = closeReasonPromise.future

  private val lock = new Object()

  private val states = new mutable.HashMap[SequencerId, SequencerTransportState]()

  private val sequencerTrustThreshold =
    new AtomicReference[PositiveInt](initialSequencerTransports.sequencerTrustThreshold)

  def getSequencerTrustThreshold: PositiveInt = sequencerTrustThreshold.get()

  private val submissionRequestAmplification =
    new AtomicReference[SubmissionRequestAmplification](
      initialSequencerTransports.submissionRequestAmplification
    )

  def getSubmissionRequestAmplification: SubmissionRequestAmplification =
    submissionRequestAmplification.get()

  blocking(lock.synchronized {
    val sequencerIdToTransportStateMap = initialSequencerTransports.sequencerIdToTransportMap.map {
      case (sequencerId, transport) =>
        (sequencerId, SequencerTransportState(transport))
    }
    states.addAll(sequencerIdToTransportStateMap).discard
  })

  private def transportState(
      sequencerId: SequencerId
  )(implicit traceContext: TraceContext): UnlessShutdown[SequencerTransportState] =
    performUnlessClosing(functionFullName)(blocking(lock.synchronized {
      states.getOrElse(
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
          states
            .put(sequencerId, transportStateBefore.withTransport(updatedTransport))
            .discard
          transportStateBefore
        }
      })
    }.flatten

  override def transport(implicit traceContext: TraceContext): SequencerClientTransportCommon =
    blocking(lock.synchronized {
      transportInternal(Set.empty)
    }).clientTransport

  override def nextAmplifiedTransport(previous: Seq[SequencerId])(implicit
      traceContext: TraceContext
  ): (
      SequencerAlias,
      SequencerId,
      SequencerClientTransportCommon,
      Option[config.NonNegativeFiniteDuration],
  ) =
    blocking(lock.synchronized {
      val SubmissionRequestAmplification(factor, patience) = submissionRequestAmplification.get()
      val transportContainer = transportInternal(previous.toSet)
      (
        transportContainer.sequencerAlias,
        transportContainer.sequencerId,
        transportContainer.clientTransport,
        Option.when(previous.sizeIs < factor.value - 1)(patience),
      )
    })

  /** Pick a random healthy sequencer connection, avoiding those in `avoid` if possible.
    * If are no healthy sequencers, returns an unhealthy sequencer connection.
    * Must only be called inside a `lock.synchronized` block.
    */
  private[this] def transportInternal(avoid: Set[SequencerId])(implicit
      traceContext: TraceContext
  ): SequencerTransportContainer[_] = {
    // We can use a plain Random instance across all threads calling this method,
    // because this method anyway uses locking on its own.
    // (In general, ThreadLocalRandom would void contention on the random number generation, but
    // the plain Random has the advantage that we can hard-code the seed so that the chosen sequencers
    // are easier to reproduce for debugging and tests.)
    val healthySequencers = states.view.collect {
      case (_sequencerId, state) if state.isSubscriptionHealthy => state.transport
    }.toVector
    if (healthySequencers.isEmpty) pickUnhealthySequencer
    else {
      val freshHealthySequencers =
        healthySequencers.filterNot(sequencer => avoid.contains(sequencer.sequencerId))
      val sequencersToPickFrom =
        if (freshHealthySequencers.isEmpty) healthySequencers else freshHealthySequencers
      val randomIndex = random.nextInt(sequencersToPickFrom.size)
      sequencersToPickFrom(randomIndex)
    }
  }

  private[this] def pickUnhealthySequencer(implicit
      traceContext: TraceContext
  ): SequencerTransportContainer[_] = {
    // TODO(i12377): Can we fallback to first sequencer transport here or should we
    //               introduce EitherT and propagate error handling?
    val (_, transportState) = states.headOption.getOrElse(
      // TODO(i12377): Error handling
      ErrorUtil.invalidState("No sequencer subscription at the moment. Try again later.")
    )
    transportState.transport
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

            states
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
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val transportCloseFutures = Future.fromTry(Try(blocking(lock.synchronized {
      sequencerTrustThreshold.set(sequencerTransports.sequencerTrustThreshold)
      submissionRequestAmplification.set(sequencerTransports.submissionRequestAmplification)
      val oldSequencerIds = states.keySet.toSet
      val newSequencerIds = sequencerTransports.sequencerIdToTransportMap.keySet

      val newValues: Set[SequencerId] = newSequencerIds.diff(oldSequencerIds)
      val removedValues: Set[SequencerId] = oldSequencerIds.diff(newSequencerIds)
      val keptValues: Set[SequencerId] = oldSequencerIds.intersect(newSequencerIds)

      if (newValues.nonEmpty || removedValues.nonEmpty) {
        ErrorUtil.internalError(
          new IllegalArgumentException(
            "Adding or removing sequencer subscriptions is not supported at the moment"
          )
        )
      } else {
        keptValues.toSeq.map(sequencerId =>
          updateTransport(sequencerId, sequencerTransports.sequencerIdToTransportMap(sequencerId))
            .map { transportStateBefore =>
              transportStateBefore.transport -> transportStateBefore.subscription.map(
                // ResubscribeOnTransportChange synchronously completes the previous subscription
                // and returns the Future of the close reason. Therefore, it is safe to combine
                // the futures outside of the synchronized block
                _.resilientSequencerSubscription.resubscribeOnTransportChange()
              )
            }
        )
      }
    })))

    transportCloseFutures.flatMap(_.parTraverse_(_.parTraverse_ { case (transport, closeFutureO) =>
      closeFutureO
        .getOrElse(Future.unit)
        .thereafter(_ => transport.clientTransport.close())
    }))
  }

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

    states.toList.foreach { case (sequencerId, subscription) =>
      closeSubscription(sequencerId, subscription)
    }

    closeReasonPromise
      .tryComplete(Success(SequencerClient.CloseReason.ClientShutdown))
      .discard
  })

  def logout(): EitherT[FutureUnlessShutdown, Status, Unit] =
    states.values.toSeq.parTraverse_ { transportState =>
      transportState.transport.clientTransport.logout()
    }

  private def isEnoughSequencersToOperateWithoutSequencer: Boolean =
    states.size > sequencerTrustThreshold.get().unwrap

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
              states.remove(sequencerId).foreach(closeSubscription(sequencerId, _))
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
              states.remove(sequencerId).foreach(closeSubscription(sequencerId, _))
              Right(())
            }
          })
        case SubscriptionCloseReason.Closed =>
          blocking(lock.synchronized {
            if (!isEnoughSequencersToOperateWithoutSequencer)
              Left(SequencerClient.CloseReason.ClientShutdown)
            else {
              states.remove(sequencerId).foreach(closeSubscription(sequencerId, _))
              Right(())
            }
          })
        case SubscriptionCloseReason.Shutdown => Left(SequencerClient.CloseReason.ClientShutdown)
        case SubscriptionCloseReason.TransportChange =>
          Right(()) // we don't want to close the sequencer client when changing transport
      }

    def complete(reason: Try[SequencerClient.CloseReason]): Unit = {
      closeReasonPromise.tryComplete(reason).discard
      Lifecycle.close(this)(logger)
    }

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

  override protected def onClosed(): Unit =
    closeAllSubscriptions()
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
