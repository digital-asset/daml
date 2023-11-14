// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import akka.Done
import akka.stream.scaladsl.Source
import akka.stream.{AbruptStageTerminationException, KillSwitch, Materializer}
import cats.syntax.either.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{FlagCloseable, OnShutdownRunner}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransportAkka
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil.{RetrySourcePolicy, WithKillSwitch}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{AkkaUtil, LoggerUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** Attempts to create a resilient [[SequencerSubscriptionAkka]] for the [[SequencerClient]] by
  * creating underlying subscriptions using the [[SequencerSubscriptionFactoryAkka]]
  * and then recreating them if they fail with a reason that is deemed retryable.
  * If a subscription is closed or fails with a reason that is not retryable the failure will be passed downstream
  * from this subscription.
  * We determine whether an error is retryable by calling the [[SubscriptionErrorRetryPolicy]]
  * of the supplied [[SequencerSubscriptionFactoryAkka]].
  * We also will delay recreating subscriptions by an interval determined by the
  * [[com.digitalasset.canton.sequencing.client.SubscriptionRetryDelayRule]].
  * The recreated subscription starts at the last event received,
  * or at the starting counter that was given initially if no event was received at all.
  *
  * The emitted events stutter whenever the subscription is recreated.
  */
class ResilientSequencerSubscriberAkka[E](
    retryDelayRule: SubscriptionRetryDelayRule,
    subscriptionFactory: SequencerSubscriptionFactoryAkka[E],
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer)
    extends FlagCloseable
    with NamedLogging {
  import ResilientSequencerSubscriberAkka.*

  /** Start running the resilient sequencer subscription from the given counter */
  def subscribeFrom(startingCounter: SequencerCounter)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionAkka[E] = {

    logger.debug(s"Starting resilient sequencer subscription from counter $startingCounter")
    val onShutdownRunner = new OnShutdownRunner.PureOnShutdownRunner(logger)
    val sequencerId = subscriptionFactory.sequencerId
    val health = new ResilientSequencerSubscriptionHealth(
      s"sequencer-subscription-for-$sequencerId-starting-at-$startingCounter",
      sequencerId,
      onShutdownRunner,
      logger,
    )
    val initial =
      RestartSourceConfig(startingCounter, retryDelayRule.initialDelay, health)(traceContext)
    val source = AkkaUtil
      .restartSource("resilient-sequencer-subscription", initial, mkSource, policy)
      // Filter out retried errors
      .filter {
        case WithKillSwitch(Left(triaged)) => !triaged.retryable
        case WithKillSwitch(Right(_)) => true
      }
      .map(_.map(_.leftMap(_.error)))
      .mapMaterializedValue { case (killSwitch, doneF) =>
        implicit val ec: ExecutionContext = materializer.executionContext
        val closedHealthF = doneF.thereafter { _ =>
          // A `restartSource` may be materialized at most once anyway,
          // so it's OK to use a shared OnShutdownRunner and HealthComponent here
          onShutdownRunner.close()
        }
        (killSwitch, closedHealthF)
      }
    SequencerSubscriptionAkka(source, health)
  }

  private val policy: RetrySourcePolicy[
    RestartSourceConfig,
    Either[TriagedError[E], OrdinarySerializedEvent],
  ] = new RetrySourcePolicy[RestartSourceConfig, Either[TriagedError[E], OrdinarySerializedEvent]] {
    override def shouldRetry(
        lastState: RestartSourceConfig,
        lastEmittedElement: Option[Either[TriagedError[E], OrdinarySerializedEvent]],
        lastFailure: Option[Throwable],
    ): Option[(FiniteDuration, RestartSourceConfig)] = {
      implicit val traceContext: TraceContext = lastState.traceContext
      val retryPolicy = subscriptionFactory.retryPolicy
      val hasReceivedEvent = lastEmittedElement.exists {
        case Left(err) => err.hasReceivedElements
        case Right(_) => true
      }
      val canRetry = lastFailure match {
        case None =>
          lastEmittedElement match {
            case Some(Right(_)) => false
            case Some(Left(err)) =>
              val canRetry = err.retryable
              if (!canRetry)
                logger.warn(s"Closing resilient sequencer subscription due to error: ${err.error}")
              canRetry
            case None =>
              logger.info("The sequencer subscription has been terminated by the server.")
              false
          }
        case Some(ex: AbruptStageTerminationException) =>
          logger.debug("Giving up on resilient sequencer subscription due to shutdown", ex)
          false
        case Some(ex) =>
          val canRetry = retryPolicy.retryOnException(ex)
          if (canRetry) {
            logger.warn(
              s"The sequencer subscription encountered an exception and will be restarted",
              ex,
            )
            true
          } else {
            logger.error(
              "Closing resilient sequencer subscription due to exception",
              ex,
            )
            false
          }
      }
      Option.when(canRetry) {
        val newDelay = retryDelayRule.nextDelay(lastState.delay, hasReceivedEvent)
        val logMessage =
          s"Waiting ${LoggerUtil.roundDurationForHumans(newDelay)} before reconnecting"
        if (newDelay < retryDelayRule.warnDelayDuration) {
          logger.debug(logMessage)
        } else if (lastState.health.isFailed) {
          logger.info(logMessage)
        } else {
          val error =
            LostSequencerSubscription.Warn(subscriptionFactory.sequencerId, _logOnCreation = true)
          lastState.health.failureOccurred(error)
        }

        val nextCounter = lastEmittedElement.fold(lastState.startingCounter)(
          _.fold(_.lastSequencerCounter, _.counter)
        )
        lastState.delay -> lastState.copy(startingCounter = nextCounter, delay = newDelay)
      }
    }
  }

  private def mkSource(
      config: RestartSourceConfig
  ): Source[Either[TriagedError[E], OrdinarySerializedEvent], (KillSwitch, Future[Done])] = {
    implicit val traceContext: TraceContext = config.traceContext
    val nextCounter = config.startingCounter
    logger.debug(s"Starting new sequencer subscription from $nextCounter")
    subscriptionFactory
      .create(nextCounter)
      .source
      .statefulMap(() => TriageState(false, nextCounter))(triageError(config.health), _ => None)
  }

  private def triageError(health: ResilientSequencerSubscriptionHealth)(
      state: TriageState,
      elementWithKillSwitch: WithKillSwitch[Either[E, OrdinarySerializedEvent]],
  )(implicit
      traceContext: TraceContext
  ): (TriageState, Either[TriagedError[E], OrdinarySerializedEvent]) = {
    val element = elementWithKillSwitch.unwrap
    val TriageState(hasPreviouslyReceivedEvents, lastSequencerCounter) = state
    val hasReceivedEvents = hasPreviouslyReceivedEvents || element.isRight
    // Resolve to healthy when we get a new element again
    if (!hasPreviouslyReceivedEvents && element.isRight) {
      health.resolveUnhealthy()
    }
    val triaged = element.leftMap { err =>
      val canRetry = subscriptionFactory.retryPolicy.retryOnError(err, hasReceivedEvents)
      TriagedError(canRetry, hasReceivedEvents, lastSequencerCounter, err)
    }
    val currentSequencerCounter = element.fold(_ => lastSequencerCounter, _.counter)
    val newState = TriageState(hasReceivedEvents, currentSequencerCounter)
    (newState, triaged)
  }
}

object ResilientSequencerSubscriberAkka {

  /** @param startingCounter The counter to start the next subscription from
    * @param delay If the next subscription fails with a retryable error,
    *              how long should we wait before starting a new subscription?
    */
  private[ResilientSequencerSubscriberAkka] final case class RestartSourceConfig(
      startingCounter: SequencerCounter,
      delay: FiniteDuration,
      health: ResilientSequencerSubscriptionHealth,
  )(val traceContext: TraceContext)
      extends PrettyPrinting {
    override def pretty: Pretty[RestartSourceConfig.this.type] = prettyOfClass(
      param("starting counter", _.startingCounter)
    )

    def copy(
        startingCounter: SequencerCounter = this.startingCounter,
        delay: FiniteDuration = this.delay,
        health: ResilientSequencerSubscriptionHealth = this.health,
    ): RestartSourceConfig = RestartSourceConfig(startingCounter, delay, health)(traceContext)
  }

  private final case class TriagedError[+E](
      retryable: Boolean,
      hasReceivedElements: Boolean,
      lastSequencerCounter: SequencerCounter,
      error: E,
  )

  def factory[E](
      sequencerID: SequencerId,
      retryDelayRule: SubscriptionRetryDelayRule,
      subscriptionFactory: SequencerSubscriptionFactoryAkka[E],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer
  ): SequencerSubscriptionFactoryAkka[E] = {
    val subscriber = new ResilientSequencerSubscriberAkka[E](
      retryDelayRule,
      subscriptionFactory,
      timeouts,
      loggerFactory,
    )
    new SequencerSubscriptionFactoryAkka[E] {
      override def sequencerId: SequencerId = sequencerID

      override def create(startingCounter: SequencerCounter)(implicit
          traceContext: TraceContext
      ): SequencerSubscriptionAkka[E] = subscriber.subscribeFrom(startingCounter)

      override val retryPolicy: SubscriptionErrorRetryPolicyAkka[E] =
        SubscriptionErrorRetryPolicyAkka.never
    }
  }

  private final case class TriageState(
      hasPreviouslyReceivedEvents: Boolean,
      lastSequencerCounter: SequencerCounter,
  )

  private class ResilientSequencerSubscriptionHealth(
      override val name: String,
      sequencerId: SequencerId,
      override protected val associatedOnShutdownRunner: OnShutdownRunner,
      override protected val logger: TracedLogger,
  ) extends AtomicHealthComponent {
    override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
    override def closingState: ComponentHealthState =
      ComponentHealthState.failed(s"Disconnected from sequencer $sequencerId")
  }
}

trait SequencerSubscriptionFactoryAkka[E] {

  /** The ID of the sequencer this factory creates subscriptions to */
  def sequencerId: SequencerId

  def create(
      startingCounter: SequencerCounter
  )(implicit traceContext: TraceContext): SequencerSubscriptionAkka[E]

  def retryPolicy: SubscriptionErrorRetryPolicyAkka[E]
}

object SequencerSubscriptionFactoryAkka {

  /** Creates a [[SequencerSubscriptionFactoryAkka]] for a [[ResilientSequencerSubscriberAkka]]
    * that uses an underlying gRPC transport.
    * Changes to the underlying gRPC transport are not supported by the [[ResilientSequencerSubscriberAkka]];
    * these can be done via the sequencer aggregator.
    */
  def fromTransport(
      sequencerID: SequencerId,
      transport: SequencerClientTransportAkka,
      requiresAuthentication: Boolean,
      member: Member,
      protocolVersion: ProtocolVersion,
  ): SequencerSubscriptionFactoryAkka[transport.SubscriptionError] =
    new SequencerSubscriptionFactoryAkka[transport.SubscriptionError] {
      override def sequencerId: SequencerId = sequencerID

      override def create(startingCounter: SequencerCounter)(implicit
          traceContext: TraceContext
      ): SequencerSubscriptionAkka[transport.SubscriptionError] = {
        val request = SubscriptionRequest(member, startingCounter, protocolVersion)
        if (requiresAuthentication) transport.subscribe(request)
        else transport.subscribeUnauthenticated(request)
      }

      override val retryPolicy: SubscriptionErrorRetryPolicyAkka[transport.SubscriptionError] =
        transport.subscriptionRetryPolicyAkka
    }
}
