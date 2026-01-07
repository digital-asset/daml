// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{HealthListener, HealthQuasiComponent}
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.SequencerConnectionXState
import com.digitalasset.canton.sequencing.SequencerSubscriptionPool.{
  SequencerSubscriptionPoolConfig,
  SequencerSubscriptionPoolHealth,
}
import com.digitalasset.canton.sequencing.SequencerSubscriptionPoolImpl.{
  ConfigWithThreshold,
  SubscriptionStartProvider,
}
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason.UnrecoverableError
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerPassive,
  ApplicationHandlerShutdown,
}
import com.digitalasset.canton.sequencing.client.{
  SequencerClient,
  SequencerClientSubscriptionError,
  SubscriptionCloseReason,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, WallClock}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, LoggerUtil, Mutex}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

final class SequencerSubscriptionPoolImpl private[sequencing] (
    private val initialConfig: SequencerSubscriptionPoolConfig,
    sequencerSubscriptionFactory: SequencerSubscriptionXFactory,
    subscriptionHandlerFactory: SubscriptionHandlerXFactory,
    pool: SequencerConnectionXPool,
    member: Member,
    private val initialSubscriptionEventO: Option[ProcessingSerializedEvent],
    subscriptionStartProvider: SubscriptionStartProvider,
    metrics: SequencerConnectionPoolMetrics,
    metricsContext: MetricsContext,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerSubscriptionPool {

  /** Use a wall clock for scheduling restart delays, so that the pool can make progress even in
    * tests that use static time without explicitly advancing the time
    */
  private val wallClock = new WallClock(timeouts, loggerFactory)

  /** Reference to the currently active configuration */
  private val configRef = new AtomicReference[SequencerSubscriptionPoolConfig](initialConfig)

  /** Flag indicating whether the connection pool has been started */
  private val startedRef = new AtomicBoolean(false)

  /** Tracks the current subscriptions */
  private val trackedSubscriptions = mutable.Set[SubscriptionManager]()

  /** Used to synchronize access to the mutable structure [[trackedSubscriptions]] */
  private val lock = new Mutex()

  /** Holds the token for the current [[adjustConnectionsIfNeeded]] request */
  private val currentRequest = new AtomicLong(0)

  override def config: SequencerSubscriptionPoolConfig = configRef.get

  /** Use this instead of [[config]] to obtain a snapshot of all the current configuration
    * parameters at once.
    */
  private def currentConfigWithThreshold: ConfigWithThreshold =
    ConfigWithThreshold(config, pool.config.trustThreshold)

  private implicit def mc: MetricsContext = metricsContext
  metrics.subscriptionThreshold.updateValue(
    currentConfigWithThreshold.activeThreshold.value
  )

  override def updateConfig(
      newConfig: SequencerSubscriptionPoolConfig
  )(implicit traceContext: TraceContext): Unit = {
    configRef.set(newConfig)
    logger.info(s"Configuration updated to: $newConfig")

    metrics.subscriptionThreshold.updateValue(config.livenessMargin.value)

    // We might need new connections
    adjustConnectionsIfNeeded()
  }

  override val health: SequencerSubscriptionPoolHealth = new SequencerSubscriptionPoolHealth(
    name = "sequencer-subscription-pool",
    associatedHasRunOnClosing = this,
    logger = logger,
  )

  override def getSubscriptionsHealthStatus: Seq[HealthQuasiComponent] =
    subscriptions.view.map(_.health).toSeq

  /** Examine the current number of subscriptions in comparison to the configured trust threshold
    * with liveness margin. If we are under, we request additional connections, and if we can't
    * obtain enough, we reschedule the check later after
    * [[SequencerSubscriptionPoolConfig.subscriptionRequestDelay]]. If we are over, we drop some
    * subscriptions.
    */
  private def adjustConnectionsIfNeeded()(implicit traceContext: TraceContext): Unit = {
    // Overflow is harmless, it will just roll over to negative
    val myToken = currentRequest.incrementAndGet()

    def adjustInternal(): Unit =
      lock.exclusive {
        if (!isClosing && currentRequest.get == myToken) {
          val activeThreshold = currentConfigWithThreshold.activeThreshold

          val current = trackedSubscriptions.toSet
          logger.debug(
            s"[token = $myToken] ${current.size} current subscriptions, active threshold = $activeThreshold"
          )

          val nbToRequest = activeThreshold.unwrap - current.size

          PositiveInt.create(nbToRequest) match {
            case Right(nbToRequestAsPosInt) =>
              logger.debug(s"Requesting $nbToRequest additional connection(s)")

              val currentSeqIds = current.map(_.connection.attributes.sequencerId)
              val newConnections =
                pool.getConnections("subscription-pool", nbToRequestAsPosInt, currentSeqIds)
              logger.debug(
                s"Received ${newConnections.size} new connection(s): ${newConnections.map(_.name)}"
              )

              val newSubscriptions = newConnections.flatMap { connection =>
                val sequencerId = connection.attributes.sequencerId
                val subscription = createSubscription(connection)
                for {
                  _ <- subscription
                    .start()
                    .leftMap(error =>
                      logger.warn(s"Failed to start subscription for $sequencerId: $error")
                    )
                    .toOption
                } yield {
                  logger.debug(s"Successfully started subscription for $sequencerId")
                  metrics
                    .subscriptionHealth(mc.withExtraLabels("connection" -> connection.config.name))
                    .updateValue(1)
                  new SubscriptionManager(subscription)
                }
              }

              trackedSubscriptions ++= newSubscriptions

              metrics.activeSubscriptions.updateValue(
                trackedSubscriptions.size
              )

              updateHealth()

              // Note that the following calls to `register` may trigger a reentrant call here: indeed, `register`
              // will register on the connection health, which will immediately trigger a `poke`. If the connection
              // has meanwhile gone bad (state != `Validated`), we will remove the connection from the subscription
              // pool (as this runs in the same thread and the lock is reentrant) and call back here to adjust.
              // Similarly, a subscription that closes immediately could call back here to adjust.
              // This should however not interfere with our processing, and obtain a new connection.
              newSubscriptions.foreach(_.register())

              if (newConnections.sizeIs < nbToRequest) scheduleNext()

            case Left(_) if nbToRequest < 0 =>
              val toRemove = trackedSubscriptions.take(-nbToRequest)
              logger.info(
                s"Dropping ${toRemove.size} extra subscription(s): ${toRemove.map(_.subscription.connection.name).mkString(", ")}"
              )
              removeSubscriptionsFromPool(toRemove.toSeq*)

            case _ => // nbToRequest == 0
              logger.debug("No additional connection needed")
          }

        } else logger.debug(s"Cancelling request with token = $myToken")
      }

    def scheduleNext()(implicit traceContext: TraceContext): Unit =
      if (!isClosing) {
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          {
            val delay = config.subscriptionRequestDelay
            logger.debug(
              s"Scheduling new check in ${LoggerUtil.roundDurationForHumans(delay.toScala)}"
            )
            wallClock.scheduleAfter(_ => adjustInternal(), delay.duration)
          },
          "adjustConnectionsIfNeeded",
        )
      }.discard

    adjustInternal()
  }

  private def createSubscription(connection: SequencerConnectionX)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionX[SequencerClientSubscriptionError] = {
    val preSubscriptionEventO =
      subscriptionStartProvider.getLatestProcessedEventO.orElse(initialSubscriptionEventO)

    sequencerSubscriptionFactory.create(
      connection,
      member,
      preSubscriptionEventO,
      subscriptionHandlerFactory,
      parent = this,
    )
  }

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()

  override def completion: Future[SequencerClient.CloseReason] = closeReasonPromise.future

  // TODO(i28969): Clean up shutdown and notification mechanism
  // When completing the subscription pool with a reason, we generally also update the health of the subscription pool.
  // This is because other places in the code react to one or the other: for example, the sequencer client will watch
  // the promise and close with the associated reason, which will propagate up to the synchronizer connection manager,
  // whereas the liveness health of mediator nodes has the sequencer client's health as a critical dependency.
  // If we are completing due to a lost subscription, it can trigger another fatal condition happening concurrently (e.g.
  // threshold no longer reachable). Only the first reason will complete the promise, but the health change will be
  // arbitrary.
  // Furthermore, since the health of a component can freely change between any state, an arbitrary health change
  // can mean that a Fatal health which should trigger the liveness to go to NOT_SERVING can be missed if the component
  // closes before liveness sees it (as closing overrides the health to Failed).
  // The following attempts to maintain coherency by limiting the changes to the first completion call, and updating the
  // health before completing so that it has time to propagate.
  // This whole shutdown and notification mechanism, partly inherited from the old transports, should definitely be
  // revisited and cleaned up.
  private val completed = new AtomicBoolean(false)
  private def completeWithReason(
      reason: Try[SequencerClient.CloseReason],
      updateHealth: (SequencerSubscriptionPoolHealth, String) => Unit,
  )(implicit traceContext: TraceContext): Unit =
    if (!completed.getAndSet(true)) {
      updateHealth(health, reason.toString)
      logger.debug(s"Completing sequencer subscription pool with reason: $reason")
      closeReasonPromise.tryComplete(reason).discard
    }

  private def closeWithSubscriptionReason(manager: SubscriptionManager)(
      subscriptionCloseReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
  )(implicit traceContext: TraceContext): Unit = {
    def isThresholdStillReachable(ignoreCurrent: Boolean): Boolean = lock.exclusive {
      val ignored: Set[ConnectionX.ConnectionXConfig] =
        if (ignoreCurrent) Set(manager.connection.config) else Set.empty
      val trustThreshold = currentConfigWithThreshold.trustThreshold
      val result = pool.isThresholdStillReachable(trustThreshold, ignored)
      logger.debug(s"isThresholdStillReachable(ignored = $ignored) = $result")
      result
    }

    def complete(
        reason: Try[SequencerClient.CloseReason]
    )(implicit traceContext: TraceContext): Unit = {
      completeWithReason(reason, _.fatalOccurred(_))
      LifeCycle.close(this)(logger)
    }

    subscriptionCloseReason match {
      case Success(SubscriptionCloseReason.TokenExpiration) =>
        removeSubscriptionsFromPool(manager)

      case Success(SubscriptionCloseReason.HandlerException(ex)) =>
        complete(Success(SequencerClient.CloseReason.UnrecoverableException(ex)))

      case Success(SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(_reason))) =>
        complete(Success(SequencerClient.CloseReason.BecamePassive))

      case Success(SubscriptionCloseReason.HandlerError(ApplicationHandlerShutdown)) =>
        complete(Success(SequencerClient.CloseReason.ClientShutdown))

      case Success(SubscriptionCloseReason.HandlerError(err)) =>
        complete(
          Success(SequencerClient.CloseReason.UnrecoverableError(s"handler returned error: $err"))
        )

      case Success(permissionDenied: SubscriptionCloseReason.PermissionDeniedError) =>
        if (!isThresholdStillReachable(ignoreCurrent = true))
          complete(Success(SequencerClient.CloseReason.PermissionDenied(s"$permissionDenied")))

      case Success(subscriptionError: SubscriptionCloseReason.SubscriptionError) =>
        if (!isThresholdStillReachable(ignoreCurrent = true))
          complete(
            Success(
              SequencerClient.CloseReason.UnrecoverableError(
                s"subscription implementation failed: $subscriptionError"
              )
            )
          )

      case Success(SubscriptionCloseReason.Closed) =>
        if (!isThresholdStillReachable(ignoreCurrent = false))
          complete(Success(SequencerClient.CloseReason.ClientShutdown))

      case Success(SubscriptionCloseReason.Shutdown) =>
        complete(Success(SequencerClient.CloseReason.ClientShutdown))

      case Success(SubscriptionCloseReason.TransportChange) =>
        ErrorUtil.invalidState(
          s"Close reason 'TransportChange' cannot happen on a pool connection"
        )

      case Failure(throwable) => complete(Failure(throwable))
    }
  }

  private class SubscriptionManager(
      val subscription: SequencerSubscriptionX[SequencerClientSubscriptionError]
  ) extends NamedLogging
      with AutoCloseable {
    val connection: SequencerConnectionX = subscription.connection

    protected override val loggerFactory: NamedLoggerFactory =
      SequencerSubscriptionPoolImpl.this.loggerFactory
        .append("connection", s"${connection.name}")

    private val connectionListener = new HealthListener {
      override def name: String = s"SequencerSubscriptionPool-${connection.name}"

      override def poke()(implicit traceContext: TraceContext): Unit = {
        val state = connection.health.getState

        state match {
          case SequencerConnectionXState.Validated =>

          case SequencerConnectionXState.Initial | SequencerConnectionXState.Started |
              SequencerConnectionXState.Starting | SequencerConnectionXState.Stopping(_) |
              SequencerConnectionXState.Stopped(_) | SequencerConnectionXState.Fatal(_) =>
            removeSubscriptionsFromPool(SubscriptionManager.this)
        }
      }
    }

    def register()(implicit traceContext: TraceContext): Unit = {
      connection.health.registerOnHealthChange(connectionListener).discard[Boolean]
      subscription.closeReason.onComplete(closeWithSubscriptionReason(this))
    }

    def close(): Unit = {
      // If the connection comes back, the listener will be a different instance,
      // so we need to unregister to prevent the listeners to accumulate
      connection.health.unregisterOnHealthChange(connectionListener).discard[Boolean]
      LifeCycle.close(subscription)(logger)
    }

    override def toString: String = s"SubscriptionManager for $subscription"
  }

  private def updateHealth()(implicit traceContext: TraceContext): Unit = {
    val currentConfig = currentConfigWithThreshold

    trackedSubscriptions.size match {
      case nb if nb >= currentConfig.activeThreshold.unwrap => health.resolveUnhealthy()
      case nb if nb >= currentConfig.trustThreshold.unwrap =>
        health.degradationOccurred(
          s"below liveness margin: $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}," +
            s" liveness margin = ${currentConfig.livenessMargin}"
        )
      case _ if !pool.isThresholdStillReachable(currentConfig.trustThreshold, Set.empty) =>
        val reason = s"Trust threshold ${currentConfig.trustThreshold} is no longer reachable"
        completeWithReason(Success(UnrecoverableError(reason)), _.fatalOccurred(_))
      case nb =>
        health.failureOccurred(
          s"only $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}"
        )
    }
  }

  override def start()(implicit traceContext: TraceContext): Unit =
    if (startedRef.getAndSet(true)) {
      logger.debug("Subscription pool already started -- ignoring")
    } else {
      adjustConnectionsIfNeeded()
    }

  override def onClosed(): Unit = {
    val instances = lock.exclusive(trackedSubscriptions.toSeq)
    LifeCycle.close(instances*)(logger)
    super.onClosed()
  }

  override def subscriptions: Set[SequencerSubscriptionX[SequencerClientSubscriptionError]] =
    lock.exclusive(trackedSubscriptions.toSet).map(_.subscription)

  private def removeSubscriptionsFromPool(managers: SubscriptionManager*)(implicit
      traceContext: TraceContext
  ): Unit =
    lock.exclusive {
      managers.foreach { manager =>
        logger.debug(s"Removing ${manager.connection.name} from the subscription pool")
        if (trackedSubscriptions.remove(manager)) {
          manager.close()
          metrics
            .subscriptionHealth(mc.withExtraLabels("connection" -> manager.connection.config.name))
            .updateValue(0)
        }
      }

      metrics.activeSubscriptions.updateValue(trackedSubscriptions.size)

      updateHealth()

      // Immediately request new connections if needed
      adjustConnectionsIfNeeded()
    }
}

object SequencerSubscriptionPoolImpl {
  private final case class ConfigWithThreshold(
      private val poolConfig: SequencerSubscriptionPoolConfig,
      trustThreshold: PositiveInt,
  ) {
    val livenessMargin: NonNegativeInt = poolConfig.livenessMargin
    val subscriptionRequestDelay: NonNegativeFiniteDuration = poolConfig.subscriptionRequestDelay
    lazy val activeThreshold: PositiveInt = trustThreshold + livenessMargin
  }

  /** Trait for an object that can provide the starting event for a subscription
    */
  trait SubscriptionStartProvider {

    /** Return the latest event that was successfully aggregated and deposited in the inbox/
      */
    def getLatestProcessedEventO: Option[SequencedSerializedEvent]
  }
}

class SequencerSubscriptionPoolFactoryImpl(
    sequencerSubscriptionFactory: SequencerSubscriptionXFactory,
    subscriptionHandlerFactory: SubscriptionHandlerXFactory,
    metrics: SequencerConnectionPoolMetrics,
    metricsContext: MetricsContext,
    timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionPoolFactory
    with NamedLogging {
  import SequencerSubscriptionPool.SequencerSubscriptionPoolConfig

  def create(
      initialConfig: SequencerSubscriptionPoolConfig,
      connectionPool: SequencerConnectionXPool,
      member: Member,
      initialSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionStartProvider: SubscriptionStartProvider,
  )(implicit ec: ExecutionContext): SequencerSubscriptionPool =
    new SequencerSubscriptionPoolImpl(
      initialConfig,
      sequencerSubscriptionFactory,
      subscriptionHandlerFactory,
      connectionPool,
      member,
      initialSubscriptionEventO,
      subscriptionStartProvider,
      metrics,
      metricsContext,
      timeouts,
      loggerFactory,
    )
}
