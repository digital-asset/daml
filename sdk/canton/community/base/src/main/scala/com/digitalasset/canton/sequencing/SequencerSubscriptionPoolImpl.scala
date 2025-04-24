// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.HealthListener
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.SequencerConnectionXState
import com.digitalasset.canton.sequencing.SequencerSubscriptionPool.{
  FakeSequencerSubscription,
  SequencerSubscriptionPoolConfig,
  SequencerSubscriptionPoolHealth,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, LoggerUtil}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.concurrent.blocking

final class SequencerSubscriptionPoolImpl private[sequencing] (
    initialConfig: SequencerSubscriptionPoolConfig,
    pool: SequencerConnectionXPool,
    clock: Clock,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionPool {

  /** Reference to the currently active configuration */
  private val configRef = new AtomicReference[SequencerSubscriptionPoolConfig](initialConfig)

  /** Flag indicating whether the connection pool has been started */
  private val startedRef = new AtomicBoolean(false)

  /** Tracks the current subscriptions */
  private val trackedSubscriptions = mutable.Set[SubscriptionHandler]()

  /** Used to synchronize access to the mutable structure [[trackedSubscriptions]] */
  private val lock = new Object()

  /** Holds the token for the current [[adjustConnectionsIfNeeded]] request */
  private val currentRequest = new AtomicLong(0)

  override def config: SequencerSubscriptionPoolConfig = configRef.get

  override def updateConfig(
      newConfig: SequencerSubscriptionPoolConfig
  )(implicit traceContext: TraceContext): Unit = {
    configRef.set(newConfig)

    // We might need new connections
    adjustConnectionsIfNeeded()
  }

  override val health: SequencerSubscriptionPoolHealth = new SequencerSubscriptionPoolHealth(
    name = "sequencer-subscription-pool",
    associatedHasRunOnClosing = this,
    logger = logger,
  )

  /** Examine the current number of subscriptions in comparison to the configured trust threshold
    * with liveness margin. If we are under, we request additional connections, and if we can't
    * obtain enough, we reschedule the check later after
    * [[SequencerSubscriptionPoolConfig.connectionRequestDelay]]. If we are over, we drop some
    * subscriptions.
    */
  private def adjustConnectionsIfNeeded()(implicit traceContext: TraceContext): Unit = {
    // Overflow is harmless, it will just roll over to negative
    val myToken = currentRequest.incrementAndGet()

    def adjustInternal(): Unit = blocking {
      lock.synchronized {
        if (!isClosing && currentRequest.get == myToken) {
          val currentConfig = config
          val activeThreshold = currentConfig.activeThreshold

          val current = trackedSubscriptions.toSet
          logger.debug(
            s"[token = $myToken] ${current.size} current subscriptions, active threshold = $activeThreshold"
          )

          val nbToRequest = activeThreshold.unwrap - current.size
          if (nbToRequest > 0) {
            logger.debug(s"Requesting $nbToRequest additional connection(s)")

            val currentSeqIds = current.map(_.connection.attributes.sequencerId)
            val newConnections = pool.getConnections(nbToRequest, currentSeqIds)
            logger.debug(
              s"Received ${newConnections.size} new connection(s): ${newConnections.map(_.name)}"
            )

            val newSubscriptions = newConnections.map { connection =>
              val subscription = new FakeSequencerSubscription(connection)
              new SubscriptionHandler(subscription)
            }

            trackedSubscriptions ++= newSubscriptions
            updateHealth()

            // Note that the following calls to `register` may trigger a reentrant call here: indeed, `register`
            // will register on the connection health, which will immediately trigger a `poke`. If the connection
            // has meanwhile gone bad (state != `Validated`), we will remove the connection from the subscription
            // pool (as this runs in the same thread and the lock is reentrant) and call back here to adjust.
            // This should however not interfere with our processing, and obtain a new connection.
            newSubscriptions.foreach(_.register())

            if (newConnections.sizeIs < nbToRequest) scheduleNext()

          } else if (nbToRequest < 0) {
            val toRemove = trackedSubscriptions.take(-nbToRequest)
            logger.debug(
              s"Dropping ${toRemove.size} subscriptions: ${toRemove.map(_.subscription.connection.name)}"
            )
            removeSubscriptionsFromPool(toRemove.toSeq*)

          } else {
            logger.debug("No additional connection needed")
          }

        } else logger.debug(s"Cancelling request with token = $myToken")
      }
    }

    def scheduleNext()(implicit traceContext: TraceContext): Unit =
      if (!isClosing) {
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          {
            val delay = config.connectionRequestDelay
            logger.debug(
              s"Scheduling new check in ${LoggerUtil.roundDurationForHumans(delay.duration)}"
            )
            clock.scheduleAfter(_ => adjustInternal(), delay.asJava)
          },
          "adjustConnectionsIfNeeded",
        )
      }.discard

    adjustInternal()
  }

  private class SubscriptionHandler(val subscription: FakeSequencerSubscription)
      extends NamedLogging
      with AutoCloseable {
    val connection: SequencerConnectionX = subscription.connection

    protected override val loggerFactory: NamedLoggerFactory =
      SequencerSubscriptionPoolImpl.this.loggerFactory
        .append("connection", s"${connection.config.name}")

    private val connectionListener = new HealthListener {
      override def name: String = s"SequencerSubscriptionPool-${connection.name}"

      override def poke()(implicit traceContext: TraceContext): Unit = {
        val state = connection.health.getState

        state match {
          case SequencerConnectionXState.Validated =>

          case SequencerConnectionXState.Initial | SequencerConnectionXState.Started |
              SequencerConnectionXState.Starting | SequencerConnectionXState.Stopping |
              SequencerConnectionXState.Stopped | SequencerConnectionXState.Fatal =>
            removeSubscriptionsFromPool(SubscriptionHandler.this)
        }
      }
    }

    def register(): Unit =
      connection.health.registerOnHealthChange(connectionListener).discard[Boolean]

    def close(): Unit = {
      // If the connection comes back, the listener will be a different instance,
      // so we need to unregister to prevent the listeners to accumulate
      connection.health.unregisterOnHealthChange(connectionListener).discard[Boolean]
      LifeCycle.close(subscription)(logger)
    }
  }

  private def updateHealth()(implicit traceContext: TraceContext): Unit = {
    val currentConfig = config

    trackedSubscriptions.size match {
      case nb if nb >= currentConfig.activeThreshold.unwrap => health.resolveUnhealthy()
      case nb if nb >= currentConfig.trustThreshold.unwrap =>
        health.degradationOccurred(
          s"below liveness margin: $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}," +
            s" liveness margin = ${currentConfig.livenessMargin}"
        )
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
    val instances = blocking(lock.synchronized(trackedSubscriptions.toSeq))
    LifeCycle.close(instances*)(logger)
    super.onClosed()
  }

  override def subscriptions: Set[FakeSequencerSubscription] = {
    val handlers = blocking {
      lock.synchronized(trackedSubscriptions.toSet)
    }
    handlers.map(_.subscription)
  }

  private def removeSubscriptionsFromPool(handlers: SubscriptionHandler*)(implicit
      traceContext: TraceContext
  ): Unit = blocking {
    lock.synchronized {
      handlers.foreach { handler =>
        logger.debug(s"Removing ${handler.connection.name} from the subscription pool")
        if (trackedSubscriptions.remove(handler)) {
          handler.close()
        }
      }

      updateHealth()

      // Immediately request new connections if needed
      adjustConnectionsIfNeeded()
    }
  }
}
