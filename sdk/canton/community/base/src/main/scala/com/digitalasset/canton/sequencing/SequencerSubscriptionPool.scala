// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

/** Pool of sequencer subscriptions.
  *
  * The purpose of this pool is to obtain sequencer connections and start subscriptions on them,
  * trying to maintain a number of live subscriptions sufficient to satisfy the trust requirements.
  *
  * More precisely, it strives to maintain at least as many subscriptions as the trust threshold. In
  * order to ensure liveness and not run under the trust threshold as soon as a subscription fails,
  * it maintains a few extra subscriptions, defined as the liveness margin. On the other end, it
  * does not maintain as many subscriptions as possible (i.e. all available connections) in order to
  * limit the network traffic.
  *
  * This pool's health is determined as follows:
  *   - it is healthy if the number of subscriptions is at least the trust threshold + the liveness
  *     margin;
  *   - it is degraded if the number of subscriptions is at least the trust threshold, but there are
  *     fewer extra subscriptions than the liveness margin;
  *   - it is failing if the number of subscriptions is below the trust threshold.
  */
trait SequencerSubscriptionPool extends FlagCloseable with NamedLogging {
  import SequencerSubscriptionPool.*

  def start()(implicit traceContext: TraceContext): Unit

  /** Returns the current configuration of the pool. */
  def config: SequencerSubscriptionPoolConfig

  /** Dynamically update the pool configuration. */
  def updateConfig(newConfig: SequencerSubscriptionPoolConfig)(implicit
      traceContext: TraceContext
  ): Unit

  def health: SequencerSubscriptionPoolHealth

  /** Return the current active subscriptions in the pool. */
  def subscriptions: Set[FakeSequencerSubscription]

  /** Return the number of active subscriptions in the pool. */
  final def nbSubscriptions: NonNegativeInt = NonNegativeInt.tryCreate(subscriptions.size)
}

object SequencerSubscriptionPool {

  /** Subscription pool configuration
    * @param trustThreshold
    *   Minimal number of subscriptions needed to satisfy the trust requirements.
    * @param livenessMargin
    *   Number of extra subscriptions to maintain to ensure liveness.
    * @param connectionRequestDelay
    *   Delay between the attempts to obtain new connections, when the current number of
    *   subscriptions is not [[trustThreshold]] + [[livenessMargin]].
    */
  final case class SequencerSubscriptionPoolConfig(
      trustThreshold: PositiveInt,
      livenessMargin: NonNegativeInt,
      connectionRequestDelay: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofSeconds(1),
  ) {
    lazy val activeThreshold: PositiveInt = trustThreshold + livenessMargin
  }

  class SequencerSubscriptionPoolHealth(
      override val name: String,
      override protected val associatedHasRunOnClosing: HasRunOnClosing,
      override protected val logger: TracedLogger,
  ) extends AtomicHealthComponent {
    override protected val initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState
  }

  /** Currently, a fake sequencer subscription, used as a placeholder before wiring a real one */
  private[sequencing] class FakeSequencerSubscription(val connection: SequencerConnectionX)
      extends AutoCloseable {
    def close(): Unit = ()
  }
}

object SequencerSubscriptionPoolFactory {
  import SequencerSubscriptionPool.SequencerSubscriptionPoolConfig

  def create(
      initialConfig: SequencerSubscriptionPoolConfig,
      pool: SequencerConnectionXPool,
      clock: Clock,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): SequencerSubscriptionPool =
    new SequencerSubscriptionPoolImpl(initialConfig, pool, clock, timeouts, loggerFactory)
}
