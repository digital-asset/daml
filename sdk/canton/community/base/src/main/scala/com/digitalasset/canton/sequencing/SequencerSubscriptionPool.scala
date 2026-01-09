// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.health.{
  AtomicHealthComponent,
  ComponentHealthState,
  HealthQuasiComponent,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.SequencerSubscriptionPool.SequencerSubscriptionPoolConfig
import com.digitalasset.canton.sequencing.SequencerSubscriptionPoolImpl.SubscriptionStartProvider
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientSubscriptionError}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

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

  /** Return the health status of the subscriptions. Only the active subscriptions are reported. */
  def getSubscriptionsHealthStatus: Seq[HealthQuasiComponent]

  /** Return the current active subscriptions in the pool. */
  def subscriptions: Set[SequencerSubscriptionX[SequencerClientSubscriptionError]]

  /** Return the number of active subscriptions in the pool. */
  final def nbSubscriptions: NonNegativeInt = NonNegativeInt.tryCreate(subscriptions.size)

  /** Future that completes when the sequencer subscription pool closes */
  def completion: Future[SequencerClient.CloseReason]
}

object SequencerSubscriptionPool {

  /** Subscription pool configuration
    * @param livenessMargin
    *   Number of extra subscriptions to maintain to ensure liveness.
    * @param subscriptionRequestDelay
    *   Delay between the attempts to obtain new connections, when the current number of
    *   subscriptions is not `trustThreshold` + [[livenessMargin]].
    */
  final case class SequencerSubscriptionPoolConfig(
      livenessMargin: NonNegativeInt,
      subscriptionRequestDelay: NonNegativeFiniteDuration,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[SequencerSubscriptionPoolConfig] = prettyOfClass(
      param("livenessMargin", _.livenessMargin),
      param("subscriptionRequestDelay", _.subscriptionRequestDelay),
    )
  }

  object SequencerSubscriptionPoolConfig {

    /** Create a sequencer subscription pool configuration from the existing format.
      *
      * TODO(i27260): remove when no longer needed
      */
    def fromSequencerTransports(
        sequencerTransports: SequencerTransports[?]
    ): SequencerSubscriptionPoolConfig =
      SequencerSubscriptionPoolConfig(
        livenessMargin = sequencerTransports.sequencerLivenessMargin,
        subscriptionRequestDelay =
          sequencerTransports.sequencerConnectionPoolDelays.subscriptionRequestDelay,
      )
  }

  class SequencerSubscriptionPoolHealth(
      override val name: String,
      override protected val associatedHasRunOnClosing: HasRunOnClosing,
      override protected val logger: TracedLogger,
  ) extends AtomicHealthComponent {
    override protected val initialHealthState: ComponentHealthState =
      ComponentHealthState.NotInitializedState
  }
}

trait SequencerSubscriptionPoolFactory {
  def create(
      initialConfig: SequencerSubscriptionPoolConfig,
      connectionPool: SequencerConnectionXPool,
      member: Member,
      initialSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionStartProvider: SubscriptionStartProvider,
  )(implicit ec: ExecutionContext): SequencerSubscriptionPool
}
