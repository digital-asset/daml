// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.Eval
import com.daml.metrics.api.*
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory}

import scala.collection.concurrent.TrieMap

class SequencerConnectionPoolMetrics(
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {
  val trackedConnections: Gauge[Int] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "tracked-connections",
      summary = "Number of connections tracked by the connection pool",
      description =
        """The configuration of the connection pool defines the parameters of the sequencer connections.
          | This metrics shows the current number of those connections.""".stripMargin,
      qualification = MetricQualification.Saturation,
    ),
    0,
  )

  val trustThreshold: Gauge[Int] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "trust-threshold",
      summary = "Trust threshold configured in the connection pool",
      description =
        """The trust threshold determines how many connections to sequencers must be available and consistent
          | (same synchronizer ID, same protocol version, same static parameters) for the connection pool to
          | initialize.
          | Furthermore, it also determines the number of sequencer subscriptions that must deliver identical
          | copies of an event for that event to be accepted and processed by the node.""".stripMargin,
      qualification = MetricQualification.Saturation,
    ),
    0,
  )

  val subscriptionThreshold: Gauge[Int] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "subscription-threshold",
      summary = "Sum of trust threshold and liveness margin configured in the subscription pool",
      description = """The liveness margin determines how many subscriptions on different sequencers are
          |continuously maintained, beyond the minimum number defined by the trust threshold.
          |In other words, the subscription pool will strive to maintain at all times (trust threshold +
          |liveness margin)-many subscriptions active.
          |This provides tolerance to subscriptions falling, enabling the node to continue operating
          |while some sequencers are down.""".stripMargin,
      qualification = MetricQualification.Saturation,
    ),
    0,
  )

  val validatedConnections: Gauge[Int] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "validated-connections",
      summary = "Number of connections validated by the connection pool",
      description =
        """This metric indicates the current number of connections that are up and validated.
          |These connections are available for components of the node that need to communicate
          |with the synchronizer.""".stripMargin,
      qualification = MetricQualification.Saturation,
    ),
    0,
  )

  val activeSubscriptions: Gauge[Int] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "active-subscriptions",
      summary = "Number of active subscriptions in the subscription pool",
      description =
        """This metric indicates the current number of subscriptions that are active.""",
      qualification = MetricQualification.Saturation,
    ),
    0,
  )

  // TODO(i28571): Factor this pattern into a helper class
  // Gauges don't support metrics context per update. So instead create a map with a gauge per context.
  private val connectionHealthMetrics: TrieMap[MetricsContext, Eval[Gauge[Int]]] = TrieMap.empty
  def connectionHealth(mc: MetricsContext): Gauge[Int] = {
    def createConnectionHealthGauge: Gauge[Int] = metricsFactory.gauge(
      MetricInfo(
        prefix :+ "connection-health",
        summary = "Health of a connection",
        description = """Health of a connection (0: fatal, 1: failed, 2: validated)
            |A failed connection is periodically retried for availability.
            |A fatal subscription is considered invalid and will never be retried.""".stripMargin,
        qualification = MetricQualification.Saturation,
      ),
      0,
    )(context.merge(mc))

    // Two concurrent calls with the same context may cause getOrElseUpdate to evaluate the new value expression twice,
    // even though only one of the results will be stored in the map.
    // Eval.later ensures that we actually create only one instance of the gauge in such a case by delaying the creation
    // until the getOrElseUpdate call has finished.
    connectionHealthMetrics.getOrElseUpdate(mc, Eval.later(createConnectionHealthGauge)).value
  }

  // Gauges don't support metrics context per update. So instead create a map with a gauge per context.
  private val subscriptionHealthMetrics: TrieMap[MetricsContext, Eval[Gauge[Int]]] = TrieMap.empty
  def subscriptionHealth(mc: MetricsContext): Gauge[Int] = {
    def createSubscriptionHealthGauge: Gauge[Int] = metricsFactory.gauge(
      MetricInfo(
        prefix :+ "subscription-health",
        summary = "Health of a subscription",
        description = """Health of a subscription (0: down, 1: active)
            |A subscription can be down either because there are no more connections available, or because
            |there are already enough subscriptions active (according to the trust threshold and liveness
            |margin parameters)""".stripMargin,
        qualification = MetricQualification.Saturation,
      ),
      0,
    )(context.merge(mc))

    // Two concurrent calls with the same context may cause getOrElseUpdate to evaluate the new value expression twice,
    // even though only one of the results will be stored in the map.
    // Eval.later ensures that we actually create only one instance of the gauge in such a case by delaying the creation
    // until the getOrElseUpdate call has finished.
    subscriptionHealthMetrics
      .getOrElseUpdate(mc, Eval.later(createSubscriptionHealthGauge))
      .value
  }

  val connectionRequests: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "grpc-requests",
      summary = "Number of gRPC requests sent on this connection",
      description =
        """This metric indicates the number of gRPC requests that have been sent on this connection.""",
      qualification = MetricQualification.Saturation,
    )
  )
}
