// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Traffic}
import com.daml.metrics.api.MetricHandle.{Gauge, Meter}
import com.daml.metrics.api.noop.NoOpGauge
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}
import com.daml.metrics.grpc.{DamlGrpcServerMetrics, GrpcServerMetrics}
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.metrics.MetricHandle.{
  LabeledMetricsFactory,
  MetricsFactory,
  NoOpMetricsFactory,
}
import com.digitalasset.canton.metrics.{DbStorageMetrics, SequencerClientMetrics}
import com.google.common.annotations.VisibleForTesting

import scala.annotation.nowarn

class SequencerMetrics(
    parent: MetricName,
    @nowarn("cat=deprecation")
    val metricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {
  override val prefix: MetricName = MetricName(parent :+ "sequencer")

  override def storageMetrics: DbStorageMetrics = dbStorage

  object blockMetrics extends BlockMetrics(prefix, labeledMetricsFactory)

  object sequencerClient extends SequencerClientMetrics(prefix, metricsFactory)

  @MetricDoc.Tag(
    summary = "Number of active sequencer subscriptions",
    description =
      """This metric indicates the number of active subscriptions currently open and actively
        |served subscriptions at the sequencer.""",
    qualification = Debug,
  )
  val subscriptionsGauge: Gauge[Int] =
    metricsFactory.gauge[Int](MetricName(prefix :+ "subscriptions"), 0)(MetricsContext.Empty)
  @MetricDoc.Tag(
    summary = "Number of messages processed by the sequencer",
    description = """This metric measures the number of successfully validated messages processed
                    |by the sequencer since the start of this process.""",
    qualification = Debug,
  )
  val messagesProcessed: Meter = metricsFactory.meter(prefix :+ "processed")

  @MetricDoc.Tag(
    summary = "Number of message bytes processed by the sequencer",
    description =
      """This metric measures the total number of message bytes processed by the sequencer.
        |If the message received by the sequencer contains duplicate or irrelevant fields,
        |the contents of these fields do not contribute to this metric.""",
    qualification = Debug,
  )
  val bytesProcessed: Meter = metricsFactory.meter(prefix :+ "processed-bytes")

  @MetricDoc.Tag(
    summary = "Number of time requests received by the sequencer",
    description =
      """When a Participant needs to know the domain time it will make a request for a time proof to be sequenced.
        |It would be normal to see a small number of these being sequenced, however if this number becomes a significant
        |portion of the total requests to the sequencer it could indicate that the strategy for requesting times may
        |need to be revised to deal with different clock skews and latencies between the sequencer and participants.""",
    qualification = Debug,
  )
  val timeRequests: Meter = metricsFactory.meter(prefix :+ "time-requests")

  @MetricDoc.Tag(
    summary = "Age of oldest unpruned sequencer event.",
    description =
      """This gauge exposes the age of the oldest, unpruned sequencer event in hours as a way to quantify the
        |pruning backlog.""",
    qualification = Debug,
  )
  val maxEventAge: Gauge[Long] =
    metricsFactory.gauge[Long](MetricName(prefix :+ "max-event-age"), 0L)(MetricsContext.Empty)

  object dbStorage extends DbStorageMetrics(prefix, metricsFactory)

  // TODO(i14580): add testing
  object trafficControl {
    private val prefix = SequencerMetrics.this.prefix :+ "traffic-control"

    @MetricDoc.Tag(
      summary = "Raw size of an event received in the sequencer.",
      description =
        """This the raw payload size of an event, on the write path. Final event cost calculation.""",
      qualification = Traffic,
    )
    val eventReceived: Meter = metricsFactory.meter(prefix :+ "event-received-size")

    @MetricDoc.Tag(
      summary = "Cost of rejected event.",
      description =
        """Cost of an event that was rejected because it exceeded the sender's traffic limit.""",
      qualification = Traffic,
    )
    val eventRejected: Meter = metricsFactory.meter(prefix :+ "event-rejected-cost")

    @MetricDoc.Tag(
      summary = "Cost of delivered event.",
      description = """Cost of an event that was delivered.""",
      qualification = Traffic,
    )
    val eventDelivered: Meter = metricsFactory.meter(prefix :+ "event-delivered-cost")
  }
}

object SequencerMetrics {

  @VisibleForTesting
  def noop(testName: String) = new SequencerMetrics(
    MetricName(testName),
    NoOpMetricsFactory,
    NoOpMetricsFactory,
    new DamlGrpcServerMetrics(NoOpMetricsFactory, "sequencer"),
    new HealthMetrics(NoOpMetricsFactory),
  )

}

class EnvMetrics(
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") factory: MetricsFactory
) {
  def prefix: MetricName = MetricName("env")

  val executionContextQueueSizeName: MetricName = prefix :+ "execution-context" :+ "queue-size"
  @MetricDoc.Tag(
    summary = "Gives the number size of the global execution context queue",
    description = """This execution context is shared across all nodes running on the JVM""",
    qualification = Debug,
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private val executionContextQueueSizeDoc: Gauge[Long] = // For docs only
    NoOpGauge(executionContextQueueSizeName, 0L)

  @nowarn("cat=deprecation")
  def registerExecutionContextQueueSize(f: () => Long): Unit = {
    factory
      .gaugeWithSupplier(
        executionContextQueueSizeName,
        f,
      )(MetricsContext.Empty)
      .discard
  }

}

@MetricDoc.GroupTag(
  representative = "canton.<component>.sequencer-client",
  groupableClass = classOf[SequencerClientMetrics],
)
class DomainMetrics(
    val prefix: MetricName,
    @nowarn("cat=deprecation")
    val metricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  override def storageMetrics: DbStorageMetrics = dbStorage

  object dbStorage extends DbStorageMetrics(prefix, metricsFactory)

  object sequencer
      extends SequencerMetrics(
        prefix,
        metricsFactory,
        labeledMetricsFactory,
        grpcMetrics,
        healthMetrics,
      )

  object mediator extends MediatorMetrics(prefix, metricsFactory)

  object topologyManager extends IdentityManagerMetrics(prefix, metricsFactory)

}

class MediatorNodeMetrics(
    val prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") val metricsFactory: MetricsFactory,
    val grpcMetrics: GrpcServerMetrics,
    val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  override def storageMetrics: DbStorageMetrics = dbStorage

  @nowarn("cat=deprecation")
  object dbStorage extends DbStorageMetrics(prefix, metricsFactory)

  @nowarn("cat=deprecation")
  object mediator extends MediatorMetrics(prefix, metricsFactory)
}

class MediatorMetrics(
    basePrefix: MetricName,
    @nowarn("cat=deprecation") metricsFactory: MetricsFactory,
) {

  val prefix: MetricName = basePrefix :+ "mediator"

  object sequencerClient extends SequencerClientMetrics(prefix, metricsFactory)

  @MetricDoc.Tag(
    summary = "Number of currently outstanding requests",
    description = """This metric provides the number of currently open requests registered
                    |with the mediator.""",
    qualification = Debug,
  )
  val outstanding: Gauge[Int] =
    metricsFactory.gauge(prefix :+ "outstanding-requests", 0)(MetricsContext.Empty)

  @MetricDoc.Tag(
    summary = "Number of totally processed requests",
    description = """This metric provides the number of totally processed requests since the system
                    |has been started.""",
    qualification = Debug,
  )
  val requests: Meter = metricsFactory.meter(prefix :+ "requests")

  @MetricDoc.Tag(
    summary = "Age of oldest unpruned mediator response.",
    description =
      """This gauge exposes the age of the oldest, unpruned mediator response in hours as a way to quantify the
        |pruning backlog.""",
    qualification = Debug,
  )
  val maxEventAge: Gauge[Long] =
    metricsFactory.gauge[Long](MetricName(prefix :+ "max-event-age"), 0L)(MetricsContext.Empty)

  // TODO(i14580): add testing
  object trafficControl {
    @MetricDoc.Tag(
      summary = "Event rejected because of traffic limit exceeded",
      description =
        """This metric is being incremented every time a sequencer rejects an event because
           the sender does not have enough credit.""",
      qualification = Traffic,
    )
    val eventRejected: Meter = metricsFactory.meter(prefix :+ "event-rejected")
  }
}

class IdentityManagerMetrics(
    basePrefix: MetricName,
    @nowarn("cat=deprecation")
    metricsFactory: MetricsFactory,
) {
  val prefix: MetricName = basePrefix :+ "topology-manager"

  object sequencerClient extends SequencerClientMetrics(prefix, metricsFactory)
}
