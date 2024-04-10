// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Traffic}
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.noop.NoOpGauge
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.data.TaskSchedulerMetrics
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.metrics.Metrics as LedgerApiServerMetrics
import com.digitalasset.canton.participant.metrics.PruningMetrics as ParticipantPruningMetrics

import scala.collection.concurrent.TrieMap

class ParticipantMetrics(
    parent: MetricName,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends BaseMetrics {

  override val prefix: MetricName = parent

  override def grpcMetrics: GrpcServerMetrics = ledgerApiServer.grpc
  override def healthMetrics: HealthMetrics = ledgerApiServer.health
  override def storageMetrics: DbStorageMetrics = dbStorage

  private implicit val mc: MetricsContext =
    MetricsContext.Empty // participant -> participant1 is already set in MetricsFactory

  object dbStorage extends DbStorageMetrics(prefix, openTelemetryMetricsFactory)

  val ledgerApiServer: LedgerApiServerMetrics =
    new LedgerApiServerMetrics(prefix, openTelemetryMetricsFactory)

  val httpApiServer: HttpApiMetrics =
    new HttpApiMetrics(openTelemetryMetricsFactory, openTelemetryMetricsFactory)

  private val clients = TrieMap[DomainAlias, SyncDomainMetrics]()

  object pruning extends ParticipantPruningMetrics(prefix, openTelemetryMetricsFactory)

  def domainMetrics(alias: DomainAlias): SyncDomainMetrics = {
    clients.getOrElseUpdate(
      alias,
      new SyncDomainMetrics(prefix, openTelemetryMetricsFactory)(
        mc.withExtraLabels("domain" -> alias.unwrap)
      ),
    )
  }

  @MetricDoc.Tag(
    summary = "Number of updates published through the read service to the indexer",
    description =
      """When an update is published through the read service, it has already been committed to the ledger.
        |The indexer will subsequently store the update in a form that allows for querying the ledger efficiently.""",
    qualification = Debug,
  )
  val updatesPublished: Meter = openTelemetryMetricsFactory.meter(prefix :+ "updates-published")

  @MetricDoc.Tag(
    summary = "Number of requests being validated.",
    description = """Number of requests that are currently being validated.
                    |This also covers requests submitted by other participants.
                    |""",
    qualification = Debug,
    labelsWithDescription = Map(
      "participant" -> "The id of the participant for which the value applies."
    ),
  )
  val dirtyRequests: Gauge[Int] =
    openTelemetryMetricsFactory.gauge(
      prefix :+ "dirty_requests",
      0,
      "Number of requests being validated.",
    )

  @MetricDoc.Tag(
    summary = "Configured maximum number of requests currently being validated.",
    description =
      """Configuration for the maximum number of requests that are currently being validated.
        |This also covers requests submitted by other participants.
        |A negative value means no configuration value was provided and no limit is enforced.
        |""",
    qualification = Debug,
    labelsWithDescription = Map(
      "participant" -> "The id of the participant for which the value applies."
    ),
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val maxDirtyRequestGaugeForDocs: Gauge[Int] =
    NoOpGauge(prefix :+ "max_dirty_requests", 0)

  def registerMaxDirtyRequest(value: () => Option[Int]): Gauge.CloseableGauge =
    openTelemetryMetricsFactory.gaugeWithSupplier(
      prefix :+ "max_dirty_requests",
      () => value().getOrElse(-1),
      """
        |Configuration for the maximum number of requests that are currently being validated.
        | If the value is negative then there is no configuration value and no limit is enforced.""".stripMargin,
    )

}

class SyncDomainMetrics(
    prefix: MetricName,
    factory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {

  object sequencerClient extends SequencerClientMetrics(prefix, factory)

  object conflictDetection extends TaskSchedulerMetrics {

    private val prefix = SyncDomainMetrics.this.prefix :+ "conflict-detection"

    @MetricDoc.Tag(
      summary = "Size of conflict detection sequencer counter queue",
      description =
        """The task scheduler will work off tasks according to the timestamp order, scheduling
          |the tasks whenever a new timestamp has been observed. This metric exposes the number of
          |un-processed sequencer messages that will trigger a timestamp advancement.""",
      qualification = Debug,
    )
    val sequencerCounterQueue: Counter =
      factory.counter(prefix :+ "sequencer-counter-queue")

    @MetricDoc.Tag(
      summary = "Size of conflict detection task queue",
      description = """This metric measures the size of the queue for conflict detection between
                      |concurrent transactions.
                      |A huge number does not necessarily indicate a bottleneck;
                      |it could also mean that a huge number of tasks have not yet arrived at their execution time.""",
      qualification = Debug,
    )
    val taskQueueForDoc: Gauge[Int] = NoOpGauge(prefix :+ "task-queue", 0)
    def taskQueue(size: () => Int): CloseableGauge =
      factory.gauge(prefix :+ "task-queue", 0)

  }

  object transactionProcessing extends TransactionProcessingMetrics(prefix, factory)

  @MetricDoc.Tag(
    summary = "Number of requests being validated on the domain.",
    description = """Number of requests that are currently being validated on the domain.
        |This also covers requests submitted by other participants.
        |""",
    qualification = Debug,
  )
  val numDirtyRequests: Counter = factory.counter(prefix :+ "dirty-requests")

  object recordOrderPublisher extends TaskSchedulerMetrics {

    private val prefix = SyncDomainMetrics.this.prefix :+ "request-tracker"

    @MetricDoc.Tag(
      summary = "Size of record order publisher sequencer counter queue",
      description = """Same as for conflict-detection, but measuring the sequencer counter
                      |queues for the publishing to the ledger api server according to record time.""",
      qualification = Debug,
    )
    val sequencerCounterQueue: Counter =
      factory.counter(prefix :+ "sequencer-counter-queue")

    @MetricDoc.Tag(
      summary = "Size of record order publisher task queue",
      description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                      |exposes the number of tasks that are waiting in the task queue for the right time to pass.""",
      qualification = Debug,
    )
    val taskQueueForDoc: Gauge[Int] = NoOpGauge(prefix :+ "task-queue", 0)
    def taskQueue(size: () => Int): CloseableGauge =
      factory.gaugeWithSupplier(prefix :+ "task-queue", size)
  }

  // TODO(i14580): add testing
  object trafficControl {

    private val prefix = SyncDomainMetrics.this.prefix :+ "traffic-control"

    @MetricDoc.Tag(
      summary = "Current amount of extra traffic remaining",
      description = """Gets updated with every event received.""",
      qualification = Traffic,
    )
    val extraTrafficAvailable: Gauge[Long] =
      factory.gauge(prefix :+ "extra-traffic-credit-available", 0L)

    @MetricDoc.Tag(
      summary = "Records a new top up on the participant",
      description = """Records top up events and the new extra traffic limit associated.""",
      qualification = Traffic,
    )
    val topologyTransaction: Gauge[Long] =
      factory.gauge(prefix :+ "traffic-state-topology-transaction", 0L)

    @MetricDoc.Tag(
      summary = "Event was not delivered because of traffic limit exceeded",
      description = """An event was not delivered because of insufficient traffic credit.""",
      qualification = Traffic,
    )
    val eventAboveTrafficLimit: Meter = factory.meter(prefix :+ "event-above-traffic-limit")

    @MetricDoc.Tag(
      summary = "Event was delivered",
      description = """An event was not delivered.""",
      qualification = Traffic,
    )
    val eventDelivered: Meter = factory.meter(prefix :+ "event-delivered")
  }

}
