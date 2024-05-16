// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import cats.Eval
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, LabeledMetricsFactory, Meter}
import com.daml.metrics.api.noop.NoOpGauge
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.data.TaskSchedulerMetrics
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.http.metrics.{HttpApiHistograms, HttpApiMetrics}
import com.digitalasset.canton.metrics.HistogramInventory.Item
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.participant.metrics.PruningMetrics as ParticipantPruningMetrics

import scala.collection.concurrent.TrieMap

class ParticipantHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "participant"

  private[metrics] val ledgerApiServer: LedgerApiServerHistograms =
    new LedgerApiServerHistograms(prefix :+ "api")

  private[metrics] val httpApi: HttpApiHistograms =
    new HttpApiHistograms(prefix)

  private[metrics] val dbStorage: DbStorageHistograms =
    new DbStorageHistograms(parent)
  private[metrics] val sequencerClient: SequencerClientHistograms = new SequencerClientHistograms(
    parent
  )
  private[metrics] val syncDomain: SyncDomainHistograms = new SyncDomainHistograms(
    prefix,
    sequencerClient,
  )
  private[metrics] val pruning: PruningHistograms = new PruningHistograms(parent)

  private[metrics] val consolePrefix: MetricName = prefix :+ "console"
  private[metrics] val consoleNodeCount: Item =
    Item(
      consolePrefix :+ "tx-node-count",
      "Number of nodes per transaction histogram, measured using canton console ledger_api.updates.start_measure",
      MetricQualification.Debug,
    )
  private[metrics] val consoleTransactionSize: Item =
    Item(
      consolePrefix :+ "tx-size",
      "Transaction size histogram, measured using canton console ledger_api.updates.start_measure ",
      MetricQualification.Debug,
    )

}

class ParticipantMetrics(
    inventory: ParticipantHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends BaseMetrics {

  private implicit val mc: MetricsContext = MetricsContext.Empty

  override val prefix: MetricName = inventory.prefix

  override def grpcMetrics: GrpcServerMetrics = ledgerApiServer.grpc
  override def healthMetrics: HealthMetrics = ledgerApiServer.health
  override def storageMetrics: DbStorageMetrics = dbStorage

  object dbStorage extends DbStorageMetrics(inventory.dbStorage, openTelemetryMetricsFactory)

  object consoleThroughput {
    private val prefix = ParticipantMetrics.this.prefix :+ "console"
    val metric: Meter =
      openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "tx-nodes-emitted",
          "Total number of nodes emitted, measured using canton console ledger_api.updates.start_measure",
          MetricQualification.Debug,
        )
      )
    val nodeCount: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.consoleNodeCount.info)
    val transactionSize: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.consoleTransactionSize.info)
  }

  val ledgerApiServer: LedgerApiServerMetrics =
    new LedgerApiServerMetrics(
      inventory.ledgerApiServer,
      openTelemetryMetricsFactory,
    )

  val httpApiServer: HttpApiMetrics =
    new HttpApiMetrics(inventory.httpApi, openTelemetryMetricsFactory)

  private val clients = TrieMap[DomainAlias, Eval[SyncDomainMetrics]]()

  object pruning extends ParticipantPruningMetrics(inventory.pruning, openTelemetryMetricsFactory)

  def domainMetrics(alias: DomainAlias): SyncDomainMetrics = {
    clients
      .getOrElseUpdate(
        alias,
        // Two concurrent calls with the same domain alias may cause getOrElseUpdate to evaluate the new value expression twice,
        // even though only one of the results will be stored in the map.
        // Eval.later ensures that we actually create only one instance of SyncDomainMetrics in such a case
        // by delaying the creation until the getOrElseUpdate call has finished.
        Eval.later(
          new SyncDomainMetrics(inventory.syncDomain, openTelemetryMetricsFactory)(
            mc.withExtraLabels("domain" -> alias.unwrap)
          )
        ),
      )
      .value
  }

  val updatesPublished: Meter = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "updates-published",
      summary = "Number of updates published through the read service to the indexer",
      description =
        """When an update is published through the read service, it has already been committed to the ledger.
        |The indexer will subsequently store the update in a form that allows for querying the ledger efficiently.""",
      qualification = MetricQualification.Traffic,
    )
  )

  val inflightValidationRequests: Gauge[Int] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "inflight_validation_requests",
        summary = "Number of requests being validated.",
        description = """Number of requests that are currently being validated.
                        |This also covers requests submitted by other participants.
                        |""",
        qualification = MetricQualification.Saturation,
        labelsWithDescription = Map(
          "participant" -> "The id of the participant for which the value applies."
        ),
      ),
      0,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val maxInflightValidationRequestGaugeForDocs: Gauge[Int] =
    NoOpGauge(
      MetricInfo(
        prefix :+ "max_inflight_validation_requests",
        summary = "Configured maximum number of requests currently being validated.",
        description =
          """Configuration for the maximum number of requests that are currently being validated.
          |This also covers requests submitted by other participants.
          |A negative value means no configuration value was provided and no limit is enforced.
          |""",
        qualification = MetricQualification.Debug,
        labelsWithDescription = Map(
          "participant" -> "The id of the participant for which the value applies."
        ),
      ),
      0,
    )

  def registerMaxInflightValidationRequest(value: () => Option[Int]): Gauge.CloseableGauge =
    openTelemetryMetricsFactory.gaugeWithSupplier(
      maxInflightValidationRequestGaugeForDocs.info,
      () => value().getOrElse(-1),
    )

}

class SyncDomainHistograms(val parent: MetricName, val sequencerClient: SequencerClientHistograms)(
    implicit inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "sync"

  private[metrics] val transactionProcessing: TransactionProcessingHistograms =
    new TransactionProcessingHistograms(prefix)

  private[metrics] val commitments: CommitmentHistograms = new CommitmentHistograms(prefix)

}

class SyncDomainMetrics(
    histograms: SyncDomainHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  object sequencerClient extends SequencerClientMetrics(histograms.sequencerClient, factory)

  object conflictDetection extends TaskSchedulerMetrics {

    private val prefix = histograms.prefix :+ "conflict-detection"

    val sequencerCounterQueue: Counter =
      factory.counter(
        MetricInfo(
          prefix :+ "sequencer-counter-queue",
          summary = "Size of conflict detection sequencer counter queue",
          description =
            """The task scheduler will work off tasks according to the timestamp order, scheduling
            |the tasks whenever a new timestamp has been observed. This metric exposes the number of
            |un-processed sequencer messages that will trigger a timestamp advancement.""",
          qualification = MetricQualification.Debug,
        )
      )

    val taskQueueForDoc: Gauge[Int] = NoOpGauge(
      MetricInfo(
        prefix :+ "task-queue",
        summary = "Size of conflict detection task queue",
        description = """This metric measures the size of the queue for conflict detection between
                      |concurrent transactions.
                      |A huge number does not necessarily indicate a bottleneck;
                      |it could also mean that a huge number of tasks have not yet arrived at their execution time.""",
        qualification = MetricQualification.Debug,
      ),
      0,
    )
    def taskQueue(size: () => Int): CloseableGauge =
      factory.gauge(taskQueueForDoc.info, 0)

  }

  object commitments extends CommitmentMetrics(histograms.commitments, factory)

  object transactionProcessing
      extends TransactionProcessingMetrics(histograms.transactionProcessing, factory)

  val numInflightValidations: Counter = factory.counter(
    MetricInfo(
      histograms.prefix :+ "inflight-validations",
      summary = "Number of requests being validated on the domain.",
      description = """Number of requests that are currently being validated on the domain.
                    |This also covers requests submitted by other participants.
                    |""",
      qualification = MetricQualification.Saturation,
    )
  )

  object recordOrderPublisher extends TaskSchedulerMetrics {

    private val prefix = histograms.prefix :+ "request-tracker"

    val sequencerCounterQueue: Counter =
      factory.counter(
        MetricInfo(
          prefix :+ "sequencer-counter-queue",
          summary = "Size of record order publisher sequencer counter queue",
          description = """Same as for conflict-detection, but measuring the sequencer counter
                        |queues for the publishing to the ledger api server according to record time.""",
          qualification = MetricQualification.Debug,
        )
      )

    val taskQueueForDoc: Gauge[Int] = NoOpGauge(
      MetricInfo(
        prefix :+ "task-queue",
        summary = "Size of record order publisher task queue",
        description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                      |exposes the number of tasks that are waiting in the task queue for the right time to pass.""",
        qualification = MetricQualification.Debug,
      ),
      0,
    )
    def taskQueue(size: () => Int): CloseableGauge =
      factory.gaugeWithSupplier(taskQueueForDoc.info, size)
  }

  // TODO(i14580): add testing
  object trafficControl {

    private val prefix = histograms.prefix :+ "traffic-control"

    val extraTrafficAvailable: Gauge[Long] =
      factory.gauge(
        MetricInfo(
          prefix :+ "extra-traffic-credit-available",
          summary = "Current amount of extra traffic remaining",
          description = """Gets updated with every event received.""",
          qualification = MetricQualification.Traffic,
        ),
        0L,
      )

    val topologyTransaction: Gauge[Long] =
      factory.gauge(
        MetricInfo(
          prefix :+ "traffic-state-topology-transaction",
          summary = "Records a new top up on the participant",
          description = """Records top up events and the new extra traffic limit associated.""",
          qualification = MetricQualification.Traffic,
        ),
        0L,
      )

    val eventAboveTrafficLimit: Meter = factory.meter(
      MetricInfo(
        prefix :+ "event-above-traffic-limit",
        summary = "Event was not delivered because of traffic limit exceeded",
        description = """An event was not delivered because of insufficient traffic credit.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val eventDelivered: Meter = factory.meter(
      MetricInfo(
        prefix :+ "event-delivered",
        summary = "Event was delivered",
        description = """An event was not delivered.""",
        qualification = MetricQualification.Traffic,
      )
    )
  }

}
