// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.daml.metrics.grpc.{DamlGrpcServerHistograms, DamlGrpcServerMetrics, GrpcServerMetrics}
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.metrics.{
  CacheMetrics,
  DbStorageHistograms,
  DbStorageMetrics,
  DeclarativeApiMetrics,
  SequencerClientHistograms,
  SequencerClientMetrics,
  TrafficConsumptionMetrics,
}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  MediatorGroupRecipient,
  MemberRecipient,
  Recipient,
  SequencersOfSynchronizer,
}
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.annotation.unused

class SequencerHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {
  @unused
  private val _grpc = new DamlGrpcServerHistograms()
  private[metrics] val prefix = parent :+ "sequencer"
  private[metrics] val sequencerClient = new SequencerClientHistograms(parent)
  private[metrics] val dbStorage = new DbStorageHistograms(parent)
  private[metrics] val bftOrdering: BftOrderingHistograms = new BftOrderingHistograms(prefix)
}

class SequencerMetrics(
    histograms: SequencerHistograms,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends BaseMetrics {
  override val prefix: MetricName = histograms.prefix
  private implicit val mc: MetricsContext = MetricsContext.Empty
  override val declarativeApiMetrics: DeclarativeApiMetrics =
    new DeclarativeApiMetrics(prefix, openTelemetryMetricsFactory)

  override val grpcMetrics: GrpcServerMetrics =
    new DamlGrpcServerMetrics(openTelemetryMetricsFactory, "sequencer")
  override val healthMetrics: HealthMetrics = new HealthMetrics(openTelemetryMetricsFactory)

  val bftOrdering: BftOrderingMetrics =
    new BftOrderingMetrics(
      histograms.bftOrdering,
      openTelemetryMetricsFactory,
      new DamlGrpcServerMetrics(openTelemetryMetricsFactory, "bftordering"),
      new HealthMetrics(openTelemetryMetricsFactory),
    )

  val dbSequencer: DatabaseSequencerMetrics =
    new DatabaseSequencerMetrics(
      prefix,
      openTelemetryMetricsFactory,
    )

  override def storageMetrics: DbStorageMetrics = dbStorage

  val block: BlockMetrics = new BlockMetrics(prefix, openTelemetryMetricsFactory)

  val sequencerClient: SequencerClientMetrics =
    new SequencerClientMetrics(histograms.sequencerClient, openTelemetryMetricsFactory)

  // Private constructor to avoid being instantiated multiple times by accident
  final class PublicApiMetrics private[SequencerMetrics] {
    private val prefix = SequencerMetrics.this.prefix :+ "public-api"
    val subscriptionsGauge: Gauge[Int] =
      openTelemetryMetricsFactory.gauge[Int](
        MetricInfo(
          prefix :+ "subscriptions",
          summary = "Number of active sequencer subscriptions",
          description =
            """This metric indicates the number of active subscriptions currently open and actively
            |served subscriptions at the sequencer.""",
          qualification = MetricQualification.Traffic,
        ),
        0,
      )

    val messagesProcessed: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "processed",
        summary = "Number of messages processed by the sequencer",
        description =
          """This metric measures the number of successfully validated messages processed
          |by the sequencer since the start of this process.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val bytesProcessed: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ s"processed-${Histogram.Bytes}",
        summary = "Number of message bytes processed by the sequencer",
        description =
          """This metric measures the total number of message bytes processed by the sequencer.
          |If the message received by the sequencer contains duplicate or irrelevant fields,
          |the contents of these fields do not contribute to this metric.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val timeRequests: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "time-requests",
        summary = "Number of time requests received by the sequencer",
        description =
          """When a Participant needs to know the synchronizer time it will make a request for a time proof to be sequenced.
          |It would be normal to see a small number of these being sequenced, however if this number becomes a significant
          |portion of the total requests to the sequencer it could indicate that the strategy for requesting times may
          |need to be revised to deal with different clock skews and latencies between the sequencer and participants.""",
        qualification = MetricQualification.Debug,
      )
    )
  }

  val publicApi = new PublicApiMetrics

  val maxEventAge: Gauge[Long] =
    openTelemetryMetricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "max-event-age",
        summary = "Age of oldest unpruned sequencer event.",
        description =
          """This gauge exposes the age of the oldest, unpruned sequencer event in hours as a way to quantify the
            |pruning backlog.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val dbStorage: DbStorageMetrics =
    new DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  // Private constructor to avoid being instantiated multiple times by accident
  final class TrafficControlMetrics private[SequencerMetrics] {
    private val prefix: MetricName = SequencerMetrics.this.prefix :+ "traffic-control"

    val trafficConsumption = new TrafficConsumptionMetrics(prefix, openTelemetryMetricsFactory)

    val purchaseCache: CacheMetrics =
      new CacheMetrics(prefix :+ "purchase-cache", openTelemetryMetricsFactory)

    val consumedCache: CacheMetrics =
      new CacheMetrics(prefix :+ "consumed-cache", openTelemetryMetricsFactory)

    val wastedSequencing: Meter =
      openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "wasted-sequencing",
          summary =
            "Byte size of events that got sequenced but failed to pass validation steps after sequencing",
          description =
            """Record the raw byte size of events that are ordered but were not delivered because of traffic enforcement.
              |""",
          qualification = MetricQualification.Traffic,
        )
      )

    val wastedSequencingCounter: Counter = openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "wasted-sequencing-counter",
        summary =
          "Number of events that failed traffic validation and were not delivered because of it.",
        description = """Counter for wasted-sequencing.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val wastedTraffic: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "wasted-traffic",
        summary = "Cost of event that was deducted but not delivered.",
        description = """Events can have their cost deducted but still not be delivered
            | due to other failed validation after ordering. This metrics records the traffic cost
            | of such events.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val wastedTrafficCounter: Counter = openTelemetryMetricsFactory.counter(
      MetricInfo(
        prefix :+ "wasted-traffic-counter",
        summary = "Number of events that cost traffic but were not delivered.",
        description = """Counter for wasted-traffic.""",
        qualification = MetricQualification.Traffic,
      )
    )

    val balanceUpdateProcessed: Counter =
      openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "balance-update",
          summary = "Counts balance updates fully processed by the sequencer.",
          description = """Value of balance updates for all (aggregated).""",
          qualification = MetricQualification.Traffic,
        )
      )

    val balanceCacheMissesForTimestamp: Counter =
      openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "balance-cache-miss-for-timestamp",
          summary = "Counts cache misses when trying to retrieve a balance for a given timestamp.",
          description = """The per member cache only keeps in memory a subset of all the non-pruned balance updates persisted in the database.
                        |If the cache contains *some* balances for a member but not the one requested, a DB call will be made to try to retrieve it.
                        |When that happens, this metric is incremented. If this occurs too frequently, consider increasing the config value of trafficPurchasedCacheSizePerMember.""",
          qualification = MetricQualification.Debug,
        )
      )
  }
  // TODO(i14580): add testing
  val trafficControl = new TrafficControlMetrics
}

object SequencerMetrics {

  @VisibleForTesting
  def noop(testName: String) = new SequencerMetrics(
    new SequencerHistograms(MetricName(testName))(new HistogramInventory),
    NoOpMetricsFactory,
  )

  private[synchronizer] final case class RecipientStats(
      participants: Boolean = false,
      mediators: Boolean = false,
      sequencers: Boolean = false,
      broadcast: Boolean = false,
  ) {

    private[synchronizer] def metricsContext(
        sender: Member,
        logger: TracedLogger,
        warnOnUnexpected: Boolean = true,
    )(implicit traceContext: TraceContext): MetricsContext = {
      val messageType =
        // by looking at the recipient lists and the sender, we'll figure out what type of message we've been getting
        (sender, participants, mediators, sequencers, broadcast) match {
          case (ParticipantId(_), false, true, false, false) =>
            "send-confirmation-response"
          case (ParticipantId(_), true, true, false, false) =>
            "send-confirmation-request"
          case (MediatorId(_), true, false, false, false) =>
            "send-verdict"
          case (ParticipantId(_), true, false, false, false) =>
            "send-commitment"
          case (SequencerId(_), true, false, true, false) =>
            "send-topup"
          case (SequencerId(_), false, true, true, false) =>
            "send-topup-med"
          case (_, false, false, false, true) =>
            "send-topology"
          case (_, false, false, false, false) =>
            "send-time-proof"
          case _ =>
            def r(boolean: Boolean, s: String) = if (boolean) Seq(s) else Seq.empty

            val recipients = r(participants, "participants") ++
              r(mediators, "mediators") ++
              r(sequencers, "sequencers") ++
              r(broadcast, "broadcast")
            if (warnOnUnexpected)
              logger.warn(s"Unexpected message from $sender to " + recipients.mkString(","))
            "send-unexpected"
        }
      MetricsContext(
        "member" -> sender.toString,
        "type" -> messageType,
      )
    }
  }

  def submissionTypeMetricsContext(
      allRecipients: Set[Recipient],
      member: Member,
      logger: TracedLogger,
      warnOnUnexpected: Boolean = true,
  )(implicit traceContext: TraceContext): MetricsContext =
    allRecipients
      .foldLeft(RecipientStats()) {
        case (acc, MemberRecipient(ParticipantId(_))) =>
          acc.copy(participants = true)
        case (acc, MemberRecipient(MediatorId(_)) | MediatorGroupRecipient(_)) =>
          acc.copy(mediators = true)
        case (acc, MemberRecipient(SequencerId(_)) | SequencersOfSynchronizer) =>
          acc.copy(sequencers = true)
        case (acc, AllMembersOfSynchronizer) => acc.copy(broadcast = true)
      }
      .metricsContext(member, logger, warnOnUnexpected)
}

class MediatorHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ "mediator"
  private[metrics] val sequencerClient = new SequencerClientHistograms(parent)
  private[metrics] val dbStorage = new DbStorageHistograms(parent)

}
class MediatorMetrics(
    histograms: MediatorHistograms,
    val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends BaseMetrics {

  override val grpcMetrics: GrpcServerMetrics =
    new DamlGrpcServerMetrics(openTelemetryMetricsFactory, "mediator")
  override val healthMetrics: HealthMetrics = new HealthMetrics(openTelemetryMetricsFactory)

  override val prefix: MetricName = histograms.prefix
  private implicit val mc: MetricsContext = MetricsContext.Empty

  override val declarativeApiMetrics: DeclarativeApiMetrics =
    new DeclarativeApiMetrics(prefix, openTelemetryMetricsFactory)

  override def storageMetrics: DbStorageMetrics = dbStorage

  val dbStorage: DbStorageMetrics =
    new DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  val sequencerClient: SequencerClientMetrics =
    new SequencerClientMetrics(histograms.sequencerClient, openTelemetryMetricsFactory)

  val outstanding: Gauge[Int] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "outstanding-requests",
        summary = "Number of currently outstanding requests",
        description = """This metric provides the number of currently open requests registered
                      |with the mediator.""",
        qualification = MetricQualification.Debug,
      ),
      0,
    )(MetricsContext.Empty)

  val requests: Meter = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "requests",
      summary = "Total number of processed confirmation requests (approved and rejected)",
      description =
        """This metric provides the number of processed confirmation requests since the system
           |has been started.""",
      qualification = MetricQualification.Debug,
    )
  )

  val approvedRequests: Meter = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "approved-requests",
      summary = "Total number of approved confirmation requests",
      description =
        """This metric provides the total number of approved confirmation requests since the system
          |has been started.
          |A confirmation request is approved if all the required confirmations are received by the mediator
          |within the decision time.""",
      qualification = MetricQualification.Debug,
    )
  )

  val maxEventAge: Gauge[Long] =
    openTelemetryMetricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "max-event-age",
        summary = "Age of oldest unpruned confirmation response.",
        description =
          """This gauge exposes the age of the oldest, unpruned confirmation response in hours as a way to quantify the
          |pruning backlog.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )(
      MetricsContext.Empty
    )
}
