// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Gauge, Histogram, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.logging.pretty.PrettyNameOnlyCase
import com.digitalasset.canton.metrics.{DbStorageHistograms, DbStorageMetrics}
import com.digitalasset.canton.topology.SequencerId

class BftOrderingHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ BftOrderingMetrics.Prefix

  private[metrics] val dbStorage = new DbStorageHistograms(parent)

  object global {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "global"

    private[metrics] val requestsOrderingLatency: Item = Item(
      prefix :+ "requests-ordering-latency",
      summary = "Requests ordering latency",
      description =
        """Records the rate and latency it takes to order requests. This metric is always meaningful
          |when queried on and restricted to the receiving sequencer; in other cases, it is meaningful only
          |when the receiving and reporting sequencers' clocks are kept synchronized.""",
      qualification = MetricQualification.Latency,
    )
  }

  object ingress {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "ingress"

    private[metrics] val requestsSize: Item = Item(
      prefix :+ "requests-size",
      summary = "Requests size",
      description = "Records the size of requests to the BFT ordering service.",
      qualification = MetricQualification.Traffic,
    )
  }

  object consensus {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "consensus"

    private[metrics] val consensusCommitLatency: Item = Item(
      prefix :+ "commit-latency",
      summary = "Consensus commit latency",
      description =
        "Records the rate and latency it takes to commit a block at the consensus level.",
      qualification = MetricQualification.Latency,
    )
  }

  object output {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "output"

    private[metrics] val blockSizeBytes: Item = Item(
      prefix :+ "block-size-bytes",
      summary = "Block size (bytes)",
      description = "Records the size (in bytes) of blocks ordered.",
      qualification = MetricQualification.Traffic,
    )

    private[metrics] val blockSizeRequests: Item = Item(
      prefix :+ "block-size-requests",
      summary = "Block size (requests)",
      description = "Records the size (in requests) of blocks ordered.",
      qualification = MetricQualification.Traffic,
    )

    private[metrics] val blockSizeBatches: Item = Item(
      prefix :+ "block-size-batches",
      summary = "Block size (batches)",
      description = "Records the size (in batches) of blocks ordered.",
      qualification = MetricQualification.Traffic,
    )
  }

  object topology {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "topology"

    private[metrics] val queryLatency: Item = Item(
      prefix :+ "query-latency",
      summary = "Topology query latency",
      description = "Records the rate and latency it takes to query the topology client.",
      qualification = MetricQualification.Latency,
    )
  }

  // Force the registration of all histograms, else it would happen too late
  //  because Scala `object`s are lazily initialized.
  {
    global.requestsOrderingLatency.discard
    ingress.requestsSize.discard
    consensus.consensusCommitLatency.discard
    topology.queryLatency.discard
    output.blockSizeBytes.discard
    output.blockSizeRequests.discard
    output.blockSizeBatches.discard
  }
}

class BftOrderingMetrics(
    histograms: BftOrderingHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
    override val grpcMetrics: GrpcServerMetrics,
    override val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  object dbStorage extends DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  private implicit val mc: MetricsContext = MetricsContext.Empty

  override val prefix: MetricName = histograms.prefix

  override def storageMetrics: DbStorageMetrics = dbStorage

  object global {

    private val prefix = histograms.global.prefix

    val blocksOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "ordered-blocks",
        summary = "Blocks ordered",
        description = "Measures the total blocks ordered.",
        qualification = MetricQualification.Traffic,
      )
    )

    object requestsOrderingLatency {
      object labels {
        val ReceivingSequencer: String = "receivingSequencer"
      }

      val timer: Timer =
        openTelemetryMetricsFactory.timer(histograms.global.requestsOrderingLatency.info)
    }
  }

  object ingress {
    private val prefix = histograms.ingress.prefix

    object labels {
      val Tag: String = "tag"
      val Sender: String = "sender"
      val ForSequencer: String = "forSequencer"

      object outcome {
        val Key: String = "outcome"

        object values {
          sealed trait OutcomeValue extends PrettyNameOnlyCase
          case object Success extends OutcomeValue
          case object QueueFull extends OutcomeValue
          case object RequestTooBig extends OutcomeValue
        }
      }
    }

    val requestsReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "received-requests",
        summary = "Requests received",
        description = "Measures the total requests received.",
        qualification = MetricQualification.Traffic,
      )
    )

    val requestsQueued: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "requests-queued",
        summary = "Requests queued",
        description = "Measures the size of the mempool.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val bytesReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "received-bytes",
        summary = "Bytes received",
        description = "Measures the total bytes received.",
        qualification = MetricQualification.Traffic,
      )
    )

    val requestsSize: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.ingress.requestsSize.info)
  }

  object security {
    private val prefix = BftOrderingMetrics.this.prefix :+ "security"

    object noncompliant {
      private val prefix = security.prefix :+ "noncompliant"

      object labels {
        val Endpoint: String = "endpoint"
        val Sequencer: String = "sequencer"
        val Epoch: String = "epoch"
        val View: String = "view"
        val Block: String = "block"

        object violationType {
          val Key: String = "violationType"

          object values {
            sealed trait ViolationTypeValue extends PrettyNameOnlyCase

            case object AuthIdentityEquivocation extends ViolationTypeValue
            case object DisseminationInvalidMessage extends ViolationTypeValue
            case object ConsensusInvalidMessage extends ViolationTypeValue
            case object ConsensusDataEquivocation extends ViolationTypeValue
            case object ConsensusRoleEquivocation extends ViolationTypeValue
          }
        }
      }

      val behavior: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "behavior",
          summary = "Non-compliant behaviors",
          description = "Number of non-compliant (potentially malicious) behaviors detected.",
          qualification = MetricQualification.Errors,
        )
      )
    }
  }

  object consensus {
    private val prefix = histograms.consensus.prefix

    val epoch: Gauge[Long] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "epoch",
        summary = "Epoch number",
        description = "Current epoch number for the node.",
        qualification = MetricQualification.Traffic,
      ),
      0,
    )

    val commitLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.consensus.consensusCommitLatency.info)

    val prepareVotesPercent: Gauge[Double] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "prepare-votes-percent",
        summary = "Block vote % during prepare",
        description =
          "Percentage of BFT sequencers that voted for a block in the PBFT prepare stage.",
        qualification = MetricQualification.Traffic,
      ),
      0.0d,
    )

    val commitVotesPercent: Gauge[Double] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "commit-votes-percent",
        summary = "Block vote % during commit",
        description =
          "Percentage of BFT sequencers that voted for a block in the PBFT commit stage.",
        qualification = MetricQualification.Traffic,
      ),
      0.0d,
    )
  }

  object output {
    val blockSizeBytes: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeBytes.info)

    val blockSizeRequests: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeRequests.info)

    val blockSizeBatches: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeBatches.info)
  }

  object topology {
    private val prefix = histograms.topology.prefix

    val validators: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "validators",
        summary = "Active validators",
        description = "Number of BFT sequencers actively involved in consensus.",
        qualification = MetricQualification.Traffic,
      ),
      0,
    )

    val queryLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.topology.queryLatency.info)
  }

  object p2p {
    private val prefix = BftOrderingMetrics.this.prefix :+ "p2p"

    object connections {
      private val prefix = p2p.prefix :+ "connections"

      val connected: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "connected",
          summary = "Connected peers",
          description = "Number of connected P2P endpoints.",
          qualification = MetricQualification.Traffic,
        ),
        0,
      )

      val authenticated: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "authenticated",
          summary = "Authenticated peers",
          description = "Number of connected P2P endpoints that are also authenticated.",
          qualification = MetricQualification.Traffic,
        ),
        0,
      )
    }

    object send {
      private val prefix = p2p.prefix :+ "send"

      object labels {
        val TargetSequencer: String = "targetSequencer"
        val DroppedAsUnauthenticated: String = "droppedAsUnauthenticated"

        object targetModule {
          val Key: String = "targetModule"

          object values {
            sealed trait TargetModuleValue extends PrettyNameOnlyCase
            case object Availability extends TargetModuleValue
            case object Consensus extends TargetModuleValue
          }
        }
      }

      val sentBytes: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "sent-bytes",
          summary = "Bytes sent",
          description = "Total P2P bytes sent.",
          qualification = MetricQualification.Traffic,
        )
      )

      val sentMessages: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "sent-messages",
          summary = "Messages sent",
          description = "Total P2P messages sent.",
          qualification = MetricQualification.Traffic,
        )
      )
    }

    object receive {
      private val prefix = p2p.prefix :+ "receive"

      object labels {
        val SourceSequencer: String = "sourceSequencer"

        object source {
          val Key: String = "targetModule"

          object values {
            sealed trait SourceValue extends PrettyNameOnlyCase
            case object SourceParsingFailed extends SourceValue
            case class Empty(from: SequencerId) extends SourceValue
            case class Availability(from: SequencerId) extends SourceValue
            case class Consensus(from: SequencerId) extends SourceValue
          }
        }
      }

      val receivedBytes: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "received-bytes",
          summary = "Bytes received",
          description = "Total P2P bytes received.",
          qualification = MetricQualification.Traffic,
        )
      )

      val receivedMessages: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "received-messages",
          summary = "Messages received",
          description = "Total P2P messages received.",
          qualification = MetricQualification.Traffic,
        )
      )
    }
  }
}

object BftOrderingMetrics {
  val Prefix: MetricName = MetricName("bftordering")
}
