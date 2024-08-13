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

import scala.collection.mutable
import scala.concurrent.blocking

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
      description = "Records the rate and latency when querying the topology client.",
      qualification = MetricQualification.Latency,
    )
  }

  object p2p {
    val prefix: MetricName = BftOrderingHistograms.this.prefix :+ "p2p"

    object send {
      val prefix: MetricName = p2p.prefix :+ "send"

      private[metrics] val networkWriteLatency: Item = Item(
        prefix :+ "network-write-latency",
        summary = "Message network write latency",
        description = "Records the rate and latency when writing P2P messages to the network.",
        qualification = MetricQualification.Latency,
      )
    }

    object receive {
      val prefix: MetricName = p2p.prefix :+ "receive"

      private[metrics] val processingLatency: Item = Item(
        prefix :+ "processing-latency",
        summary = "Message receive processing latency",
        description = "Records the rate and latency when processing incoming P2P network messages.",
        qualification = MetricQualification.Latency,
      )
    }
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
    p2p.send.networkWriteLatency.discard
    p2p.receive.processingLatency.discard
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

    object labels {
      val ReportingSequencer: String = "reporting-sequencer"
    }

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
        val ReceivingSequencer: String = "receiving-sequencer"
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
      val ForSequencer: String = "for-sequencer"

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
        description = "Measures the size of the mempool in requests.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val bytesQueued: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "bytes-queued",
        summary = "Bytes queued",
        description = "Measures the size of the mempool in bytes.",
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

  object mempool {
    private val prefix = BftOrderingMetrics.this.prefix :+ "mempool"

    val requestedBatches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "requested-batches",
        summary = "Requested batches",
        description = "Number of batches requested from the mempool by the availability module.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )
  }

  object availability {
    private val prefix = BftOrderingMetrics.this.prefix :+ "availability"

    val requestedProposals: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "requested-proposals",
        summary = "Requested proposals",
        description = "Number of proposals requested from availability by the consensus module.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val requestedBatches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "requested-batches",
        summary = "Requested batches",
        description =
          "Maximum number of batches requested from availability by the consensus module.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val readyBytes: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "ready-bytes",
        summary = "Bytes ready for consensus",
        description =
          "Number of bytes disseminated, provably highly available and ready for consensus.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val readyRequests: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "ready-requests",
        summary = "Requests ready for consensus",
        description =
          "Number of requests disseminated, provably highly available and ready for consensus.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val readyBatches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "ready-batches",
        summary = "Batches ready for consensus",
        description =
          "Number of batches disseminated, provably highly available and ready for consensus.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )
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

    val epochLength: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "epoch-length",
        summary = "Epoch length",
        description = "Length of the current epoch in number of blocks.",
        qualification = MetricQualification.Traffic,
      ),
      0,
    )

    val commitLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.consensus.consensusCommitLatency.info)

    object votes {

      object labels {
        val VotingSequencer: String = "voting-sequencer"
      }

      private val prepareGauges = mutable.Map[SequencerId, Gauge[Double]]()
      private val commitGauges = mutable.Map[SequencerId, Gauge[Double]]()

      def prepareVotesPercent(sequencerId: SequencerId): Gauge[Double] =
        getOrElseUpdateGauge(
          prepareGauges,
          sequencerId,
          "prepare-votes-percent",
          "Block vote % during prepare",
          "Percentage of BFT sequencers that voted for a block in the PBFT prepare stage.",
        )

      def commitVotesPercent(sequencerId: SequencerId): Gauge[Double] =
        getOrElseUpdateGauge(
          commitGauges,
          sequencerId,
          "commit-votes-percent",
          "Block vote % during commit",
          "Percentage of BFT sequencers that voted for a block in the PBFT commit stage.",
        )

      def cleanupVoteGauges(keepOnly: Set[SequencerId]): Unit =
        blocking {
          synchronized {
            keepOnlyGaugesFor(prepareGauges, keepOnly)
            keepOnlyGaugesFor(commitGauges, keepOnly)
          }
        }

      private def getOrElseUpdateGauge(
          gauges: mutable.Map[SequencerId, Gauge[Double]],
          sequencerId: SequencerId,
          name: String,
          summary: String,
          description: String,
      ): Gauge[Double] = {
        val mc1 = mc.withExtraLabels(labels.VotingSequencer -> sequencerId.toProtoPrimitive)
        blocking {
          synchronized {
            locally {
              implicit val mc: MetricsContext = mc1
              gauges.getOrElseUpdate(
                sequencerId,
                openTelemetryMetricsFactory.gauge(
                  MetricInfo(
                    prefix :+ name,
                    summary,
                    MetricQualification.Traffic,
                    description,
                  ),
                  0.0d,
                ),
              )
            }
          }
        }
      }

      private def keepOnlyGaugesFor[T](
          gaugesMap: mutable.Map[SequencerId, Gauge[T]],
          keepOnly: Set[SequencerId],
      ): Unit =
        gaugesMap.view.filterKeys(!keepOnly.contains(_)).foreach { case (id, gauge) =>
          gauge.close()
          gaugesMap.remove(id).discard
        }
    }
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
    private val prefix = histograms.p2p.prefix

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
      private val prefix = histograms.p2p.send.prefix

      object labels {
        val TargetSequencer: String = "target-sequencer"
        val DroppedAsUnauthenticated: String = "dropped-as-unauthenticated"

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

      val networkWriteLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.send.networkWriteLatency.info)
    }

    object receive {
      private val prefix = histograms.p2p.receive.prefix

      object labels {
        val SourceSequencer: String = "source-sequencer"

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

      val processingLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.receive.processingLatency.info)
    }
  }
}

object BftOrderingMetrics {
  val Prefix: MetricName = MetricName("bftordering")
}
