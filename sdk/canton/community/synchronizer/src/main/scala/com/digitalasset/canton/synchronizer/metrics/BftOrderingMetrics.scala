// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.*
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.logging.pretty.PrettyNameOnlyCase
import com.digitalasset.canton.metrics.{
  DbStorageHistograms,
  DbStorageMetrics,
  DeclarativeApiMetrics,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleName
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.concurrent.blocking
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

private[metrics] final class BftOrderingHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ BftOrderingMetrics.Prefix

  private[metrics] val dbStorage = new DbStorageHistograms(parent)

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class PerformanceHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "performance"

    private[metrics] val orderingStageLatency: Item = Item(
      prefix :+ "ordering-stage-latency",
      summary = "Ordering stage latency",
      description =
        """Records the rate and latency it takes for an ordering stage, which is recorded as a label.
          |This metric is meaningful only when sequencers' clocks are kept synchronized.""",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val performance = new PerformanceHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class GlobalHistograms private[BftOrderingHistograms] {
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
  private[metrics] val global = new GlobalHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class IngressHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "ingress"

    private[metrics] val requestsSize: Item = Item(
      prefix :+ "requests-size",
      summary = "Requests size",
      description = "Records the size of requests to the BFT ordering service.",
      qualification = MetricQualification.Traffic,
    )
  }
  private[metrics] val ingress = new IngressHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class ConsensusHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "consensus"

    private[metrics] val consensusCommitLatency: Item = Item(
      prefix :+ "commit-latency",
      summary = "Consensus commit latency",
      description =
        "Records the rate and latency it takes to commit a block at the consensus level.",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val consensus = new ConsensusHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class OutputHistograms private[BftOrderingHistograms] {
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

    private[metrics] val blockDelay: Item = Item(
      prefix :+ "block-delay",
      summary = "Block delay",
      description =
        "Wall-clock time of the ordered block being provided to the sequencer minus BFT time of the block.",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val output = new OutputHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class TopologyHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "topology"

    private[metrics] val queryLatency: Item = Item(
      prefix :+ "query-latency",
      summary = "Topology query latency",
      description = "Records the rate and latency when querying the topology client.",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val topology = new TopologyHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class P2PHistograms private[BftOrderingHistograms] {
    val p2pPrefix: MetricName = BftOrderingHistograms.this.prefix :+ "p2p"

    // Private constructor to avoid being instantiated multiple times by accident
    private[metrics] class SendMetrics private[P2PHistograms] {
      val prefix: MetricName = p2pPrefix :+ "send"

      private[metrics] val networkWriteLatency: Item = Item(
        prefix :+ "network-write-latency",
        summary = "Message network write latency",
        description = "Records the rate and latency when writing P2P messages to the network.",
        qualification = MetricQualification.Latency,
      )

      private[metrics] val grpcLatency: Item = Item(
        prefix :+ "grpc-latency",
        summary = "Latency of a gRPC message send",
        description = "Records the rate and latency of a gRPC message send.",
        qualification = MetricQualification.Latency,
      )
    }
    private[metrics] val send = new SendMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    private[metrics] final class ReceiveMetrics private[P2PHistograms] {
      val prefix: MetricName = p2pPrefix :+ "receive"

      private[metrics] val processingLatency: Item = Item(
        prefix :+ "processing-latency",
        summary = "Message receive processing latency",
        description = "Records the rate and latency when processing incoming P2P network messages.",
        qualification = MetricQualification.Latency,
      )
    }
    private[metrics] val receive = new ReceiveMetrics
  }
  private[metrics] val p2p = new P2PHistograms
}

class BftOrderingMetrics private[metrics] (
    histograms: BftOrderingHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
    override val grpcMetrics: GrpcServerMetrics,
    override val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  private implicit val mc: MetricsContext = MetricsContext.Empty

  val dbStorage: DbStorageMetrics =
    new DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  override val prefix: MetricName = histograms.prefix
  override val declarativeApiMetrics: DeclarativeApiMetrics =
    new DeclarativeApiMetrics(prefix, openTelemetryMetricsFactory)

  override def storageMetrics: DbStorageMetrics = dbStorage

  // Private constructor to avoid being instantiated multiple times by accident
  final class PerformanceMetrics private[BftOrderingMetrics] {

    // Private constructor to avoid being instantiated multiple times by accident
    final class OrderingStageLatencyMetrics private[BftOrderingMetrics] {
      object labels {

        object stage {
          val Key: String = "ordering-stage"

          object values {

            object mempool {
              // Insertion in batch queue until extraction and batch send to availability
              val RequestQueuedForBatchInclusion = "request-queued-for-batch-inclusion"
            }

            val BatchAvailabilityTotal = "batch-availability-total"

            object availability {
              // Time waited for a batch while there are outstanding consensus proposal requests and no dissemination is in progress
              val BatchWait = "batch-wait"

              // Cumulative latency of a batch dissemination until a PoA is formed and
              //  it can be present multiple times for a given batch if it regresses due to topology changes
              val BatchDissemination = "batch-dissemination-total"

              object dissemination {
                // The following latencies can be present multiple times for a given batch
                //  in case of multiple ack collections and regressions due to topology changes

                // Duration of batch validation when a node receives a batch store
                //  request (emitted 0 or more times per batch)
                val BatchValidation = "batch-validation"

                // Insertion in proposal queue until next event, like re-sign, re-dissemination, [re-]proposal
                //  or batch ordered (emitted 1 or more times per batch)
                val BatchQueuedForBlockInclusion = "batch-queued-for-block-inclusion"
              }
            }
          }
        }
      }

      val timer: Timer =
        openTelemetryMetricsFactory.timer(histograms.performance.orderingStageLatency.info)

      def emitModuleQueueLatency(
          moduleName: ModuleName,
          sendInstant: Instant,
          maybeDelay: Option[FiniteDuration],
      )(implicit metricsContext: MetricsContext): Unit = {
        val latency =
          Duration.between(sendInstant, Instant.now).minus(maybeDelay.fold(Duration.ZERO)(_.toJava))
        timer.update(latency)(
          metricsContext.withExtraLabels(labels.stage.Key -> s"module-queue-${moduleName.name}")
        )
      }

      def emitOrderingStageLatency[R](
          stage: String,
          op: () => R,
      )(implicit
          mc: MetricsContext
      ): R = {
        val startInstant = Instant.now()
        val result = op()
        val duration = Duration.between(startInstant, Instant.now())
        emitOrderingStageLatency(stage, duration)
        result
      }

      def emitOrderingStageLatency(
          stage: String,
          startInstant: Instant,
          endInstant: Instant,
      )(implicit
          mc: MetricsContext
      ): Unit =
        emitOrderingStageLatency(
          stage,
          Duration.between(startInstant, endInstant),
        )

      def emitOrderingStageLatency(
          stage: String,
          duration: Duration,
      )(implicit
          mc: MetricsContext
      ): Unit =
        performance.orderingStageLatency.timer
          .update(duration)(
            mc.withExtraLabels(
              performance.orderingStageLatency.labels.stage.Key ->
                stage
            )
          )
    }
    val orderingStageLatency = new OrderingStageLatencyMetrics
  }
  val performance = new PerformanceMetrics
  // Private constructor to avoid being instantiated multiple times by accident
  final class GlobalMetrics private[BftOrderingMetrics] {

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

    // Private constructor to avoid being instantiated multiple times by accident
    final class RequestsOrderingLatencyMetrics private[BftOrderingMetrics] {
      object labels {
        val ReceivingSequencer: String = "receiving-sequencer"
      }

      val timer: Timer =
        openTelemetryMetricsFactory.timer(histograms.global.requestsOrderingLatency.info)
    }
    val requestsOrderingLatency = new RequestsOrderingLatencyMetrics
  }
  val global = new GlobalMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class IngressMetrics private[BftOrderingMetrics] {
    private val prefix = histograms.ingress.prefix

    object labels {
      val Tag: String = "tag"
      val Sender: String = "sender"

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
  val ingress = new IngressMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class MempoolMetrics private[BftOrderingMetrics] {
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
  val mempool = new MempoolMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class AvailabilityMetrics private[BftOrderingMetrics] {
    private val prefix = BftOrderingMetrics.this.prefix :+ "availability"

    object requested {
      val proposals: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "requested-proposals",
          summary = "Requested proposals",
          description = "Number of proposals requested from availability by the consensus module.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val batches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "requested-batches",
          summary = "Requested batches",
          description =
            "Maximum number of batches requested from availability by the consensus module.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )
    }

    object dissemination {

      object labels {
        val ReadyForConsensus = "ready-for-consensus" // true or false
      }

      val bytes: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "disseminating-bytes",
          summary = "Bytes being disseminated",
          description = "Number of bytes being disseminated.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val requests: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "disseminating-requests",
          summary = "Requests being disseminated",
          description = "Number of requests being disseminated.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val batches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "disseminating-batches",
          summary = "Batches being disseminated",
          description = "Number of batches being disseminated.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )
    }

    object regression {
      object labels {
        object stage {
          val Key = "stage"

          object values {
            sealed trait RegressionStageValue extends PrettyNameOnlyCase
            case object Signing extends RegressionStageValue
            case object Dissemination extends RegressionStageValue
          }
        }
      }

      val batch: Counter = openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "batch-regressions",
          summary = "Batch regressions",
          description = "Count of regressions of a single batch due to topology changes",
          qualification = MetricQualification.Saturation,
        )
      )
    }
  }
  val availability = new AvailabilityMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class SecurityMetrics private[BftOrderingMetrics] {
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
            case object StateTransferInvalidMessage extends ViolationTypeValue
            case object RetransmissionResponseInvalidMessage extends ViolationTypeValue
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
  val security = new SecurityMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class ConsensusMetrics private[BftOrderingMetrics] {
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

    val epochLength: Gauge[Long] = openTelemetryMetricsFactory.gauge(
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

    // Private constructor to avoid being instantiated multiple times by accident
    final class RetransmissionsMetrics private[BftOrderingMetrics] {

      val incomingRetransmissionsRequestsMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "incoming-retransmission-requests",
          summary = "Incoming retransmissions requests",
          description = "Retransmissions requests received during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val outgoingRetransmissionsRequestsMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "outgoing-retransmission-requests",
          summary = "Outgoing retransmissions requests",
          description = "Retransmissions requests sent during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val retransmittedMessagesMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "retransmitted-messages",
          summary = "Retransmitted PBFT messages",
          description = "Number of PBFT messages retransmitted during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val retransmittedCommitCertificatesMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "retransmitted-commit-certificates",
          summary = "Retransmitted commit certificates",
          description = "Number of commit certificates retransmitted during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val discardedWrongEpochRetransmissionResponseMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "discarded-wrong-epoch-retransmission-responses",
          summary = "Discarded retransmission response messages",
          description =
            "Discarded retransmission response messages for epoch different than current one",
          qualification = MetricQualification.Traffic,
        )
      )

      val discardedRateLimitedRetransmissionRequestMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "discarded-rate-limited-retransmission-requests",
          summary = "Discarded rate limited retransmission requests",
          description = "Discarded retransmission requests messages due to rate limiting",
          qualification = MetricQualification.Traffic,
        )
      )
    }

    // Private constructor to avoid being instantiated multiple times by accident
    final class VotesMetrics private[BftOrderingMetrics] {

      object labels {
        val VotingSequencer: String = "voting-sequencer"
        val Epoch: String = "epoch"
      }

      private val prepareGauges = mutable.Map[BftNodeId, Gauge[Double]]()
      private val commitGauges = mutable.Map[BftNodeId, Gauge[Double]]()

      val discardedRepeatedMessageMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "discarded-messages",
          summary = "Discarded messages",
          description =
            "Discarded network messages received during an epoch, either due to being repeated (too many retransmissions), invalid or from a stale view",
          qualification = MetricQualification.Traffic,
        )
      )

      def prepareVotesPercent(node: BftNodeId): Gauge[Double] =
        getOrElseUpdateGauge(
          prepareGauges,
          node,
          "prepare-votes-percent",
          "Block vote % during prepare",
          "Percentage of BFT sequencers that voted for a block in the PBFT prepare stage.",
        )

      def commitVotesPercent(node: BftNodeId): Gauge[Double] =
        getOrElseUpdateGauge(
          commitGauges,
          node,
          "commit-votes-percent",
          "Block vote % during commit",
          "Percentage of BFT sequencers that voted for a block in the PBFT commit stage.",
        )

      def cleanupVoteGauges(keepOnly: Set[BftNodeId]): Unit =
        blocking {
          synchronized {
            keepOnlyGaugesFor(prepareGauges, keepOnly)
            keepOnlyGaugesFor(commitGauges, keepOnly)
          }
        }

      private def getOrElseUpdateGauge(
          gauges: mutable.Map[BftNodeId, Gauge[Double]],
          node: BftNodeId,
          name: String,
          summary: String,
          description: String,
      ): Gauge[Double] = {
        val mc1 = mc.withExtraLabels(labels.VotingSequencer -> node)
        blocking {
          synchronized {
            locally {
              implicit val mc: MetricsContext = mc1
              gauges.getOrElseUpdate(
                node,
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
          gaugesMap: mutable.Map[BftNodeId, Gauge[T]],
          keepOnly: Set[BftNodeId],
      ): Unit =
        gaugesMap.view.filterKeys(!keepOnly.contains(_)).foreach { case (id, gauge) =>
          gauge.close()
          gaugesMap.remove(id).discard
        }
    }
    val votes = new VotesMetrics
    val retransmissions = new RetransmissionsMetrics
  }
  val consensus = new ConsensusMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class OutputMetrics private[BftOrderingMetrics] {

    object labels {
      object mode {
        val Key: String = "mode"

        object values {
          sealed trait ModeValue extends PrettyNameOnlyCase with Product with Serializable
          case object Consensus extends ModeValue
          case object StateTransfer extends ModeValue
        }
      }
    }

    val blockSizeBytes: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeBytes.info)

    val blockSizeRequests: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeRequests.info)

    val blockSizeBatches: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeBatches.info)

    val blockDelay: Timer =
      openTelemetryMetricsFactory.timer(histograms.output.blockDelay.info)
  }
  val output = new OutputMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class TopologyMetrics private[BftOrderingMetrics] {
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
  val topology = new TopologyMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class P2PMetrics private[BftOrderingMetrics] {
    private val p2pPrefix = histograms.p2p.p2pPrefix

    // Private constructor to avoid being instantiated multiple times by accident
    final class ConnectionsMetrics private[P2PMetrics] {
      private val prefix = p2pPrefix :+ "connections"

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
    val connections = new ConnectionsMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    final class SendMetrics private[P2PMetrics] {
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

      val sendsRetried: Counter = openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "sends-retried",
          summary = "P2P sends retried",
          description =
            "Total P2P network sends retried after a delay due to missing connectivity.",
          qualification = MetricQualification.Latency,
        )
      )

      val networkWriteLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.send.networkWriteLatency.info)

      val grpcLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.send.grpcLatency.info)
    }
    val send = new SendMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    final class ReceiveMetrics private[P2PMetrics] {
      private val prefix = histograms.p2p.receive.prefix

      object labels {
        val SourceSequencer: String = "source-sequencer"

        object source {
          val Key: String = "targetModule"

          object values {
            sealed trait SourceValue extends PrettyNameOnlyCase
            case object SourceParsingFailed extends SourceValue
            case class Empty(from: BftNodeId) extends SourceValue
            case class Availability(from: BftNodeId) extends SourceValue
            case class Consensus(from: BftNodeId) extends SourceValue
            case class Retransmissions(from: BftNodeId) extends SourceValue
            case class StateTransfer(from: BftNodeId) extends SourceValue
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
    val receive = new ReceiveMetrics
  }
  val p2p = new P2PMetrics
}

object BftOrderingMetrics {
  val Prefix: MetricName = MetricName("bftordering")
}
