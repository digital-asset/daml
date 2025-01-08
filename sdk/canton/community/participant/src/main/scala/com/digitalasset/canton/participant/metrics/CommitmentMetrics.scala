// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import cats.Eval
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.metrics.HasDocumentedMetrics
import com.digitalasset.canton.topology.ParticipantId

import scala.collection.concurrent.TrieMap

class CommitmentHistograms(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "commitments"
  private[metrics] val compute = Item(
    prefix :+ "compute",
    summary = "Time spent on commitment computations.",
    description =
      """Participant nodes compute bilateral commitments at regular intervals. This metric
        |exposes the time spent on each computation. If the time to compute the metrics
        |starts to exceed the commitment intervals, this likely indicates a problem.""",
    qualification = MetricQualification.Debug,
  )
}

class CommitmentMetrics(
    synchronizerAlias: SynchronizerAlias,
    histograms: CommitmentHistograms,
    metricsFactory: LabeledMetricsFactory,
) extends HasDocumentedMetrics {
  import MetricsContext.Implicits.empty
  private val prefix = histograms.prefix

  val compute: Timer = metricsFactory.timer(histograms.compute.info)

  val sequencingTime: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "sequencing-time",
        summary = "Time spent in microseconds between commitment and sequencing.",
        description = """Participant nodes compute bilateral commitments at regular intervals. After a commitment
                        |has been computed it is send for sequencing. This measures the time between the end of a
                        |commitment interval and when the commitment has been sequenced. A high value indicates that
                        |the participant is lagging behind in processing messages and computing commitments or the
                        |sequencer is slow in sequencing the commitment messages.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  private val counterParticipantLatencyDescription: String = """Participant nodes compute bilateral commitments at regular intervals and transmit them. This metric
                                                                 |exposes the highest latency of a counter-participant,
                                                                 |measured by subtracting the highest known latency from the known lowest among all known counter-participants.
                                                                 |A counter-participant has to send a commitment at least once in order to appear here.
                                                                 |"""

  val largestDistinguishedCounterParticipantLatency: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ s"${synchronizerAlias.unquoted}.largest-distinguished-counter-participant-latency",
        summary =
          "The biggest latency to a currently distinguished counter-participant measured in micros.",
        description = counterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val largestCounterParticipantLatency: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ s"${synchronizerAlias.unquoted}.largest-counter-participant-latency",
        summary =
          "The biggest latency to a currently non-ignored counter-participant measured in micros.",
        description = counterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  private val monitoredCounterParticipantLatencies: TrieMap[ParticipantId, Eval[Gauge[Long]]] =
    TrieMap.empty[ParticipantId, Eval[Gauge[Long]]]

  def counterParticipantLatency(participant: ParticipantId): Gauge[Long] = {
    def createMonitoredParticipant: Gauge[Long] = metricsFactory.gauge(
      MetricInfo(
        prefix :+ s"${synchronizerAlias.unquoted}.counter-participant-latency.${participant.uid.identifier}",
        summary = "The latency to a specific counter-participant measured in micros.",
        description = counterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

    monitoredCounterParticipantLatencies
      .getOrElseUpdate(participant, Eval.later(createMonitoredParticipant))
      .value
  }

  val catchupModeEnabled: Meter = metricsFactory.meter(
    MetricInfo(
      prefix :+ "catchup-mode-enabled",
      summary = "Times the catch up mode has been activated.",
      description =
        """Participant nodes compute bilateral commitments at regular intervals. This metric
          |exposes how often catch-up mode has been activated. Catch-up mode is triggered according
          |to catch-up config and happens if the participant lags behind on computation.""",
      qualification = MetricQualification.Debug,
    )
  )

}
