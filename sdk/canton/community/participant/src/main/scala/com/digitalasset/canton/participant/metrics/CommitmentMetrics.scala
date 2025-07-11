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
import com.digitalasset.canton.topology.ParticipantId

import scala.collection.concurrent.TrieMap

class CommitmentHistograms(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "commitments"
  private[metrics] val compute = Item(
    prefix :+ "compute",
    summary = "Measures the time that the participant node spends computing commitments.",
    description =
      """Participant nodes compute bilateral commitments at regular intervals, i.e., reconciliation intervals.
        |This metric exposes the time spent on each computation in milliseconds. There are two cases that the
        |operator should pay attention to. First, fluctuations in this value are expected if the number of
        |counter-participants or common stakeholder groups changes. However, changes with no apparent reason
        |could indicate a bug and the operator should monitor closely. Second, it is a cause of concern if the
        |value starts approaching or is greater than the reconciliation interval: The participant will
        |perpetually lag behind, because it needs to compute commitments more frequently than it can manage.
        |The operator should consider asking the synchronizer operator to increase the reconciliation interval
        |if the increase in commitment computation is expected, or otherwise investigate the cause.""",
    qualification = MetricQualification.Debug,
  )
}

class CommitmentMetrics private[metrics] (
    synchronizerAlias: SynchronizerAlias,
    histograms: CommitmentHistograms,
    metricsFactory: LabeledMetricsFactory,
) {
  import MetricsContext.Implicits.empty
  private val prefix = histograms.prefix

  val compute: Timer = metricsFactory.timer(histograms.compute.info)

  val sequencingTime: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "sequencing-time",
        summary =
          "Measures the time between the end of a commitment period, and the time when the sequencer observes " +
            "the corresponding commitment.",
        description = """Participant nodes compute bilateral commitments at regular intervals. After a participant
                        |computes a commitment, it sends it for sequencing. The time between the end of a
                        |commitment interval and sequencing is measured in milliseconds. Because commitment computation
                        |is comprised within the measured time, the value is always greater than the
                        |`daml.participant.sync.commitments.compute` metric. The operator should pay attention to
                        |fluctuations of this value. An increase can be expected, e.g., because the computation time
                        |increases. However, a value increase can be a cause of concern, because it can indicate that
                        |the participant is lagging behind in processing messages and computing commitments, which is
                        |accompanied by `ACS_COMMITMENT_DEGRADATION` warnings in the participant logs. An increase can
                        |also indicate that the sequencer is slow in sequencing the commitment messages. The operator
                        |should cross-correlate with sequencing metrics such as `daml.sequencer-client.submissions.sequencing`
                        |and `daml.sequencer-client.handler.delay.` In this case, the operator should consider changing
                        |the preferred sequencer configuration.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  private val commonCounterParticipantLatencyDescription: String =
    """The metric exposes the highest latency of a counter-participant, measured by subtracting the highest known
    |counter-participant latency from the most recent period processed by the participant. A counter-participant has to
    |send a commitment at least once in order to appear here. The operator of a participant can configure a default
    |threshold per synchronizer that the participant connects to. The smaller the threshold, the more sensitive the
    |metric is to even small delays in receiving commitments from counter-participants. For example, for a threshold of
    |5 intervals and a reconciliation interval of 1 minute, the metric measures the latency of counter-participants
    |that have sent no commitments for periods covering the last 5 minutes observed by the participant."""

  private val defaultCounterParticipantLatencyDescription: String =
    """Participant nodes compute bilateral commitments at regular intervals and send them. This metric
    |is the default indicator of a counter-participant being slow.""" + commonCounterParticipantLatencyDescription

  private val distinguishedCounterParticipantLatencyDescription: String = """Participant nodes compute bilateral commitments at regular intervals and send them. This metric
                                                                      |indicates that a distinguished counter-participant is slow, i.e., the participant cannot confirm that
                                                                      |its state is the same with that of a counter-participant with whom the operator has an important
                                                                      |business relation.""" + commonCounterParticipantLatencyDescription

  val largestDistinguishedCounterParticipantLatency: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ s"${synchronizerAlias.unquoted}.largest-distinguished-counter-participant-latency",
        summary =
          "The highest latency in micros for commitments outstanding from distinguished counter-participants " +
            "for more than a threshold-number of reconciliation intervals.",
        description = distinguishedCounterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val largestCounterParticipantLatency: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ s"${synchronizerAlias.unquoted}.largest-counter-participant-latency",
        summary =
          "The highest latency in micros for commitments outstanding from counter-participants for more than a " +
            "threshold-number of reconciliation intervals.",
        description = defaultCounterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  private val individuallyMonitoredCounterParticipantLatencies
      : TrieMap[ParticipantId, Eval[Gauge[Long]]] =
    TrieMap.empty[ParticipantId, Eval[Gauge[Long]]]

  def counterParticipantLatency(participant: ParticipantId): Gauge[Long] = {
    def createMonitoredParticipant: Gauge[Long] = metricsFactory.gauge(
      MetricInfo(
        prefix :+ s"${synchronizerAlias.unquoted}.counter-participant-latency.${participant.uid.identifier}",
        summary =
          "The latency of commitments outstanding from the given counter-participants measured in micros.",
        description =
          """Participant nodes compute bilateral commitments at regular intervals and send them. This metric shows
          |the latency of the given counter-participant, measured by subtracting the counter-participant latency from
          |the most recent period processed by the participant. The counter-participant has to send a commitment at
          |least once in order to appear here.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

    individuallyMonitoredCounterParticipantLatencies
      .getOrElseUpdate(participant, Eval.later(createMonitoredParticipant))
      .value
  }

  val catchupModeEnabled: Meter = metricsFactory.meter(
    MetricInfo(
      prefix :+ "catchup-mode-enabled",
      summary =
        "Measures how many times the commitment processor catch-up mode has been triggered.",
      description =
        """Participant nodes compute bilateral commitments at regular intervals. This metric exposes how often the
          |catch-up mode has been activated. The catch-up mode is triggered according to catch-up config and happens
          |if the participant lags behind on computation. A healthy value is 0. An increasing value indicates
          |intermittent periods when a participant alternates between healthy and struggling to keep up with commitment
          |computation. However, we do not see a constantly increasing value for a participant that is consistently
          |behind commitment computation because, once catch-up mode is activated, the participant remains in catch-up
          |mode until it has completely caught up, and only triggers the metric once. In order to troubleshoot non-zero
          |values, the operator should cross-correlate this value with the
          |`daml.participant.sync.commitments.compute` metric.""",
      qualification = MetricQualification.Debug,
    )
  )

}
