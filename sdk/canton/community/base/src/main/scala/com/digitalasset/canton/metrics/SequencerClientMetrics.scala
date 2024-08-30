// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.Eval
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.digitalasset.canton.SequencerAlias

import scala.collection.concurrent.TrieMap

class SequencerClientHistograms(basePrefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = basePrefix :+ "sequencer-client"
  private[metrics] val handlerPrefix: MetricName = prefix :+ "handler"

  private[metrics] val applicationHandle: Item = Item(
    handlerPrefix :+ "application-handle",
    summary = "Timer monitoring time and rate of sequentially handling the event application logic",
    description = """All events are received sequentially. This handler records the
                      |the rate and time it takes the application (participant or domain) to handle the events.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val submissionPrefix: MetricName = prefix :+ "submissions"

  private[metrics] val submissionSends: Item = Item(
    submissionPrefix :+ "sends",
    summary = "Rate and timings of send requests to the sequencer",
    description = """Provides a rate and time of how long it takes for send requests to be accepted by the sequencer.
          |Note that this is just for the request to be made and not for the requested event to actually be sequenced.
          |""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val submissionSequencingTime: Item = Item(
    submissionPrefix :+ "sequencing",
    summary = "Rate and timings of sequencing requests",
    description =
      """This timer is started when a submission is made to the sequencer and then completed when a corresponding event
          |is witnessed from the sequencer, so will encompass the entire duration for the sequencer to sequence the
          |request. If the request does not result in an event no timing will be recorded.
          |""",
    qualification = MetricQualification.Latency,
  )

}

class SequencerClientMetrics(
    histograms: SequencerClientHistograms,
    val metricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {

  val trafficConsumption = new TrafficConsumptionMetrics(
    prefix = histograms.prefix :+ "traffic-control",
    labeledMetricsFactory = metricsFactory,
  )

  object handler {
    private val prefix = histograms.handlerPrefix
    val numEvents: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "sequencer-events",
        summary = "Number of received events from the sequencer",
        description =
          """A participant reads events from the sequencer. This metric captures the count and rate of events.""",
        qualification = MetricQualification.Debug,
      )
    )

    val applicationHandle: Timer = metricsFactory.timer(histograms.applicationHandle.info)
    val delay: Gauge[Long] = metricsFactory.gauge(
      MetricInfo(
        prefix :+ "delay",
        summary = "The delay on the event processing in milliseconds",
        description = """Every message received from the sequencer carries a timestamp that was assigned
                      |by the sequencer when it sequenced the message. This timestamp is called the sequencing timestamp.
                      |The component receiving the message on the participant or mediator is the sequencer client,
                      |while on the block sequencer itself, it's the block update generator.
                      |Upon having received the same message from enough sequencers (as configured by the trust threshold),
                      |the sequencer client compares the time difference between the
                      |sequencing time and the computers local clock and exposes this difference as the given metric.
                      |The difference will include the clock-skew and the processing latency between assigning the timestamp on the
                      |sequencer and receiving the message by the recipient from enough sequencers.
                      |If the difference is large compared to the usual latencies, clock skew can be ruled out,
                      |and enough sequencers are not slow, then it means that the node is still trying to catch up with events that the sequencers sequenced
                      |a while ago. This can happen after having been offline for a while or if the node is
                      |too slow to keep up with the messaging load.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

    private val connectionDelayMetrics: TrieMap[SequencerAlias, Eval[Gauge[Long]]] =
      TrieMap.empty[SequencerAlias, Eval[Gauge[Long]]]

    def connectionDelay(alias: SequencerAlias): Gauge[Long] = {
      def createConnectionDelayGauge: Gauge[Long] = metricsFactory.gauge(
        MetricInfo(
          prefix :+ "delay-per-connection",
          summary =
            "The delay on receiving an event over the given sequencer connection in milliseconds",
          description = """Every message received from the sequencer carries a timestamp that was assigned
              |by the sequencers when they sequenced the message. This timestamp is called the sequencing timestamp.
              |The component receiving the message on the participant or mediator is the sequencer client.
              |Upon receiving the message, the sequencer client compares the time difference between the
              |sequencing time and the computers local clock and exposes this difference as the given metric.
              |The difference will include the clock-skew and the processing latency between assigning the timestamp on the
              |sequencer and receiving the message by the recipient.
              |If the difference is large compared to the usual latencies and if clock skew can be ruled out, then
              |it means that either the sequencer is slow in delivering the messages or the node is still trying to
              |catch up with events that the sequencers sequenced a while ago.
              |This can happen after having been offline for a while or if the node is
              |too slow to keep up with the messaging load.""",
          qualification = MetricQualification.Debug,
        ),
        0L,
      )(context.withExtraLabels("sequencer" -> alias.unwrap))

      // Two concurrent calls with the same domain alias may cause getOrElseUpdate to evaluate the new value expression twice,
      // even though only one of the results will be stored in the map.
      // Eval.later ensures that we actually create only one instance of SyncDomainMetrics in such a case
      // by delaying the creation until the getOrElseUpdate call has finished.
      connectionDelayMetrics.getOrElseUpdate(alias, Eval.later(createConnectionDelayGauge)).value
    }

    val actualInFlightEventBatches: Counter =
      metricsFactory.counter(
        MetricInfo(
          prefix :+ "actual-in-flight-event-batches",
          summary =
            "Nodes process the events from the domain's sequencer in batches. This metric tracks how many such batches are processed in parallel.",
          description = """Incoming messages are processed by a sequencer client, which combines them into batches of
                        |size up to 'event-inbox-size' before sending them to an application handler for processing. Depending on the
                        |system's configuration, the rate at which event batches are sent to the handler may be throttled to avoid
                        |overwhelming it with too many events at once.
                        |
                        |Indicators that the configured upper bound may be too low:
                        |This metric constantly is closed to the configured maximum, which is exposed via 'max-in-flight-event-batches',
                        |while the system's resources are under-utilized.
                        |Indicators that the configured upper bound may be too high:
                        |Out-of-memory errors crashing the JVM or frequent garbage collection cycles that slow down processing.
                        |
                        |The metric tracks how many of these batches have been sent to the application handler but have not yet
                        |been fully processed. This metric can help identify potential bottlenecks or issues with the application's
                        |processing of events and provide insights into the overall workload of the system.""",
          qualification = MetricQualification.Saturation,
        )
      )

    val maxInFlightEventBatches: Gauge[Int] =
      metricsFactory.gauge(
        MetricInfo(
          prefix :+ "max-in-flight-event-batches",
          summary =
            "Nodes process the events from the domain's sequencer in batches. This metric tracks the upper bound of such batches being processed in parallel.",
          description = """Incoming messages are processed by a sequencer client, which combines them into batches of
                        |size up to 'event-inbox-size' before sending them to an application handler for processing. Depending on the
                        |system's configuration, the rate at which event batches are sent to the handler may be throttled to avoid
                        |overwhelming it with too many events at once.
                        |
                        |Configured by 'maximum-in-flight-event-batches' parameter in the sequencer-client config
                        |
                        |The metric shows the configured upper limit on how many batches the application handler may process concurrently.
                        |The metric 'actual-in-flight-event-batches' tracks the actual number of currently processed batches.""",
          qualification = MetricQualification.Debug,
        ),
        0,
      )
  }

  object submissions {
    val prefix: MetricName = histograms.submissionPrefix

    val inFlight: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "in-flight",
        summary =
          "Number of sequencer send requests we have that are waiting for an outcome or timeout",
        description = """Incremented on every successful send to the sequencer.
                      |Decremented when the event or an error is sequenced, or when the max-sequencing-time has elapsed.""",
        qualification = MetricQualification.Debug,
      )
    )

    val sends: Timer = metricsFactory.timer(histograms.submissionSends.info)

    val sequencingTime: Timer = metricsFactory.timer(histograms.submissionSequencingTime.info)

    val overloaded: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "overloaded",
        summary = "Count of send requests which receive an overloaded response",
        description =
          "Counter that is incremented if a send request receives an overloaded response from the sequencer.",
        qualification = MetricQualification.Errors,
      )
    )

    val dropped: Counter = metricsFactory.counter(
      MetricInfo(
        prefix :+ "dropped",
        summary = "Count of send requests that did not cause an event to be sequenced",
        description = """Counter of send requests we did not witness a corresponding event to be sequenced by the
                      |supplied max-sequencing-time. There could be many reasons for this happening: the request may
                      |have been lost before reaching the sequencer, the sequencer may be at capacity and the
                      |the max-sequencing-time was exceeded by the time the request was processed, or the supplied
                      |max-sequencing-time may just be too small for the sequencer to be able to sequence the request.""",
        qualification = MetricQualification.Errors,
      )
    )
  }
}
