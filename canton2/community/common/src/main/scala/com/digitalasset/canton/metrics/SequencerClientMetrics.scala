// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.{Debug, Saturation}
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Timer}
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}
import com.digitalasset.canton.metrics.MetricHandle.MetricsFactory

import scala.annotation.nowarn

class SequencerClientMetrics(
    basePrefix: MetricName,
    @nowarn("cat=deprecation") val metricsFactory: MetricsFactory,
) {
  val prefix: MetricName = basePrefix :+ "sequencer-client"

  @MetricDoc.Tag(
    summary = "Timer monitoring time and rate of sequentially handling the event application logic",
    description = """All events are received sequentially. This handler records the
        |the rate and time it takes the application (participant or domain) to handle the events.""",
    qualification = Debug,
  )
  val applicationHandle: Timer = metricsFactory.timer(prefix :+ "application-handle")

  @MetricDoc.Tag(
    summary = "Timer monitoring time and rate of entire event handling",
    description =
      """Most event handling cost should come from the application-handle. This timer measures
        |the full time (which should just be marginally more than the application handle.""",
    qualification = Debug,
  )
  val processingTime: Timer = metricsFactory.timer(prefix :+ "event-handle")

  @MetricDoc.Tag(
    summary = "The delay on the event processing",
    description = """Every message received from the sequencer carries a timestamp that was assigned
        |by the sequencer when it sequenced the message. This timestamp is called the sequencing timestamp.
        |The component receiving the message on the participant, mediator or topology manager side, is the sequencer client.
        |Upon receiving the message, the sequencer client compares the time difference between the
        |sequencing time and the computers local clock and exposes this difference as the given metric.
        |The difference will include the clock-skew and the processing latency between assigning the timestamp on the
        |sequencer and receiving the message by the recipient.
        |If the difference is large compared to the usual latencies and if clock skew can be ruled out, then
        |it means that the node is still trying to catch up with events that were sequenced by the
        |sequencer a while ago. This can happen after having been offline for a while or if the node is
        |too slow to keep up with the messaging load.""",
    qualification = Debug,
  )
  val delay: Gauge[Long] = metricsFactory.gauge(prefix :+ "delay", 0L)(MetricsContext.Empty)

  object handler {
    val prefix: MetricName = SequencerClientMetrics.this.prefix :+ "handler"

    @MetricDoc.Tag(
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
      qualification = Saturation,
    )
    val actualInFlightEventBatches: Counter =
      metricsFactory.counter(prefix :+ "actual-in-flight-event-batches")

    @MetricDoc.Tag(
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
      qualification = Saturation,
    )
    val maxInFlightEventBatches: Gauge[Int] =
      metricsFactory.gauge(prefix :+ "max-in-flight-event-batches", 0)(MetricsContext.Empty)
  }

  object submissions {
    val prefix: MetricName = SequencerClientMetrics.this.prefix :+ "submissions"

    @MetricDoc.Tag(
      summary =
        "Number of sequencer send requests we have that are waiting for an outcome or timeout",
      description = """Incremented on every successful send to the sequencer.
          |Decremented when the event or an error is sequenced, or when the max-sequencing-time has elapsed.""",
      qualification = Debug,
    )
    val inFlight: Counter = metricsFactory.counter(prefix :+ "in-flight")

    @MetricDoc.Tag(
      summary = "Rate and timings of send requests to the sequencer",
      description =
        """Provides a rate and time of how long it takes for send requests to be accepted by the sequencer.
          |Note that this is just for the request to be made and not for the requested event to actually be sequenced.
          |""",
      qualification = Debug,
    )
    val sends: Timer = metricsFactory.timer(prefix :+ "sends")

    @MetricDoc.Tag(
      summary = "Rate and timings of sequencing requests",
      description =
        """This timer is started when a submission is made to the sequencer and then completed when a corresponding event
          |is witnessed from the sequencer, so will encompass the entire duration for the sequencer to sequence the
          |request. If the request does not result in an event no timing will be recorded.
          |""",
      qualification = Debug,
    )
    val sequencingTime: Timer = metricsFactory.timer(prefix :+ "sequencing")

    @MetricDoc.Tag(
      summary = "Count of send requests which receive an overloaded response",
      description =
        "Counter that is incremented if a send request receives an overloaded response from the sequencer.",
      qualification = Debug,
    )
    val overloaded: Counter = metricsFactory.counter(prefix :+ "overloaded")

    @MetricDoc.Tag(
      summary = "Count of send requests that did not cause an event to be sequenced",
      description = """Counter of send requests we did not witness a corresponding event to be sequenced by the
                      |supplied max-sequencing-time. There could be many reasons for this happening: the request may
                      |have been lost before reaching the sequencer, the sequencer may be at capacity and the
                      |the max-sequencing-time was exceeded by the time the request was processed, or the supplied
                      |max-sequencing-time may just be too small for the sequencer to be able to sequence the request.""",
      qualification = Debug,
    )
    val dropped: Counter = metricsFactory.counter(prefix :+ "dropped")
  }
}
