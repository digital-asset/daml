// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Counter, Meter, Timer}

import com.codahale.metrics.{MetricRegistry}

class CommandMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  val validation: Timer = timer(prefix :+ "validation")
  val submissions: Timer = timer(prefix :+ "submissions")
  val submissionsRunning: Meter = meter(prefix :+ "submissions_running")

  val failedCommandInterpretations: Meter = meter(prefix :+ "failed_command_interpretations")
  val delayedSubmissions: Meter = meter(prefix :+ "delayed_submissions")
  val validSubmissions: Meter = meter(prefix :+ "valid_submissions")

  val inputBufferLength: Counter = counter(prefix :+ "input_buffer_length")
  val inputBufferCapacity: Counter = counter(prefix :+ "input_buffer_capacity")
  val inputBufferDelay: Timer = timer(prefix :+ "input_buffer_delay")
  val maxInFlightLength: Counter = counter(prefix :+ "max_in_flight_length")
  val maxInFlightCapacity: Counter = counter(prefix :+ "max_in_flight_capacity")
}
