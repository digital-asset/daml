// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricQualification.{Debug, Latency}
import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricDoc, MetricName}

class CommandMetrics(
    prefix: MetricName,
    factory: LabeledMetricsFactory,
) {

  import com.daml.metrics.api.MetricsContext.Implicits.empty

  @MetricDoc.Tag(
    summary = "The time to validate a Daml command.",
    description = """The time to validate a submitted Daml command before is fed to the
                    |interpreter.""",
    qualification = Debug,
  )
  val validation: Timer = factory.timer(prefix :+ "validation")

  @MetricDoc.Tag(
    summary = "The time to validate a reassignment command.",
    description = """The time to validate a submitted Daml command before is fed to the
                    |interpreter.""",
    qualification = Debug,
  )
  val reassignmentValidation: Timer = factory.timer(prefix :+ "reassignment_validation")

  @MetricDoc.Tag(
    summary = "The time to fully process a Daml command.",
    description = """The time to validate and interpret a command before it is handed over to the
                    |synchronization services to be finalized (either committed or rejected).""",
    qualification = Latency,
  )
  val submissions: Timer = factory.timer(prefix :+ "submissions")

  @MetricDoc.Tag(
    summary =
      "The number of the Daml commands that are currently being handled by the ledger api server.",
    description = """The number of the Daml commands that are currently being handled by the ledger
                    |api server (including validation, interpretation, and handing the transaction
                    |over to the synchronization services).""",
    qualification = Debug,
  )
  val submissionsRunning: Counter = factory.counter(prefix :+ "submissions_running")

  @MetricDoc.Tag(
    summary = "The number of Daml commands that failed in interpretation.",
    description = """The number of Daml commands that have been rejected by the interpreter
                    |(e.g. badly authorized action).""",
    qualification = Debug,
  )
  val failedCommandInterpretations: Meter =
    factory.meter(prefix :+ "failed_command_interpretations")

  @MetricDoc.Tag(
    summary = "The number of the delayed Daml commands.",
    description = """The number of Daml commands that have been delayed internally because they
                    |have been evaluated to require the ledger time further in the future than the
                    |expected latency.""",
    qualification = Debug,
  )
  val delayedSubmissions: Meter = factory.meter(prefix :+ "delayed_submissions")

  @MetricDoc.Tag(
    summary = "The total number of the valid Daml commands.",
    description = """The total number of the Daml commands that have passed validation and were
                    |sent to interpretation in this ledger api server process.""",
    qualification = Debug,
  )
  val validSubmissions: Meter = factory.meter(prefix :+ "valid_submissions")

  @MetricDoc.Tag(
    summary = "The number of the Daml commands awaiting completion.",
    description =
      "The number of the currently Daml commands awaiting completion in the Command Service.",
    qualification = Debug,
  )
  val maxInFlightLength: Counter = factory.counter(prefix :+ "max_in_flight_length")

  @MetricDoc.Tag(
    summary = "The maximum number of Daml commands that can await completion.",
    description =
      "The maximum number of Daml commands that can await completion in the Command Service.",
    qualification = Debug,
  )
  val maxInFlightCapacity: Counter = factory.counter(prefix :+ "max_in_flight_capacity")
}
