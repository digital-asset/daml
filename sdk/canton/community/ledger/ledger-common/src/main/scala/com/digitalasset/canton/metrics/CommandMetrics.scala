// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}

class CommandMetrics(
    prefix: MetricName,
    factory: LabeledMetricsFactory,
) {

  import com.daml.metrics.api.MetricsContext.Implicits.empty

  val validation: Timer = factory.timer(
    MetricInfo(
      prefix :+ "validation",
      summary = "The time to validate a Daml command.",
      description = """The time to validate a submitted Daml command before is fed to the
                    |interpreter.""",
      qualification = MetricQualification.Debug,
    )
  )

  val reassignmentValidation: Timer = factory.timer(
    MetricInfo(
      prefix :+ "reassignment_validation",
      summary = "The time to validate a reassignment command.",
      description = """The time to validate a submitted Daml command before is fed to the
                    |interpreter.""",
      qualification = MetricQualification.Debug,
    )
  )

  val submissions: Timer = factory.timer(
    MetricInfo(
      prefix :+ "submissions",
      summary = "The time to fully process a Daml command.",
      description = """The time to validate and interpret a command before it is handed over to the
                    |synchronization services to be finalized (either committed or rejected).""",
      qualification = MetricQualification.Latency,
    )
  )

  val submissionsRunning: Counter = factory.counter(
    MetricInfo(
      prefix :+ "submissions_running",
      summary =
        "The number of the Daml commands that are currently being handled by the ledger api server.",
      description =
        """The number of the Daml commands that are currently being handled by the ledger
                    |api server (including validation, interpretation, and handing the transaction
                    |over to the synchronization services).""",
      qualification = MetricQualification.Saturation,
    )
  )

  val failedCommandInterpretations: Meter =
    factory.meter(
      MetricInfo(
        prefix :+ "failed_command_interpretations",
        summary = "The number of Daml commands that failed in interpretation.",
        description = """The number of Daml commands that have been rejected by the interpreter
                      |(e.g. badly authorized action).""",
        qualification = MetricQualification.Errors,
      )
    )

  val delayedSubmissions: Meter = factory.meter(
    MetricInfo(
      prefix :+ "delayed_submissions",
      summary = "The number of the delayed Daml commands.",
      description = """The number of Daml commands that have been delayed internally because they
                    |have been evaluated to require the ledger time further in the future than the
                    |expected latency.""",
      qualification = MetricQualification.Debug,
    )
  )

  val validSubmissions: Meter = factory.meter(
    MetricInfo(
      prefix :+ "valid_submissions",
      summary = "The total number of the valid Daml commands.",
      description = """The total number of the Daml commands that have passed validation and were
                    |sent to interpretation in this ledger api server process.""",
      qualification = MetricQualification.Debug,
    )
  )

  val maxInFlightLength: Counter = factory.counter(
    MetricInfo(
      prefix :+ "max_in_flight_length",
      summary = "The number of the Daml commands awaiting completion.",
      description =
        "The number of the currently Daml commands awaiting completion in the Command Service.",
      qualification = MetricQualification.Debug,
    )
  )

  val maxInFlightCapacity: Counter = factory.counter(
    MetricInfo(
      prefix :+ "max_in_flight_capacity",
      summary = "The maximum number of Daml commands that can await completion.",
      description =
        "The maximum number of Daml commands that can await completion in the Command Service.",
      qualification = MetricQualification.Debug,
    )
  )
}
