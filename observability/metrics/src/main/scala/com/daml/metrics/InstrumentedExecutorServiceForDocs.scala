// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, Meter, Timer}
import com.daml.metrics.api.dropwizard.{DropwizardCounter, DropwizardMeter, DropwizardTimer}
import com.daml.metrics.api.{MetricDoc, MetricName}

class InstrumentedExecutorServiceForDocs(name: MetricName) {

  @MetricDoc.Tag(
    summary = "The number of tasks submitted to an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val submitted: Meter = DropwizardMeter(name :+ "submitted", null)

  @MetricDoc.Tag(
    summary = "The number of tasks running in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val running: Counter = DropwizardCounter(name :+ "running", null)

  @MetricDoc.Tag(
    summary = "The number of tasks completed in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val completed: Meter = DropwizardMeter(name :+ "completed", null)

  @MetricDoc.Tag(
    summary = "The time that a task is idle in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val idle: Timer = DropwizardTimer(name :+ "idle", null)

  @MetricDoc.Tag(
    summary = "The duration of a task is running in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val duration: Timer = DropwizardTimer(name :+ "duration", null)
}
