// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricDoc.MetricQualification.Debug
import com.daml.metrics.MetricHandle.{Counter, Meter, Timer}

class InstrumentedExecutorServiceForDocs(name: MetricName) {

  @MetricDoc.GroupTag(
    representative = "daml.parallel_indexer.<stage>.executor.submitted",
  )
  @MetricDoc.Tag(
    summary = "The number of tasks submitted to an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val submitted: Meter = Meter(name :+ "submitted", null)

  @MetricDoc.GroupTag(
    representative = "daml.parallel_indexer.<stage>.executor.running",
  )
  @MetricDoc.Tag(
    summary = "The number of tasks running in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val running: Counter = Counter(name :+ "running", null)

  @MetricDoc.GroupTag(
    representative = "daml.parallel_indexer.<stage>.executor.completed",
  )
  @MetricDoc.Tag(
    summary = "The number of tasks completed in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val completed: Meter = Meter(name :+ "completed", null)

  @MetricDoc.GroupTag(
    representative = "daml.parallel_indexer.<stage>.executor.idle",
  )
  @MetricDoc.Tag(
    summary = "The time that a task is idle in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val idle: Timer = Timer(name :+ "idle", null)

  @MetricDoc.GroupTag(
    representative = "daml.parallel_indexer.<stage>.executor.duration",
  )
  @MetricDoc.Tag(
    summary = "The duration of a task is running in an instrumented executor.",
    description = """Thread pools within the ledger api server are instrumented using the
                    |dropwizard's InstrumentedExecutorService:
                    |https://www.javadoc.io/doc/io.dropwizard.metrics/metrics-core/latest/com/codahale/metrics/InstrumentedExecutorService.html""",
    qualification = Debug,
  )
  val duration: Timer = Timer(name :+ "duration", null)
}
