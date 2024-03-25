// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, Timer}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

class DatabaseMetrics(
    val name: String,
    val factory: LabeledMetricsFactory,
) {

  private val dbPrefix: MetricName = MetricName.Daml :+ "db"
  private implicit val mc: MetricsContext = MetricsContext("name" -> name)

  @MetricDoc.Tag(
    summary = "The time needed to acquire a connection to the database.",
    description = """SQL statements are run in a dedicated executor. This metric measures the time
                    |it takes between creating the SQL statement corresponding to the <operation>
                    |and the point when it starts running on the dedicated executor.""",
    qualification = Debug,
    labelsWithDescription = Map(
      "name" -> "The operation/pool for which the metric is registered."
    ),
  )
  val waitTimer: Timer = factory.timer(dbPrefix :+ "wait")

  @MetricDoc.Tag(
    summary = "The time needed to run the SQL query and read the result.",
    description = """This metric encompasses the time measured by `query` and `commit` metrics.
                    |Additionally it includes the time needed to obtain the DB connection,
                    |optionally roll it back and close the connection at the end.""",
    qualification = Debug,
    labelsWithDescription = Map(
      "name" -> "The operation/pool for which the metric is registered."
    ),
  )
  val executionTimer: Timer = factory.timer(dbPrefix :+ "exec")

  @MetricDoc.Tag(
    summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
    description = """Some index database queries that target contracts and transactions involve a
                    |Daml-LF translation step. For such queries this metric stands for the time it
                    |takes to turn the serialized Daml-LF values into in-memory representation.""",
    qualification = Debug,
    labelsWithDescription = Map(
      "name" -> "The operation/pool for which the metric is registered."
    ),
  )
  val translationTimer: Timer = factory.timer(dbPrefix :+ "translation")

  @MetricDoc.Tag(
    summary = "The time needed to decompress the SQL query result.",
    description = """Some index database queries that target contracts involve a decompression
                    |step. For such queries this metric represents the time it takes to decompress
                    |contract arguments retrieved from the database.""",
    qualification = Debug,
    labelsWithDescription = Map(
      "name" -> "The operation/pool for which the metric is registered."
    ),
  )
  val compressionTimer: Timer = factory.timer(dbPrefix :+ "compression")

  @MetricDoc.Tag(
    summary = "The time needed to perform the SQL query commit.",
    description = """This metric measures the time it takes to commit an SQL transaction relating
                    |to the <operation>. It roughly corresponds to calling `commit()` on a DB
                    |connection.""",
    qualification = Debug,
    labelsWithDescription = Map(
      "name" -> "The operation/pool for which the metric is registered."
    ),
  )
  val commitTimer: Timer = factory.timer(dbPrefix :+ "commit")

  @MetricDoc.Tag(
    summary = "The time needed to run the SQL query.",
    description = """This metric measures the time it takes to execute a block of code (on a
                    |dedicated executor) related to the <operation> that can issue multiple SQL
                    |statements such that all run in a single DB transaction (either committed or
                    |aborted).""",
    qualification = Debug,
    labelsWithDescription = Map(
      "name" -> "The operation/pool for which the metric is registered."
    ),
  )
  val queryTimer: Timer = factory.timer(dbPrefix :+ "query")
}

object DatabaseMetrics {

  def ForTesting(metricsName: String): DatabaseMetrics =
    new DatabaseMetrics(
      name = metricsName,
      NoOpMetricsFactory,
    )
}
