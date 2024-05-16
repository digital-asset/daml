// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.MetricQualification.Debug
import com.daml.metrics.api.{MetricName, MetricQualification}
import com.digitalasset.canton.domain.metrics.{MediatorHistograms, SequencerHistograms}
import com.digitalasset.canton.metrics.HistogramInventory.Item
import com.digitalasset.canton.participant.metrics.ParticipantHistograms

// TODO(#17917) move upstream
class DamlHttpHistograms(implicit
    inventory: HistogramInventory
) {
  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  val latency: Item =
    Item(
      httpMetricsPrefix :+ "requests",
      "The duration of the HTTP requests.",
      MetricQualification.Debug,
    )
  val requestsPayloadBytes: Item =
    Item(
      httpMetricsPrefix :+ "requests" :+ "payload" :+ Histogram.Bytes,
      "Distribution of the sizes of payloads received in HTTP requests.",
      MetricQualification.Debug,
    )
  val responsesPayloadBytes: Item =
    Item(
      httpMetricsPrefix :+ "responses" :+ "payload" :+ Histogram.Bytes,
      "Distribution of the sizes of payloads sent in HTTP responses.",
      MetricQualification.Debug,
    )
}

// TODO(#17917) move upstream
class DamlWebSocketsHistograms(implicit
    inventory: HistogramInventory
) {
  private val httpMetricsPrefix = MetricName.Daml :+ "http"

  val messagesReceivedBytes: Item =
    Item(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "received" :+ Histogram.Bytes,
      "Distribution of the size of received WebSocket messages.",
      MetricQualification.Debug,
    )

  val messagesSentBytes: Item =
    Item(
      httpMetricsPrefix :+ "websocket" :+ "messages" :+ "sent" :+ Histogram.Bytes,
      "Distribution of the size of sent WebSocket messages.",
      MetricQualification.Debug,
    )

}

// TODO(#17917) move to upstream
class DatabaseMetricsHistograms(implicit
    inventory: HistogramInventory
) {

  private val dbPrefix: MetricName = MetricName.Daml :+ "db"
  private lazy val labelsWithDescription = Map(
    "name" -> "The operation/pool for which the metric is registered."
  )
  val waitTimer: Item = Item(
    dbPrefix :+ "wait",
    summary = "The time needed to acquire a connection to the database.",
    description = """SQL statements are run in a dedicated executor. This metric measures the time
                      |it takes between creating the SQL statement corresponding to the <operation>
                      |and the point when it starts running on the dedicated executor.""",
    qualification = Debug,
    labelsWithDescription = labelsWithDescription,
  )

  val executionTimer: Item = Item(
    dbPrefix :+ "exec",
    summary = "The time needed to run the SQL query and read the result.",
    description = """This metric encompasses the time measured by `query` and `commit` metrics.
                      |Additionally it includes the time needed to obtain the DB connection,
                      |optionally roll it back and close the connection at the end.""",
    qualification = Debug,
    labelsWithDescription = labelsWithDescription,
  )

  val translationTimer: Item = Item(
    dbPrefix :+ "translation",
    summary = "The time needed to turn serialized Daml-LF values into in-memory objects.",
    description = """Some index database queries that target contracts and transactions involve a
                      |Daml-LF translation step. For such queries this metric stands for the time it
                      |takes to turn the serialized Daml-LF values into in-memory representation.""",
    qualification = Debug,
    labelsWithDescription = labelsWithDescription,
  )

  val compressionTimer: Item = Item(
    dbPrefix :+ "compression",
    summary = "The time needed to decompress the SQL query result.",
    description = """Some index database queries that target contracts involve a decompression
                      |step. For such queries this metric represents the time it takes to decompress
                      |contract arguments retrieved from the database.""",
    qualification = Debug,
    labelsWithDescription = labelsWithDescription,
  )

  val commitTimer: Item = Item(
    dbPrefix :+ "commit",
    summary = "The time needed to perform the SQL query commit.",
    description = """This metric measures the time it takes to commit an SQL transaction relating
                      |to the <operation>. It roughly corresponds to calling `commit()` on a DB
                      |connection.""",
    qualification = Debug,
    labelsWithDescription = labelsWithDescription,
  )

  val queryTimer: Item = Item(
    dbPrefix :+ "query",
    summary = "The time needed to run the SQL query.",
    description = """This metric measures the time it takes to execute a block of code (on a
                      |dedicated executor) related to the <operation> that can issue multiple SQL
                      |statements such that all run in a single DB transaction (either committed or
                      |aborted).""",
    qualification = Debug,
    labelsWithDescription = labelsWithDescription,
  )

}

/** Pre-register histogram metrics
  *
  * Open telemetry requires us to define the histogram buckets before defining the actual metric.
  * Therefore, we define the name here and ensure that the same name is known wherever the metric is used.
  */
class CantonHistograms()(implicit val inventory: HistogramInventory) {

  val prefix: MetricName = MetricName.Daml
  private[metrics] val participant: ParticipantHistograms =
    new ParticipantHistograms(prefix)
  private[metrics] val mediator: MediatorHistograms = new MediatorHistograms(prefix)
  private[metrics] val sequencer: SequencerHistograms = new SequencerHistograms(prefix)

  // TODO(#17917) move everything below to upstream
  private val _http: DamlHttpHistograms = new DamlHttpHistograms()
  private val _webSockets: DamlWebSocketsHistograms =
    new DamlWebSocketsHistograms()
  private val _grpc = new DamlGrpcServerHistograms()
  private val _db = new DatabaseMetricsHistograms()

}
