// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.{CacheMetrics, HealthMetrics}
import com.daml.metrics.api.MetricHandle.{
  Counter,
  LabeledMetricsFactory,
  MetricsFactory,
  Timer,
}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardMetricsFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryMetricsFactory
import com.daml.metrics.http.{DamlHttpMetrics, DamlWebSocketMetrics}
import io.opentelemetry.api.GlobalOpenTelemetry

object HttpJsonApiMetrics {
  lazy val ForTesting =
    new HttpJsonApiMetrics(
      new DropwizardMetricsFactory(new MetricRegistry),
      new OpenTelemetryMetricsFactory(GlobalOpenTelemetry.getMeter("test")),
    )

  final val ComponentName = "json_api"
}

class HttpJsonApiMetrics(
    defaultMetricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  import HttpJsonApiMetrics._

  val prefix: MetricName = MetricName.Daml :+ "http_json_api"

  object Db {
    val prefix: MetricName = HttpJsonApiMetrics.this.prefix :+ "db"

    val fetchByIdFetch: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_id_fetch")
    val fetchByIdQuery: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_id_query")
    val fetchByKeyFetch: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_key_fetch")
    val fetchByKeyQuery: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_key_query")
    val searchFetch: Timer = defaultMetricsFactory.timer(prefix :+ "search_fetch")
    val searchQuery: Timer = defaultMetricsFactory.timer(prefix :+ "search_query")
  }

  val surrogateTemplateIdCache =
    new CacheMetrics(prefix :+ "surrogate_tpid_cache", defaultMetricsFactory)

  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "incoming_json_parsing_and_validation_timing")
  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "response_creation_timing")
  // Meters how long a find by contract key database operation takes
  val dbFindByContractKey: Timer =
    defaultMetricsFactory.timer(prefix :+ "db_find_by_contract_key_timing")
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Timer =
    defaultMetricsFactory.timer(prefix :+ "db_find_by_contract_id_timing")
  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "command_submission_ledger_timing")
  // Meters http requests throughput
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter =
    defaultMetricsFactory.counter(prefix :+ "websocket_request_count")

  val http = new DamlHttpMetrics(labeledMetricsFactory, ComponentName)
  val websocket = new DamlWebSocketMetrics(labeledMetricsFactory, ComponentName)

  val health = new HealthMetrics(labeledMetricsFactory)
}
