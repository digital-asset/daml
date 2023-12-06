// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory, MetricsFactory, Timer}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.http.{DamlHttpMetrics, DamlWebSocketMetrics}
import com.daml.metrics.{CacheMetrics, HealthMetrics}

import scala.annotation.nowarn

object HttpApiMetrics {
  lazy val ForTesting =
    new HttpApiMetrics(
      NoOpMetricsFactory,
      NoOpMetricsFactory,
    )

  final val ComponentName = "json_api"
}

// TODO(#13303) Clean up metrics
class HttpApiMetrics(
    @nowarn @deprecated defaultMetricsFactory: MetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  import HttpApiMetrics._

  val prefix: MetricName = MetricName.Daml :+ "http_json_api"

  @nowarn
  object Db {
    val prefix: MetricName = HttpApiMetrics.this.prefix :+ "db"

    val fetchByIdFetch: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_id_fetch")
    val fetchByIdQuery: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_id_query")
    val fetchByKeyFetch: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_key_fetch")
    val fetchByKeyQuery: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_by_key_query")
    val searchFetch: Timer = defaultMetricsFactory.timer(prefix :+ "search_fetch")
    val searchQuery: Timer = defaultMetricsFactory.timer(prefix :+ "search_query")
  }

  val surrogateTemplateIdCache =
    new CacheMetrics(prefix :+ "surrogate_tpid_cache", labeledMetricsFactory)

  // Meters how long parsing and decoding of an incoming json payload takes
  @nowarn
  val incomingJsonParsingAndValidationTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "incoming_json_parsing_and_validation_timing")
  // Meters how long the construction of the response json payload takes
  @nowarn
  val responseCreationTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "response_creation_timing")
  // Meters how long a find by contract key database operation takes
  @nowarn
  val dbFindByContractKey: Timer =
    defaultMetricsFactory.timer(prefix :+ "db_find_by_contract_key_timing")
  // Meters how long a find by contract id database operation takes
  @nowarn
  val dbFindByContractId: Timer =
    defaultMetricsFactory.timer(prefix :+ "db_find_by_contract_id_timing")
  // Meters how long processing of the command submission request takes on the ledger
  @nowarn
  val commandSubmissionLedgerTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "command_submission_ledger_timing")
  // Meters http requests throughput
  // Meters how many websocket connections are currently active
  @nowarn
  val websocketRequestCounter: Counter =
    defaultMetricsFactory.counter(prefix :+ "websocket_request_count")

  val http = new DamlHttpMetrics(labeledMetricsFactory, ComponentName)
  val websocket = new DamlWebSocketMetrics(labeledMetricsFactory, ComponentName)

  val health = new HealthMetrics(labeledMetricsFactory)
}
