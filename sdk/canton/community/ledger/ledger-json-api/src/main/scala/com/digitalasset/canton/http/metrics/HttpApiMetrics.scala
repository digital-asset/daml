// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.http.{DamlHttpMetrics, DamlWebSocketMetrics}
import com.daml.metrics.{CacheMetrics, HealthMetrics}

object HttpApiMetrics {
  lazy val ForTesting =
    new HttpApiMetrics(
      MetricName("test"),
      NoOpMetricsFactory,
      NoOpMetricsFactory,
    )

  final val ComponentName = "json_api"
}

// TODO(#13303) Clean up metrics
class HttpApiMetrics(
    parent: MetricName,
    defaultMetricsFactory: LabeledMetricsFactory,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  import HttpApiMetrics.*

  import com.daml.metrics.api.MetricsContext.Implicits.empty

  val prefix: MetricName = parent :+ "http_json_api"

  object Db {
    val prefix: MetricName = HttpApiMetrics.this.prefix :+ "db"

    val fetchByIdFetch: Timer = defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "fetch_by_id_fetch", "", MetricQualification.Debug)
    )
    val fetchByIdQuery: Timer = defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "fetch_by_id_query", "", MetricQualification.Debug)
    )
    val fetchByKeyFetch: Timer = defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "fetch_by_key_fetch", "", MetricQualification.Debug)
    )
    val fetchByKeyQuery: Timer = defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "fetch_by_key_query", "", MetricQualification.Debug)
    )
    val searchFetch: Timer = defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "search_fetch", "", MetricQualification.Debug)
    )
    val searchQuery: Timer = defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "search_query", "", MetricQualification.Debug)
    )
  }

  val surrogateTemplateIdCache =
    new CacheMetrics(prefix :+ "surrogate_tpid_cache", labeledMetricsFactory)

  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Timer =
    defaultMetricsFactory.timer(
      MetricInfo(
        prefix :+ "incoming_json_parsing_and_validation_timing",
        "",
        MetricQualification.Debug,
      )
    )
  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Timer =
    defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "response_creation_timing", "", MetricQualification.Debug)
    )
  // Meters how long a find by contract key database operation takes
  val dbFindByContractKey: Timer =
    defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "db_find_by_contract_key_timing", "", MetricQualification.Debug)
    )
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Timer =
    defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "db_find_by_contract_id_timing", "", MetricQualification.Debug)
    )
  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Timer =
    defaultMetricsFactory.timer(
      MetricInfo(prefix :+ "command_submission_ledger_timing", "", MetricQualification.Debug)
    )
  // Meters http requests throughput
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter =
    defaultMetricsFactory.counter(
      MetricInfo(prefix :+ "websocket_request_count", "", MetricQualification.Debug)
    )

  val http = new DamlHttpMetrics(labeledMetricsFactory, ComponentName)
  val websocket = new DamlWebSocketMetrics(labeledMetricsFactory, ComponentName)

  val health = new HealthMetrics(labeledMetricsFactory)
}
