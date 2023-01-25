// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.MetricHandle.{
  Counter,
  LabeledMetricsFactory,
  Meter,
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
}

class HttpJsonApiMetrics(
    defaultMetricsFactory: MetricsFactory,
    labeledMetricsFactory: MetricsFactory with LabeledMetricsFactory,
) {
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

  // Meters how long processing of a command submission request takes
  val commandSubmissionTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "command_submission_timing")
  // Meters how long processing of a query GET request takes
  val queryAllTimer: Timer = defaultMetricsFactory.timer(prefix :+ "query_all_timing")
  // Meters how long processing of a query POST request takes
  val queryMatchingTimer: Timer = defaultMetricsFactory.timer(prefix :+ "query_matching_timing")
  // Meters how long processing of a fetch request takes
  val fetchTimer: Timer = defaultMetricsFactory.timer(prefix :+ "fetch_timing")
  // Meters how long processing of a get party/parties request takes
  val getPartyTimer: Timer = defaultMetricsFactory.timer(prefix :+ "get_party_timing")
  // Meters how long processing of a party management request takes
  val allocatePartyTimer: Timer = defaultMetricsFactory.timer(prefix :+ "allocate_party_timing")
  // Meters how long processing of a package download request takes
  val downloadPackageTimer: Timer =
    defaultMetricsFactory.timer(prefix :+ "download_package_timing")
  // Meters how long processing of a package upload request takes
  val uploadPackageTimer: Timer = defaultMetricsFactory.timer(prefix :+ "upload_package_timing")
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
  val httpRequestThroughput: Meter =
    defaultMetricsFactory.meter(prefix :+ "http_request_throughput")
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter =
    defaultMetricsFactory.counter(prefix :+ "websocket_request_count")
  // Meters command submissions throughput
  val commandSubmissionThroughput: Meter =
    defaultMetricsFactory.meter(prefix :+ "command_submission_throughput")
  // Meters package uploads throughput
  val uploadPackagesThroughput: Meter =
    defaultMetricsFactory.meter(prefix :+ "upload_packages_throughput")
  // Meters party allocation throughput
  val allocatePartyThroughput: Meter =
    defaultMetricsFactory.meter(prefix :+ "allocation_party_throughput")

  val http = new DamlHttpMetrics(labeledMetricsFactory, "json-api")
  val websocket = new DamlWebSocketMetrics(labeledMetricsFactory, "json-api")
}
