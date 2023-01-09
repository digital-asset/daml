// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.metrics

import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.MetricHandle.{Counter, Factory, Meter, Timer}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.http.{DamlHttpMetrics, DamlWebSocketMetrics}

object HttpJsonApiMetrics {
  lazy val ForTesting =
    new HttpJsonApiMetrics(
      NoOpMetricsFactory
    )
}

class HttpJsonApiMetrics(
    metricsFactory: Factory
) {
  val prefix: MetricName = MetricName.Daml :+ "http_json_api"

  object Db {
    val prefix: MetricName = HttpJsonApiMetrics.this.prefix :+ "db"

    val fetchByIdFetch: Timer = metricsFactory.timer(prefix :+ "fetch_by_id_fetch")
    val fetchByIdQuery: Timer = metricsFactory.timer(prefix :+ "fetch_by_id_query")
    val fetchByKeyFetch: Timer = metricsFactory.timer(prefix :+ "fetch_by_key_fetch")
    val fetchByKeyQuery: Timer = metricsFactory.timer(prefix :+ "fetch_by_key_query")
    val searchFetch: Timer = metricsFactory.timer(prefix :+ "search_fetch")
    val searchQuery: Timer = metricsFactory.timer(prefix :+ "search_query")
  }

  val surrogateTemplateIdCache =
    new CacheMetrics(prefix :+ "surrogate_tpid_cache", metricsFactory)

  // Meters how long processing of a command submission request takes
  val commandSubmissionTimer: Timer =
    metricsFactory.timer(prefix :+ "command_submission_timing")
  // Meters how long processing of a query GET request takes
  val queryAllTimer: Timer = metricsFactory.timer(prefix :+ "query_all_timing")
  // Meters how long processing of a query POST request takes
  val queryMatchingTimer: Timer = metricsFactory.timer(prefix :+ "query_matching_timing")
  // Meters how long processing of a fetch request takes
  val fetchTimer: Timer = metricsFactory.timer(prefix :+ "fetch_timing")
  // Meters how long processing of a get party/parties request takes
  val getPartyTimer: Timer = metricsFactory.timer(prefix :+ "get_party_timing")
  // Meters how long processing of a party management request takes
  val allocatePartyTimer: Timer = metricsFactory.timer(prefix :+ "allocate_party_timing")
  // Meters how long processing of a package download request takes
  val downloadPackageTimer: Timer =
    metricsFactory.timer(prefix :+ "download_package_timing")
  // Meters how long processing of a package upload request takes
  val uploadPackageTimer: Timer = metricsFactory.timer(prefix :+ "upload_package_timing")
  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Timer =
    metricsFactory.timer(prefix :+ "incoming_json_parsing_and_validation_timing")
  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Timer =
    metricsFactory.timer(prefix :+ "response_creation_timing")
  // Meters how long a find by contract key database operation takes
  val dbFindByContractKey: Timer =
    metricsFactory.timer(prefix :+ "db_find_by_contract_key_timing")
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Timer =
    metricsFactory.timer(prefix :+ "db_find_by_contract_id_timing")
  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Timer =
    metricsFactory.timer(prefix :+ "command_submission_ledger_timing")
  // Meters http requests throughput
  val httpRequestThroughput: Meter =
    metricsFactory.meter(prefix :+ "http_request_throughput")
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter =
    metricsFactory.counter(prefix :+ "websocket_request_count")
  // Meters command submissions throughput
  val commandSubmissionThroughput: Meter =
    metricsFactory.meter(prefix :+ "command_submission_throughput")
  // Meters package uploads throughput
  val uploadPackagesThroughput: Meter =
    metricsFactory.meter(prefix :+ "upload_packages_throughput")
  // Meters party allocation throughput
  val allocatePartyThroughput: Meter =
    metricsFactory.meter(prefix :+ "allocation_party_throughput")

  val http = new DamlHttpMetrics(metricsFactory, "json-api")
  val websocket = new DamlWebSocketMetrics(metricsFactory, "json-api")
}
