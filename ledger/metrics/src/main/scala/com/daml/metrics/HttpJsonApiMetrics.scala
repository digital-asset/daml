// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.{Counter, Meter, Timer}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardFactory

class HttpJsonApiMetrics(val prefix: MetricName, override val registry: MetricRegistry)
    extends DropwizardFactory {

  object Db extends DropwizardFactory {
    val prefix: MetricName = HttpJsonApiMetrics.this.prefix :+ "db"
    override val registry: MetricRegistry = HttpJsonApiMetrics.this.registry

    val fetchByIdFetch: Timer = timer(prefix :+ "fetch_by_id_fetch")
    val fetchByIdQuery: Timer = timer(prefix :+ "fetch_by_id_query")
    val fetchByKeyFetch: Timer = timer(prefix :+ "fetch_by_key_fetch")
    val fetchByKeyQuery: Timer = timer(prefix :+ "fetch_by_key_query")
    val searchFetch: Timer = timer(prefix :+ "search_fetch")
    val searchQuery: Timer = timer(prefix :+ "search_query")
  }

  val surrogateTemplateIdCache = new CacheMetrics(prefix :+ "surrogate_tpid_cache", registry)

  // Meters how long processing of a command submission request takes
  val commandSubmissionTimer: Timer = timer(prefix :+ "command_submission_timing")
  // Meters how long processing of a query GET request takes
  val queryAllTimer: Timer = timer(prefix :+ "query_all_timing")
  // Meters how long processing of a query POST request takes
  val queryMatchingTimer: Timer = timer(prefix :+ "query_matching_timing")
  // Meters how long processing of a fetch request takes
  val fetchTimer: Timer = timer(prefix :+ "fetch_timing")
  // Meters how long processing of a get party/parties request takes
  val getPartyTimer: Timer = timer(prefix :+ "get_party_timing")
  // Meters how long processing of a party management request takes
  val allocatePartyTimer: Timer = timer(prefix :+ "allocate_party_timing")
  // Meters how long processing of a package download request takes
  val downloadPackageTimer: Timer = timer(prefix :+ "download_package_timing")
  // Meters how long processing of a package upload request takes
  val uploadPackageTimer: Timer = timer(prefix :+ "upload_package_timing")
  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Timer =
    timer(prefix :+ "incoming_json_parsing_and_validation_timing")
  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Timer = timer(prefix :+ "response_creation_timing")
  // Meters how long a find by contract key database operation takes
  val dbFindByContractKey: Timer = timer(prefix :+ "db_find_by_contract_key_timing")
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Timer = timer(prefix :+ "db_find_by_contract_id_timing")
  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Timer =
    timer(prefix :+ "command_submission_ledger_timing")
  // Meters http requests throughput
  val httpRequestThroughput: Meter = meter(prefix :+ "http_request_throughput")
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter = counter(prefix :+ "websocket_request_count")
  // Meters command submissions throughput
  val commandSubmissionThroughput: Meter =
    meter(prefix :+ "command_submission_throughput")
  // Meters package uploads throughput
  val uploadPackagesThroughput: Meter = meter(prefix :+ "upload_packages_throughput")
  // Meters party allocation throughput
  val allocatePartyThroughput: Meter = meter(prefix :+ "allocation_party_throughput")
}
