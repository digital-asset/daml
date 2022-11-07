// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricHandle.{Counter, Meter, Timer}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardFactory
import com.daml.metrics.api.opentelemetry.OpenTelemetryFactory
import io.opentelemetry.api.metrics.{Meter => OtelMeter}

class HttpJsonApiMetrics(
    val prefix: MetricName,
    val registry: MetricRegistry,
    val otelMeter: OtelMeter,
) {

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

  val dropWizardFactory = new DropwizardFactory {
    override val registry: MetricRegistry = HttpJsonApiMetrics.this.registry
  }

  // Meters how long processing of a command submission request takes
  val commandSubmissionTimer: Timer =
    dropWizardFactory.timer(prefix :+ "command_submission_timing")
  // Meters how long processing of a query GET request takes
  val queryAllTimer: Timer = dropWizardFactory.timer(prefix :+ "query_all_timing")
  // Meters how long processing of a query POST request takes
  val queryMatchingTimer: Timer = dropWizardFactory.timer(prefix :+ "query_matching_timing")
  // Meters how long processing of a fetch request takes
  val fetchTimer: Timer = dropWizardFactory.timer(prefix :+ "fetch_timing")
  // Meters how long processing of a get party/parties request takes
  val getPartyTimer: Timer = dropWizardFactory.timer(prefix :+ "get_party_timing")
  // Meters how long processing of a party management request takes
  val allocatePartyTimer: Timer = dropWizardFactory.timer(prefix :+ "allocate_party_timing")
  // Meters how long processing of a package download request takes
  val downloadPackageTimer: Timer =
    dropWizardFactory.timer(prefix :+ "download_package_timing")
  // Meters how long processing of a package upload request takes
  val uploadPackageTimer: Timer = dropWizardFactory.timer(prefix :+ "upload_package_timing")
  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Timer =
    dropWizardFactory.timer(prefix :+ "incoming_json_parsing_and_validation_timing")
  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Timer =
    dropWizardFactory.timer(prefix :+ "response_creation_timing")
  // Meters how long a find by contract key database operation takes
  val dbFindByContractKey: Timer =
    dropWizardFactory.timer(prefix :+ "db_find_by_contract_key_timing")
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Timer =
    dropWizardFactory.timer(prefix :+ "db_find_by_contract_id_timing")
  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Timer =
    dropWizardFactory.timer(prefix :+ "command_submission_ledger_timing")
  // Meters http requests throughput
  val httpRequestThroughput: Meter =
    dropWizardFactory.meter(prefix :+ "http_request_throughput")
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter =
    dropWizardFactory.counter(prefix :+ "websocket_request_count")
  // Meters command submissions throughput
  val commandSubmissionThroughput: Meter =
    dropWizardFactory.meter(prefix :+ "command_submission_throughput")
  // Meters package uploads throughput
  val uploadPackagesThroughput: Meter =
    dropWizardFactory.meter(prefix :+ "upload_packages_throughput")
  // Meters party allocation throughput
  val allocatePartyThroughput: Meter =
    dropWizardFactory.meter(prefix :+ "allocation_party_throughput")

  val openTelemetryFactory = new OpenTelemetryFactory {
    override val otelMeter: OtelMeter = HttpJsonApiMetrics.this.otelMeter
  }

  // golden signals
  val httpRequestsTotal: Meter = openTelemetryFactory.meter(prefix :+ "requests_total")
  val httpErrorsTotal: Meter = openTelemetryFactory.meter(prefix :+ "errors_total")
  val httpLatency: Timer = openTelemetryFactory.timer(prefix :+ "requests_duration_seconds")
  val httpRequestsPayloadBytesTotal: Meter =
    openTelemetryFactory.meter(prefix :+ "requests_payload_bytes_total")
  val httpResponsesPayloadBytesTotal: Meter =
    openTelemetryFactory.meter(prefix :+ "responses_payload_bytes_total")

  val websocketReceivedTotal: Meter =
    openTelemetryFactory.meter(prefix :+ "websocket_messages_received_total")
  val websocketReceivedBytesTotal: Meter =
    openTelemetryFactory.meter(prefix :+ "websocket_messages_received_bytes_total")
  val websocketSentTotal: Meter =
    openTelemetryFactory.meter(prefix :+ "websocket_messages_sent_total")
  val websocketSentBytesTotal: Meter =
    openTelemetryFactory.meter(prefix :+ "websocket_messages_sent_bytes_total")
}
