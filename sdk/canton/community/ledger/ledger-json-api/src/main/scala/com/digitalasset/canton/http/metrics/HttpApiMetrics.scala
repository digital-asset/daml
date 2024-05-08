// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricHandle.{Counter, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.daml.metrics.http.{DamlHttpHistograms, DamlHttpMetrics, DamlWebSocketMetrics, DamlWebSocketsHistograms}
import com.daml.metrics.api.HistogramInventory
import com.daml.metrics.api.HistogramInventory.Item
import com.digitalasset.canton.http.metrics.HttpApiMetrics.ComponentName

object HttpApiMetrics {
  lazy val ForTesting =
    new HttpApiMetrics(
      new HttpApiHistograms(MetricName("test"))(new HistogramInventory),
      NoOpMetricsFactory,
    )

  final val ComponentName = "json_api"
}

class HttpApiHistograms(parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private val _http: DamlHttpHistograms = new DamlHttpHistograms()
  private val _webSockets: DamlWebSocketsHistograms = new DamlWebSocketsHistograms()

  val prefix: MetricName = parent :+ "http_json_api"

  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Item =
    Item(
      prefix :+ "incoming_json_parsing_and_validation_timing",
      "",
      MetricQualification.Debug,
    )

  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Item =
    Item(
      prefix :+ "response_creation_timing",
      "",
      MetricQualification.Debug,
    )
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Item =
    Item(prefix :+ "db_find_by_contract_id_timing", "", MetricQualification.Debug)

  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Item =
    Item(prefix :+ "command_submission_ledger_timing", "", MetricQualification.Debug)

}

// TODO(#13303) Clean up metrics
class HttpApiMetrics(
    parent: HttpApiHistograms,
    labeledMetricsFactory: LabeledMetricsFactory,
) {
  import HttpApiMetrics.*
  import com.daml.metrics.api.MetricsContext.Implicits.empty

  val prefix: MetricName = parent.prefix

  // Meters how long parsing and decoding of an incoming json payload takes
  val incomingJsonParsingAndValidationTimer: Timer =
    labeledMetricsFactory.timer(
      parent.incomingJsonParsingAndValidationTimer.info
    )

  // Meters how long the construction of the response json payload takes
  val responseCreationTimer: Timer =
    labeledMetricsFactory.timer(
      parent.responseCreationTimer.info
    )
  // Meters how long a find by contract id database operation takes
  val dbFindByContractId: Timer =
    labeledMetricsFactory.timer(
      parent.dbFindByContractId.info
    )
  // Meters how long processing of the command submission request takes on the ledger
  val commandSubmissionLedgerTimer: Timer =
    labeledMetricsFactory.timer(
      parent.commandSubmissionLedgerTimer.info
    )
  // Meters http requests throughput
  // Meters how many websocket connections are currently active
  val websocketRequestCounter: Counter =
    labeledMetricsFactory.counter(
      MetricInfo(prefix :+ "websocket_request_count", "", MetricQualification.Debug)
    )

  val http = new DamlHttpMetrics(labeledMetricsFactory, ComponentName)
  val websocket = new DamlWebSocketMetrics(labeledMetricsFactory, ComponentName)

  val health = new HealthMetrics(labeledMetricsFactory)
}
