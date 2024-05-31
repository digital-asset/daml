// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification, MetricsContext}

class TransactionProcessingHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private val protocolPrefix = prefix :+ "protocol-messages"

  private[metrics] val confirmationRequestCreation: Item =
    Item(
      protocolPrefix :+ "confirmation-request-creation",
      summary = "Time to create a transaction confirmation request",
      description =
        """The time that the transaction protocol processor needs to create a transaction confirmation request.""",
      qualification = MetricQualification.Latency,
    )

  private[metrics] val transactionMessageReceipt: Item =
    Item(
      protocolPrefix :+ "transaction-message-receipt",
      summary = "Time to parse and decrypt a transaction message",
      description =
        """The time that the transaction protocol processor needs to parse and decrypt an incoming confirmation request.""",
      qualification = MetricQualification.Debug,
    )

  private[metrics] val confirmationRequestSize: Item =
    Item(
      protocolPrefix :+ "confirmation-request-size",
      summary = "Confirmation request size",
      description =
        """Records the histogram of the sizes of (transaction) confirmation requests.""",
      qualification = MetricQualification.Debug,
    )

}

class TransactionProcessingMetrics(
    histograms: TransactionProcessingHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  object protocolMessages {

    val confirmationRequestCreation: Timer =
      factory.timer(histograms.confirmationRequestCreation.info)

    val transactionMessageReceipt: Timer = factory.timer(histograms.transactionMessageReceipt.info)

    val confirmationRequestSize: Histogram =
      factory.histogram(histograms.confirmationRequestSize.info)
  }

}
