// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}

class TransactionProcessingMetrics(
    val prefix: MetricName,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  object protocolMessages {
    private val prefix = TransactionProcessingMetrics.this.prefix :+ "protocol-messages"

    val confirmationRequestCreation: Timer =
      factory.timer(
        MetricInfo(
          prefix :+ "confirmation-request-creation",
          summary = "Time to create a transaction confirmation request",
          description =
            """The time that the transaction protocol processor needs to create a transaction confirmation request.""",
          qualification = MetricQualification.Latency,
        )
      )

    val transactionMessageReceipt: Timer = factory.timer(
      MetricInfo(
        prefix :+ "transaction-message-receipt",
        summary = "Time to parse a transaction message",
        description =
          """The time that the transaction protocol processor needs to parse and decrypt an incoming confirmation request.""",
        qualification = MetricQualification.Debug,
      )
    )

    val confirmationRequestSize: Histogram =
      factory.histogram(
        MetricInfo(
          prefix :+ "confirmation-request-size",
          summary = "Confirmation request size",
          description =
            """Records the histogram of the sizes of (transaction) confirmation requests.""",
          qualification = MetricQualification.Debug,
        )
      )
  }

}
