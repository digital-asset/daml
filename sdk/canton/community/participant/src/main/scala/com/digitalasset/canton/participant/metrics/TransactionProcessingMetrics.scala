// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.{Histogram, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.MetricQualification.Debug
import com.daml.metrics.api.{MetricDoc, MetricName, MetricsContext}

class TransactionProcessingMetrics(
    val prefix: MetricName,
    factory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {

  object protocolMessages {
    private val prefix = TransactionProcessingMetrics.this.prefix :+ "protocol-messages"

    @MetricDoc.Tag(
      summary = "Time to create a transaction confirmation request",
      description =
        """The time that the transaction protocol processor needs to create a transaction confirmation request.""",
      qualification = Debug,
    )
    val confirmationRequestCreation: Timer =
      factory.timer(prefix :+ "confirmation-request-creation")

    @MetricDoc.Tag(
      summary = "Time to parse a transaction message",
      description =
        """The time that the transaction protocol processor needs to parse and decrypt an incoming confirmation request.""",
      qualification = Debug,
    )
    val transactionMessageReceipt: Timer = factory.timer(prefix :+ "transaction-message-receipt")

    @MetricDoc.Tag(
      summary = "Confirmation request size",
      description =
        """Records the histogram of the sizes of (transaction) confirmation requests.""",
      qualification = Debug,
    )
    val confirmationRequestSize: Histogram =
      factory.histogram(prefix :+ "confirmation-request-size")
  }

}
