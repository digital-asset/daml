// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Histogram, MetricsFactory, Timer}
import com.daml.metrics.api.{MetricDoc, MetricName}

import scala.annotation.nowarn

class TransactionProcessingMetrics(
    val prefix: MetricName,
    @deprecated("Use LabeledMetricsFactory", since = "2.7.0") factory: MetricsFactory,
) {

  object protocolMessages {
    private val prefix = TransactionProcessingMetrics.this.prefix :+ "protocol-messages"

    @MetricDoc.Tag(
      summary = "Time to create a confirmation request",
      description =
        """The time that the transaction protocol processor needs to create a confirmation request.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val confirmationRequestCreation: Timer =
      factory.timer(prefix :+ "confirmation-request-creation")

    @MetricDoc.Tag(
      summary = "Time to parse a transaction message",
      description =
        """The time that the transaction protocol processor needs to parse and decrypt an incoming confirmation request.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val transactionMessageReceipt: Timer = factory.timer(prefix :+ "transaction-message-receipt")

    @MetricDoc.Tag(
      summary = "Confirmation request size",
      description =
        """Records the histogram of the sizes of (transaction) confirmation requests.""",
      qualification = Debug,
    )
    @nowarn("cat=deprecation")
    val confirmationRequestSize: Histogram =
      factory.histogram(prefix :+ "confirmation-request-size")
  }

}
