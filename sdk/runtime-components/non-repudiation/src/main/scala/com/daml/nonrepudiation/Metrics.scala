// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.MetricName
import com.daml.resources.HasExecutionContext

import scala.concurrent.duration.Duration

class Metrics(metricsFactory: LabeledMetricsFactory) {

  private val Prefix = MetricName("daml", "nonrepudiation")

  private def name(suffix: String) = Prefix :+ suffix

  // daml.nonrepudiation.processing
  // Overall time taken from interception to forwarding to the participant (or rejecting)
  val processingTimer: Timer = metricsFactory.timer(name("processing"))

  // daml.nonrepudiation.get_key
  // Time taken to retrieve the key from the certificate store
  // Part of the time tracked in daml.nonrepudiation.processing
  val getKeyTimer: Timer = metricsFactory.timer(name("get_key"))

  // daml.nonrepudiation.verify_signature
  // Time taken to verify the signature of a command
  // Part of the time tracked in daml.nonrepudiation.processing
  val verifySignatureTimer: Timer = metricsFactory.timer(name("verify_signature"))

  // daml.nonrepudiation.add_signed_payload
  // Time taken to add the signed payload before ultimately forwarding the command
  // Part of the time tracked in daml.nonrepudiation.processing
  val addSignedPayloadTimer: Timer = metricsFactory.timer(name("add_signed_payload"))

  // daml.nonrepudiation.rejections
  // Rate of calls that are being rejected before they can be forwarded to the participant
  // Historical and exponentially-weighted moving average rate over the latest 1, 5 and 15 minutes
  val rejectionsMeter: Meter = metricsFactory.meter(name("rejections"))

}

object Metrics {

  def owner[Context: HasExecutionContext](reportingInterval: Duration) = new MetricsOwner(
    reportingInterval
  )

}
