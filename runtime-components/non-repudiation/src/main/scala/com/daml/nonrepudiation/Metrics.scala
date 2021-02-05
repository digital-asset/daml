// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation

import com.codahale.metrics.{Meter, MetricRegistry, Timer}

final class Metrics(registry: MetricRegistry) {

  private val Prefix = "daml.nonrepudiation"

  private def name(suffix: String): String = s"$Prefix.$suffix"

  // For further details on the metrics below, see: https://metrics.dropwizard.io/4.1.2/manual/core.html
  // Quick reference:
  // - meters track rates, keeping both historical mean and exponentially-weighted
  //   moving average over the last 1, 5 and 15 minutes
  // - timers act as meters and also keep an histogram of the time for the
  //   measured action, giving exponentially more weight to more recent data

  // daml.nonrepudiation.processing
  // Overall time taken from interception to forwarding to the participant (or rejecting)
  val processingTimer: Timer = registry.timer(name("processing"))

  // daml.nonrepudiation.get_key
  // Time taken to retrieve the key from the certificate store
  // Part of the time tracked in daml.nonrepudiation.processing
  val getKeyTimer: Timer = registry.timer(name("get_key"))

  // daml.nonrepudiation.verify_signature
  // Time taken to verify the signature of a command
  // Part of the time tracked in daml.nonrepudiation.processing
  val verifySignatureTimer: Timer = registry.timer(name("verify_signature"))

  // daml.nonrepudiation.add_signed_payload
  // Time taken to add the signed payload before ultimately forwarding the command
  // Part of the time tracked in daml.nonrepudiation.processing
  val addSignedPayloadTimer: Timer = registry.timer(name("add_signed_payload"))

  // daml.nonrepudiation.rejections
  // Rate of calls that are being rejected before they can be forwarded to the participant
  // Historical and exponentially-weighted moving average rate over the latest 1, 5 and 15 minutes
  val rejectionsMeter: Meter = registry.meter(name("rejections"))

}
