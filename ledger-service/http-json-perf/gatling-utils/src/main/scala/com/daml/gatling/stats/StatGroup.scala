// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

// generic container for Gatling statistics things
case class StatGroup(name: String, count: Int, percentage: Double) {
  import OutputFormattingHelpers._

  def formatted: String = s"> %-${available}s%8s (%3s%%)".format(name, count, printN(percentage))
}
