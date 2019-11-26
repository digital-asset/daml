// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

case class HealthChecks() {
  def ++(other: HealthChecks): HealthChecks = this
}

object HealthChecks {
  val empty: HealthChecks = new HealthChecks()
}
