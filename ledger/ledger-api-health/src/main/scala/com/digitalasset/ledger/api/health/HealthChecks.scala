// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

class HealthChecks(components: (String, ReportsHealth)*) {
  def isHealthy: Boolean = components.forall(_._2.currentHealth == Healthy)

  def ++(other: HealthChecks): HealthChecks = this
}

object HealthChecks {
  val empty: HealthChecks = new HealthChecks()
}
