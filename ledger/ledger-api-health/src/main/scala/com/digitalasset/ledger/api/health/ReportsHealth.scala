// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

trait ReportsHealth {

  /**
    * Reports the current health of the object. This should always return immediately.
    */
  def currentHealth(): HealthStatus
}
