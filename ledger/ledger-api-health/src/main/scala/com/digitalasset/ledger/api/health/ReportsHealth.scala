// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.health

trait ReportsHealth {

  /** Reports the current health of the object. This should always return immediately.
    */
  def currentHealth(): HealthStatus
}
