// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

sealed abstract class HealthStatus extends Product with Serializable {
  val healthy: HealthStatus = Healthy

  val unhealthy: HealthStatus = Unhealthy

  def and(other: HealthStatus): HealthStatus = (this, other) match {
    case (_ @Healthy, _ @Healthy) => Healthy
    case _ => Unhealthy
  }
}

case object Healthy extends HealthStatus

case object Unhealthy extends HealthStatus
