// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.health

sealed abstract class HealthStatus extends Product with Serializable {
  def and(other: HealthStatus): HealthStatus = (this, other) match {
    case (Healthy, Healthy) => Healthy
    case _ => Unhealthy
  }
}

object HealthStatus {
  val healthy: HealthStatus = Healthy

  val unhealthy: HealthStatus = Unhealthy
}

case object Healthy extends HealthStatus

case object Unhealthy extends HealthStatus
