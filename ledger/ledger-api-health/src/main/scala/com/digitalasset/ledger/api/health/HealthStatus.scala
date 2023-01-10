// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.health

sealed abstract class HealthStatus extends Product with Serializable {
  def and(other: HealthStatus): HealthStatus = (this, other) match {
    case (Healthy, Healthy) => Healthy
    case _ => Unhealthy
  }
}

case object Healthy extends HealthStatus

case object Unhealthy extends HealthStatus

// These methods are intended to be used by Java code, where the Scala values can be unidiomatic.
// You are encouraged to use the Scala values above when writing Scala code.
object HealthStatus {
  val healthy: HealthStatus = Healthy

  val unhealthy: HealthStatus = Unhealthy
}
