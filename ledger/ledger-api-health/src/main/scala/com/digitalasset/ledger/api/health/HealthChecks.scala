// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.health

import com.daml.ledger.api.health.HealthChecks._

class HealthChecks(private val components: Components) {
  def this(components: Component*) = this(components.toMap)

  def hasComponent(componentName: ComponentName): Boolean =
    components.exists(_._1 == componentName)

  def isHealthy(componentName: Option[ComponentName]): Boolean =
    componentName match {
      case None => components.forall(_._2.currentHealth() == Healthy)
      case Some(name) => components(name).currentHealth() == Healthy
    }
}

object HealthChecks {
  type ComponentName = String

  type Component = (ComponentName, ReportsHealth)

  type Components = Map[ComponentName, ReportsHealth]
}
