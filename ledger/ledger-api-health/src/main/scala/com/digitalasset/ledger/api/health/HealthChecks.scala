// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

import com.digitalasset.ledger.api.health.HealthChecks._

class HealthChecks(private val components: Components) {
  def this(components: Component*) = this(components.toMap)

  def hasComponent(componentName: ComponentName): Boolean =
    components.exists(_._1 == componentName)

  def ++(other: HealthChecks): HealthChecks = new HealthChecks(this.components ++ other.components)

  def isHealthy(componentName: Option[ComponentName]): Boolean =
    componentsMatching(componentName).forall(_._2.currentHealth() == Healthy)

  private def componentsMatching(componentName: Option[ComponentName]): Components =
    componentName match {
      case None => components
      case Some(name) => components.filterKeys(_ == name)
    }
}

object HealthChecks {
  type ComponentName = String

  type Component = (ComponentName, ReportsHealth)

  type Components = Map[ComponentName, ReportsHealth]

  val empty: HealthChecks = new HealthChecks()
}
