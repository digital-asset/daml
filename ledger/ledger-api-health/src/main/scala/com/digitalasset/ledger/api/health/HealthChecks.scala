// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.health

import com.digitalasset.ledger.api.health.HealthChecks._

class HealthChecks(components: Component*) {
  def hasComponent(componentName: String): Boolean =
    components.exists(_._1 == componentName)

  def ++(other: HealthChecks): HealthChecks = this

  def isHealthy(componentName: Option[String]): Boolean =
    componentsMatching(componentName).forall(_._2.currentHealth == Healthy)

  private def componentsMatching(componentName: Option[String]): Seq[Component] =
    componentName match {
      case None => components
      case Some(name) => components.filter(_._1 == name)
    }
}

object HealthChecks {
  type Component = (String, ReportsHealth)

  val empty: HealthChecks = new HealthChecks()
}
