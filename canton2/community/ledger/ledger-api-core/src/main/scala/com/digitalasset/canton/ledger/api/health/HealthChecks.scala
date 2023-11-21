// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.health

import com.digitalasset.canton.ledger.api.health.HealthChecks.*

class HealthChecks(components: Components) {
  def this(components: Component*) = this(components.toMap)

  def hasComponent(componentName: ComponentName): Boolean =
    components.exists(_._1 == componentName)

  def isHealthy(componentName: Option[ComponentName]): Boolean =
    componentName match {
      case None => components.values.forall(_.currentHealth() == Healthy)
      case Some(name) => components(name).currentHealth() == Healthy
    }

  def +(component: Component) =
    new HealthChecks(this.components + component)
}

object HealthChecks {
  type ComponentName = String

  type Component = (ComponentName, ReportsHealth)

  type Components = Map[ComponentName, ReportsHealth]
}
