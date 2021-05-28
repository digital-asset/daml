// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective

trait Metric[Elem] {

  type V <: MetricValue

  type Objective <: ServiceLevelObjective[V]

  def onNext(value: Elem): Metric[Elem]

  def periodicValue(): (Metric[Elem], V)

  def finalValue(totalDurationSeconds: Double): V

  def violatedObjectives: Map[Objective, V] = Map.empty

  def name: String = getClass.getSimpleName

}

object Metric {
  def rounded(value: Double): String = "%.2f".format(value)
}
