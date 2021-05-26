// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

trait Metric[Elem] {

  type Value <: MetricValue

  type Objective <: ServiceLevelObjective[Value]

  def onNext(value: Elem): Metric[Elem]

  def periodicValue(): (Metric[Elem], Value)

  def finalValue(totalDurationSeconds: Double): Value

  def violatedObjectives: Map[Objective, Value] = Map.empty

  def name: String = getClass.getSimpleName

}

object Metric {
  def rounded(value: Double): String = "%.2f".format(value)
}
