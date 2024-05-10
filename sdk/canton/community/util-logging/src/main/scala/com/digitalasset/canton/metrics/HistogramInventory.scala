// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.digitalasset.canton.metrics.HistogramInventory.Item
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.AtomicBoolean

class HistogramInventory extends LazyLogging {

  private val items = collection.mutable.ListBuffer[Item]()
  private val complete = new AtomicBoolean(false)

  def register(item: Item): Unit = {
    if (complete.get()) {
      logger.warn(
        s"Attempted to register a nested item ${item} after the inventory creation is completed."
      )
    }
    items.addOne(item)
  }

  def registered(): Seq[Item] = {
    complete.set(true)
    items.toList.distinct
  }

}
object HistogramInventory {

  def create[T](f: HistogramInventory => T): (HistogramInventory, T) = {
    val inventory = new HistogramInventory
    (inventory, f(inventory))
  }

  final case class Item(
      name: MetricName,
      summary: String,
      qualification: MetricQualification,
      description: String = "",
      labelsWithDescription: Map[String, String] = Map.empty,
  )(implicit
      histogramInventory: HistogramInventory
  ) {
    histogramInventory.register(this)

    /** Builds the info aligning with the histogram definition
      *
      * Due to open telemetry insisting that histograms are defined before the metric is known,
      * we need to dance through a few hoops to keep things consistent
      */
    def info: MetricInfo =
      MetricInfo(
        name = name,
        summary = summary,
        qualification = qualification,
        description = description,
        labelsWithDescription = labelsWithDescription,
      )

  }

  object Item {
    def apply(
        prefix: MetricName,
        suffix: MetricName,
        summary: String,
        qualification: MetricQualification,
    )(implicit histogramInventory: HistogramInventory): Item =
      new Item(prefix :+ suffix, summary, qualification)

    def apply(
        prefix: MetricName,
        suffix: String,
        summary: String,
        qualification: MetricQualification,
    )(implicit histogramInventory: HistogramInventory): Item =
      new Item(prefix :+ suffix, summary, qualification)
  }

}
