// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import com.daml.metrics.api.HistogramInventory.Item
import java.util.concurrent.atomic.AtomicBoolean

/** A helper class to register histogram items
  *
  * This class is used to register histogram items in a lazy way. It is used to ensure that all
  * histogram items are registered before the actual metrics are created.
  * This is necessary as open telemetry needs to know about the histogram views before
  * the actual metrics are created.
  *
  * Therefore, metrics definitions are split into two parts:
  *   - first, define the histogram names
  *   - seconds, pass that definition into the actual metric definition
  */
class HistogramInventory {

  private val items = collection.mutable.ListBuffer[Item]()
  private val complete = new AtomicBoolean(false)
  private lazy val logger = org.slf4j.LoggerFactory.getLogger(getClass)

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
