package com.digitalasset.canton.metrics

import com.digitalasset.canton.metrics.MetricsConfig.MetricsFilterConfig
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.`export`.{CollectionRegistration, MetricReader}
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}

import java.util
import java.util.Collections
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.*
import scala.collection.concurrent.TrieMap

class FilteringMetricsReader private (filters: Seq[MetricsFilterConfig], parent: MetricReader) extends MetricReader {

  // cache the result of the filter for each metric name so we don't have to traverse lists all the time
  private val computedFilters = TrieMap[String, Boolean]()

  private def includeMetric(data: MetricData): Boolean = {
    if(data.getName.isEmpty) false
    else if(filters.isEmpty) true
    else computedFilters.getOrElseUpdate(data.getName, filters.exists(_.matches(data.getName)))
  }

  override def register(registration: CollectionRegistration): Unit = parent.register(new CollectionRegistration {
    override def collectAllMetrics(): util.Collection[MetricData] = {
      registration.collectAllMetrics().stream().filter(includeMetric(_)).collect(Collectors.toList())
    }
  })

  override def forceFlush(): CompletableResultCode = parent.forceFlush()

  override def shutdown(): CompletableResultCode = parent.shutdown()

  override def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality = parent.getAggregationTemporality(instrumentType)
}

object FilteringMetricsReader {
  def create(filters: Seq[MetricsFilterConfig], parent: MetricReader): MetricReader =
    if(filters.isEmpty) parent else new FilteringMetricsReader(filters, parent)
}
