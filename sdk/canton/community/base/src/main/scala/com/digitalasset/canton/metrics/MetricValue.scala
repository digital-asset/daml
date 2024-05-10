// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.metrics.data.*

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

sealed trait MetricValue extends PrettyPrinting {

  def attributes: Map[String, String]

  def toCsvHeader(data: MetricData): String
  def toCsvRow(ts: CantonTimestamp, data: MetricData): String

  final def select[TargetType <: MetricValue](implicit
      M: ClassTag[TargetType]
  ): Option[TargetType] = M.unapply(this)

}

object MetricValue {

  def allFromMetricData(items: Iterable[MetricData]): Seq[(MetricValue, MetricData)] = {
    items.flatMap { data =>
      MetricValue.fromMetricData(data).map { value => (value, data) }
    }.toSeq
  }
  def fromMetricData(item: MetricData): Seq[MetricValue] = {
    item.getType match {
      case MetricDataType.LONG_GAUGE =>
        item.getLongGaugeData.getPoints.asScala.map(fromLongPoint).toSeq
      case MetricDataType.DOUBLE_GAUGE =>
        item.getDoubleGaugeData.getPoints.asScala.map(fromDoublePoint).toSeq
      case MetricDataType.LONG_SUM => item.getLongSumData.getPoints.asScala.map(fromLongPoint).toSeq
      case MetricDataType.DOUBLE_SUM =>
        item.getDoubleSumData.getPoints.asScala.map(fromDoublePoint).toSeq
      case MetricDataType.SUMMARY =>
        item.getSummaryData.getPoints.asScala.map(fromSummaryValue).toSeq
      case MetricDataType.HISTOGRAM =>
        item.getHistogramData.getPoints.asScala.map(fromHistogramData).toSeq
      case MetricDataType.EXPONENTIAL_HISTOGRAM =>
        item.getHistogramData.getPoints.asScala.map(fromHistogramData).toSeq
    }
  }

  import Pretty.*

  import scala.jdk.CollectionConverters.*

  implicit val prettyValueAtPercentile: Pretty[ValueAtQuantile] = prettyOfClass(
    param("percentile", _.getQuantile.toString.unquoted),
    param("value", _.getValue.toString.unquoted),
  )

  implicit val prettyOfAttributes: Pretty[Map[String, String]] = prettyOfClass(
    unnamedParam(_.map { case (k, v) => s"$k=$v".singleQuoted }.toSeq)
  )

  trait Point[T] {
    this: MetricValue =>

    def value: T

    override def toCsvHeader(data: MetricData): String = {
      Seq("timestamp", "count").mkString(",")
    }

    override def toCsvRow(ts: CantonTimestamp, data: MetricData): String = {
      Seq(ts.getEpochSecond.toString, value.toString).mkString(",")
    }

  }

  final case class LongPoint(value: Long, attributes: Map[String, String])
      extends MetricValue
      with Point[Long] {
    override def pretty: Pretty[LongPoint] = prettyOfClass(
      param("value", _.value),
      param(
        "attributes",
        _.attributes,
      ),
    )

  }

  final case class DoublePoint(value: Double, attributes: Map[String, String])
      extends MetricValue
      with Point[Double] {
    override def pretty: Pretty[DoublePoint] = prettyOfClass(
      param("value", _.value.toString.unquoted),
      param(
        "attributes",
        _.attributes,
      ),
    )

  }

  final case class Summary(
      sum: Double,
      count: Long,
      quantiles: Seq[ValueAtQuantile],
      attributes: Map[String, String],
  ) extends MetricValue {
    override def pretty: Pretty[Summary] = prettyOfClass(
      param("sum", _.sum.toString.unquoted),
      param("count", _.count),
      param("quantiles", _.quantiles),
      param(
        "attributes",
        _.attributes,
      ),
    )

    override def toCsvHeader(data: MetricData): String = {
      (Seq("timestamp", "sum", "count") ++ quantiles.map(_.getQuantile).map(x => s"p$x%2.0f"))
        .mkString(",")
    }

    override def toCsvRow(ts: CantonTimestamp, data: MetricData): String = {
      (Seq(ts.getEpochSecond.toString, sum.toString, count.toString) ++ quantiles.map(
        _.getValue.toString
      ))
        .mkString(",")
    }

  }

  final case class Histogram(
      sum: Double,
      count: Long,
      counts: List[Long],
      boundaries: List[Double],
      attributes: Map[String, String],
  ) extends MetricValue {
    def maxBoundary: Double = {
      counts.zipAll(boundaries, 0L, Double.MaxValue).foldLeft(0.0) {
        case (acc, (count, boundary)) =>
          if (count > 0L) boundary else acc
      }
    }

    def average: Double = sum / Math.max(count, 1)

    def percentileBoundary(percentile: Double): Double = {
      require(percentile >= 0 && percentile <= 1, "percentile must be between 0 and 1")
      @tailrec
      def go(remaining: List[(Long, Double)], aggregatedCount: Long): Double = {
        remaining match {
          case Nil => Double.MaxValue
          case (count, boundary) :: tail =>
            val newCount = aggregatedCount + count
            if (newCount >= percentile * count) boundary else go(tail, newCount)
        }
      }
      go(counts.zipAll(boundaries, 0, Double.MaxValue), 0)
    }

    override def pretty: Pretty[Histogram] = prettyOfClass(
      param("sum", _.sum.toString.unquoted),
      param("count", _.count),
      param("counts", _.counts),
      param("boundaries", _.boundaries.map(_.toString.unquoted)),
      param(
        "attributes",
        _.attributes,
      ),
    )

    override def toCsvHeader(data: MetricData): String = {
      (Seq("timestamp", "sum", "count"))
        .mkString(",")
    }

    override def toCsvRow(ts: CantonTimestamp, data: MetricData): String = {
      (Seq(ts.getEpochSecond.toString, sum.toString, count.toString))
        .mkString(",")
    }

  }

  private def fromLongPoint(data: LongPointData): LongPoint = {
    LongPoint(data.getValue, mapAttributes(data.getAttributes))
  }
  private def fromDoublePoint(data: DoublePointData): DoublePoint = {
    DoublePoint(data.getValue, mapAttributes(data.getAttributes))
  }

  private def fromSummaryValue(data: SummaryPointData): Summary = {
    Summary(
      data.getSum,
      data.getCount,
      data.getValues.asScala.toSeq,
      mapAttributes(data.getAttributes),
    )
  }

  private def fromHistogramData(data: HistogramPointData): Histogram = {
    Histogram(
      data.getSum,
      data.getCount,
      data.getCounts.asScala.map(_.longValue()).toList,
      data.getBoundaries.asScala.map(_.doubleValue()).toList,
      mapAttributes(data.getAttributes),
    )
  }

  private def mapAttributes(attributes: Attributes): Map[String, String] =
    attributes.asMap().asScala.map { case (k, v) => (k.getKey, v.toString) }.toMap

  implicit val prettyMetricData: Pretty[MetricData] = {
    prettyOfClass(
      param("name", _.getName.singleQuoted),
      param("unit", _.getUnit.singleQuoted),
      param("type", _.getType.name().singleQuoted),
      param("values", x => fromMetricData(x)),
    )
  }
}
