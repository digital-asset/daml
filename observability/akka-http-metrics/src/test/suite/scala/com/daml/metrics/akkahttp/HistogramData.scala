package com.daml.metrics.akkahttp

import scala.annotation.tailrec

/** Data structure used to compare the content of histogram metrics
  * The exact recorded values are not accessible, using the other attributes to check that
  * the data was correctly recorded.
  */
case class HistogramData(
    count: Long,
    sum: Long,
    bucketCounts: List[Long],
)

object HistogramData {
  final val defaultBucketBoundaries =
    List(5L, 10L, 25L, 50L, 75L, 100L, 250L, 500L, 750L, 1000L, 2500L, 5000L, 7500L, 10000L)
  final val bucketCountsZero = List.fill(15)(0L)

  /** Generates a HistogramData instance, from the given value, to compare with the data
    * extracted from a metric.
    */
  def apply(value: Long): HistogramData = {
    HistogramData(List(value))
  }

  /** Generates an HistogramData instance, from the given values, to compare with the data
    * extracted from a metric.
    */
  def apply(values: List[Long]): HistogramData = {
    computeData(values, HistogramData(0, 0, bucketCountsZero))
  }

  @tailrec private def computeData(values: List[Long], acc: HistogramData): HistogramData = {
    values match {
      case head :: tail =>
        computeData(
          tail,
          HistogramData(
            acc.count + 1,
            acc.sum + head,
            updateBucketCounts(acc.bucketCounts, defaultBucketBoundaries, head),
          ),
        )
      case Nil =>
        acc
    }
  }

  private def updateBucketCounts(
      counts: List[Long],
      boundaries: List[Long],
      value: Long,
  ): List[Long] = {
    boundaries match {
      case head :: tail =>
        if (value <= head) {
          (counts.head + 1) :: counts.tail
        } else {
          (counts.head) :: updateBucketCounts(counts.tail, tail, value)
        }
      case Nil =>
        // last bucket
        List(counts.head + 1)
    }
  }
}

