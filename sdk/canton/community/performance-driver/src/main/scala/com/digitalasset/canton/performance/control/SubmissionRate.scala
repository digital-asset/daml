// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.control

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import com.google.common.util.concurrent.AtomicDouble

import java.time.Duration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

trait SubmissionRate {

  /** number of commands that can be submitted without exceeding the current rate */
  def available: Int
  def currentRate: Double
  def maxRate: Double
  def pending: Int
  def succeeded: Int
  def observed: Int
  def failed: Int
  def backpressured: Int
  def latencyMs: Double
  def newSubmission(fut: Future[Boolean]): Unit
  def latencyObservation(durationMs: Long): Unit
  def updateRate(now: CantonTimestamp): Unit

  def updateSettings(maxRate: Double, targetLatencyMs: Int, adjustFactor: Double): Unit

}

object SubmissionRate extends NoTracing {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  class I(
      startMaxRate: Double,
      startTargetLatencyMs: Int,
      startAdjustFactor: Double,
      prefix: MetricName,
      metrics: LabeledMetricsFactory,
      loggerFactoryE: NamedLoggerFactory,
      now: () => CantonTimestamp,
  )(implicit ec: ExecutionContext)
      extends SubmissionRate
      with NamedLogging {

    val loggerFactory: NamedLoggerFactory = loggerFactoryE

    require(startMaxRate > 0, s"startMaxRate must be positive: $startMaxRate.")
    require(startTargetLatencyMs > 0, s"targetLatencyMs must be positive: $startTargetLatencyMs.")

    private val adjustFactor = new AtomicDouble(startAdjustFactor)
    private val targetLatencyMs = new AtomicDouble(startTargetLatencyMs.toDouble)
    private val currentRate_ =
      metrics.gauge[Double](
        MetricInfo(prefix :+ "currentRate", "Current rate settings", MetricQualification.Debug),
        0,
      )
    private val maxRate_ =
      metrics.gauge[Double](
        MetricInfo(prefix :+ "maxRate", "Current maximum rate", MetricQualification.Debug),
        startMaxRate,
      )
    private val pending_ = metrics.gauge[Int](
      MetricInfo(prefix :+ "pending", "How many pending commands", MetricQualification.Debug),
      0,
    )
    private val succeeded_ = new AtomicInteger(0)

    private val observed_ = new AtomicInteger(0)
    private val failed_ = metrics.gauge[Int](
      MetricInfo(prefix :+ "failed", "How many commands failed", MetricQualification.Debug),
      0,
    )
    private val backpressured_ = new AtomicInteger(0)

    private val latency_ =
      metrics.gauge[Double](
        MetricInfo(prefix :+ "latency", "Command latency", MetricQualification.Debug),
        0,
      )
    private val lastObservation = new AtomicReference[CantonTimestamp](now())
    private val lastRateReduction = new AtomicReference[CantonTimestamp](now())
    private val newInformation = new AtomicReference[(Int, Double)]((0, 0))

    override def toString: String =
      s"rate(cur=${"%1.1f" format currentRate}, latency=${"%1.0f" format latencyMs}, max=${"%1.1f" format maxRate}, available=$available, pending=$pending, succeeded=$succeeded, observed=$observed, failed=$failed, backpressured=$backpressured)"

    override def available: Int = {
      val latencyRatio = Math.min(1.0, latencyMs / targetLatencyMs.get())
      val maxNumPending =
        Math.max(1, maxRate * targetLatencyMs.get() / 1000.0 * (2.0 - latencyRatio)).toInt
      val maxAdd = Math.max(0, maxNumPending - pending)
      val ret =
        if (maxRate > currentRate)
          Math.max(1, Math.round(maxRate - currentRate).toInt)
        else 0
      val capped = Math.min(maxAdd, ret)
      // always allow to have at least one pending command, as otherwise recovery from a bad burst takes ages
      if (capped == 0 && pending == 0)
        1
      else capped
    }

    override def currentRate: Double = currentRate_.getValue

    override def maxRate: Double = maxRate_.getValue

    override def pending: Int = pending_.getValue

    override def succeeded: Int = succeeded_.get()

    override def observed: Int = observed_.get()

    override def failed: Int = failed_.getValue

    override def backpressured: Int = backpressured_.get()

    override def latencyMs: Double = latency_.getValue

    override def updateSettings(
        newMaxRate: Double,
        newTargetLatencyMs: Int,
        newAdjustFactor: Double,
    ): Unit = {
      maxRate_.updateValue(newMaxRate)
      targetLatencyMs.set(newTargetLatencyMs.toDouble)
      adjustFactor.set(newAdjustFactor)
    }

    override def newSubmission(fut: Future[Boolean]): Unit = {

      pending_.updateValue(_ + 1)
      currentRate_.updateValue(_ + 1.0)
      updateRate(now())

      def throttle(): Boolean = {
        failed_.updateValue(_ + 1)
        val current = maxRate_.getValue
        val updated = current * 0.9
        maxRate_.updateValue(updated)
        val updatedS = "%1.2f" format updated
        val currentS = "%1.2f" format current
        logger.debug(s"Reducing max-rate to $updatedS from $currentS")
        true
      }

      fut.onComplete {
        case Success(true) =>
          succeeded_.incrementAndGet().discard
        case Success(false) =>
          throttle().discard
        case _ =>
          failed_.updateValue(_ + 1)
      }
      fut.onComplete { _ =>
        pending_.updateValue(_ - 1)
      }

    }

    override def updateRate(now: CantonTimestamp): Unit = {
      val last = lastRateReduction.getAndSet(now)
      val deltaMs = Duration.between(last.toInstant, now.toInstant).toMillis
      val current = currentRate_.getValue
      val updated = if (adjustFactor.get() == 1) {
        logger.debug("The adjust factor is constant, setting the current rate to the max rate")
        maxRate_.getValue
      } else {
        current - maxRate_.getValue * (deltaMs / 1000.0)
      }
      currentRate_.updateValue(Math.max(0, updated))
      adjustMaxRate()
    }

    override def latencyObservation(millis: Long): Unit = {
      val obs = observed_.incrementAndGet()
      val tm = now()
      val last = lastObservation.getAndSet(tm)
      val tau = targetLatencyMs.get()
      val alphaObs = 1.0 / obs
      val alphaTau = Math.min((tm.toInstant.toEpochMilli - last.toInstant.toEpochMilli) / tau, 1.0)
      val alpha = Math.max(alphaTau, alphaObs)
      val avgLatency = alpha * millis + (1.0 - alpha) * latency_.getValue
      latency_.updateValue(avgLatency)
      val _ = newInformation.updateAndGet { case (count, total) =>
        (count + 1, total + millis)
      }
    }

    def adjustMaxRate(): Unit = {
      val (count, total) = newInformation.get()
      if (count > 0) {
        newInformation.set((0, 0))
        val lat = total / count.toDouble
        val latS = "%1.1fs" format (lat / 1000.0)
        val rate = maxRate_.getValue
        val rateS = "%1.2f" format rate
        val current = currentRate_.getValue
        val currentS = "%1.2f" format current
        val factor = count / (rate * Math.max(10, lat) / 1000.0)
        val targetLatency = targetLatencyMs.get()
        val latencyRatio = 1.0 - lat / targetLatency

        def limited(change: Double, ratio: Double): Double =
          rate * Math.pow(Math.pow(change, ratio), Math.min(1, factor))

        // if latency exceeds target latency, reduce
        val (newRate, hint) = {
          val (newRate, hint) =
            // reduce if we are not using up our max rate
            if (lat < targetLatency && current < (rate * 0.5))
              (
                limited(0.9, 1.0),
                s"Latency is low $latS but current rate $currentS is less than half of max-rate",
              )
            // stay if we are balanced within 5%
            else if (Math.abs(lat - targetLatency) < 0.05)
              (rate, s"Max rate is in balance with latency $latS")
            // adjust if we deviate
            else if (lat < targetLatency) {
              if (current > rate * 0.8)
                (
                  limited(adjustFactor.get(), latencyRatio),
                  s"Latency is at $latS and current rate $currentS is high",
                )
              else
                (rate, s"Latency $latS is low but current rate $currentS has enough room")
            } else
              (limited(adjustFactor.get(), latencyRatio), s"Latency is high at $latS")
          if (rate < 1.0 / 30 && newRate < rate)
            (rate, "Already at minimal rate")
          else
            (newRate, hint)
        }

        val newRateS = "%1.2f" format newRate
        if (newRate > rate) {
          logger.debug(s"Increasing max rate to $newRateS from $rateS: " + hint)
        } else if (newRate < rate) {
          logger.debug(s"Reducing max rate to $newRateS from $rateS: " + hint)
        } else {
          logger.debug(s"Not changing max rate $newRateS: " + hint)
        }
        maxRate_.updateValue(newRate)
      }
    }
  }
}
