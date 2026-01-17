// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.control

import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import com.google.common.util.concurrent.AtomicDouble

import java.time.Duration
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

sealed abstract class SubmissionRate(
    prefix: MetricName,
    metrics: LabeledMetricsFactory,
    now: () => CantonTimestamp,
)(implicit val metricsContext: MetricsContext) {
  protected lazy val pending_ : MetricHandle.Gauge[Int] = metrics.gauge[Int](
    MetricInfo(prefix :+ "pending", "How many pending commands", MetricQualification.Debug),
    0,
  )
  protected lazy val succeeded_ : AtomicInteger = new AtomicInteger(0)

  protected lazy val observed_ : AtomicInteger = new AtomicInteger(0)
  protected lazy val failed_ : MetricHandle.Gauge[Int] = metrics.gauge[Int](
    MetricInfo(prefix :+ "failed", "How many commands failed", MetricQualification.Debug),
    0,
  )

  protected lazy val latency_ : MetricHandle.Gauge[Double] =
    metrics.gauge[Double](
      MetricInfo(prefix :+ "latency", "Command latency", MetricQualification.Debug),
      0,
    )
  protected lazy val lastObservation = new AtomicReference[CantonTimestamp](now())

  // (number of accepted transactions, sum of the latencies)
  protected lazy val newInformation: AtomicReference[(Int, Double)] = new AtomicReference((0, 0))

  /** Number of commands that can be submitted without exceeding the current rate. Must not have any
    * side effect.
    */
  def available: Int

  def currentRate: Double
  def maxRate: Double
  def pending: Int = pending_.getValue
  def succeeded: Int = succeeded_.get()
  def observed: Int = observed_.get()
  def failed: Int = failed_.getValue
  def latencyMs: Double = latency_.getValue

  /** Take into account the result of a new submission
    * @param result
    *   True if the command was successful, false if it timed out
    */
  def newSubmission(result: Future[Boolean]): Unit

  def updateRate(now: CantonTimestamp): Unit

  /** Update the latency computation
    * @param durationMs
    *   Latest observed latency
    */
  def latencyObservation(durationMs: Long): Unit
}

object SubmissionRate extends NoTracing {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  /** This instance aims to reach a given target latency.
    * @param startMaxRate
    *   Initial rate (in submissions/second)
    * @param startTargetLatencyMs
    *   Desired target latency
    * @param startAdjustFactor
    *   How quickly should the rate be adapted
    * @param now
    *   Current time retriever
    */
  class TargetLatency(
      startMaxRate: Double,
      startTargetLatencyMs: Int,
      startAdjustFactor: Double,
      prefix: MetricName,
      metrics: LabeledMetricsFactory,
      loggerFactoryE: NamedLoggerFactory,
      now: () => CantonTimestamp,
  )(implicit ec: ExecutionContext)
      extends SubmissionRate(prefix, metrics, now = now)
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

    private val lastRateReduction = new AtomicReference[CantonTimestamp](now())

    override def toString: String =
      s"rate(cur=${"%1.1f" format currentRate}, latency=${"%1.0f" format latencyMs}, max=${"%1.1f" format maxRate}, available=$available, pending=$pending, succeeded=$succeeded, observed=$observed, failed=$failed)"

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

    def updateSettings(
        newMaxRate: Double,
        newTargetLatencyMs: Int,
        newAdjustFactor: Double,
    ): Unit = {
      maxRate_.updateValue(newMaxRate)
      targetLatencyMs.set(newTargetLatencyMs.toDouble)
      adjustFactor.set(newAdjustFactor)
    }

    override def newSubmission(result: Future[Boolean]): Unit = {
      pending_.updateValue(_ + 1)
      currentRate_.updateValue(_ + 1.0)
      updateRate(now())

      def throttle(): Unit = {
        failed_.updateValue(_ + 1)
        val current = maxRate_.getValue
        val updated = current * 0.9
        maxRate_.updateValue(updated)
        val updatedS = "%1.2f" format updated
        val currentS = "%1.2f" format current
        logger.debug(s"Reducing max-rate to $updatedS from $currentS")
      }

      result.onComplete {
        case Success(true) => succeeded_.incrementAndGet().discard
        case Success(false) => throttle()
        case _ => failed_.updateValue(_ + 1)
      }
      result.onComplete { _ =>
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

    override def latencyObservation(durationMs: Long): Unit = {
      val obs = observed_.incrementAndGet()
      val tm = now()
      val last = lastObservation.getAndSet(tm)
      val tau = targetLatencyMs.get()
      val alphaObs = 1.0 / obs
      val alphaTau = Math.min((tm.toInstant.toEpochMilli - last.toInstant.toEpochMilli) / tau, 1.0)
      val alpha = Math.max(alphaTau, alphaObs)
      val avgLatency = alpha * durationMs + (1.0 - alpha) * latency_.getValue
      latency_.updateValue(avgLatency)
      newInformation.updateAndGet { case (count, total) =>
        (count + 1, total + durationMs)
      }.discard
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

  /** This instance aims at a fixed rate.
    *
    * @param rate
    *   Desired rate in requests/second.
    * @param now
    *   Current time retriever
    */
  class FixedRate(
      rate: Double,
      prefix: MetricName,
      metrics: LabeledMetricsFactory,
      loggerFactoryE: NamedLoggerFactory,
      now: () => CantonTimestamp,
  )(implicit ec: ExecutionContext)
      extends SubmissionRate(prefix, metrics, now = now)
      with NamedLogging {
    private val previousAvailableChange: AtomicReference[CantonTimestamp] =
      new AtomicReference(now())

    private val available_ : AtomicInteger = new AtomicInteger(0)

    val loggerFactory: NamedLoggerFactory = loggerFactoryE

    override def available: Int = available_.get()

    override def currentRate: Double = rate

    override def maxRate: Double = rate

    override def newSubmission(fut: Future[Boolean]): Unit = {
      pending_.updateValue(_ + 1)
      available_.decrementAndGet().discard

      fut.onComplete {
        case Success(true) => succeeded_.incrementAndGet().discard
        case _ => failed_.updateValue(_ + 1)
      }

      fut.onComplete(_ => pending_.updateValue(_ - 1))
    }

    override def updateRate(now: CantonTimestamp): Unit =
      available_.updateAndGet { oldAvailable =>
        // time since the last change in seconds
        val deltaT = Math.max((now - previousAvailableChange.get()).toMillis, 0) / 1000.0

        // Note: the `toInt` rounds down
        val newAvailable = oldAvailable + (deltaT * rate).toInt

        // Since we round down, we don't progress `previousAvailableChange` if there is no change
        if (oldAvailable != newAvailable)
          previousAvailableChange.set(now)

        newAvailable
      }.discard

    override def latencyObservation(durationMs: Long): Unit = {
      val (newCount, newTotalLatencies) = newInformation.updateAndGet {
        case (curCount, curTotalLatencies) =>
          (curCount + 1, curTotalLatencies + durationMs)
      }

      latency_.updateValue(_ => newTotalLatencies / newCount)
    }
  }
}
