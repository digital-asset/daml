package com.digitalasset.platform.sandbox.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricRegistry, Slf4jReporter}
import com.digitalasset.platform.common.util.DirectExecutionContext

import scala.concurrent.Future

/** Manages metrics and reporters. Creates and starts a JmxReporter and creates an Slf4jReporter as well, which dumps
  * its metrics when this object is closed. */
final class MetricsManager extends AutoCloseable {

  private val metrics = new MetricRegistry()

  private val jmxReporter = JmxReporter
    .forRegistry(metrics)
    .inDomain("com.digitalasset.platform.sandbox")
    .build

  private val slf4jReporter = Slf4jReporter
    .forRegistry(metrics)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .withLoggingLevel(LoggingLevel.DEBUG)
    .build()

  jmxReporter.start()

  def timedFuture[T](timerName: String, f: => Future[T]) = {
    val timer = metrics.timer(timerName)
    val ctx = timer.time()
    val res = f
    res.onComplete(_ => ctx.stop())(DirectExecutionContext)
    res
  }

  override def close(): Unit = {
    slf4jReporter.report()
    jmxReporter.close()
  }
}

object MetricsManager {
  def apply(): MetricsManager = new MetricsManager()
}
