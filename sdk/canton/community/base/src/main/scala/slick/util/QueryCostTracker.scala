// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slick.util

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.{QueryCostMonitoringConfig, QueryCostSortBy}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.metrics.DbQueueMetrics
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.util.{LoggerUtil, MonadUtil}
import com.typesafe.scalalogging.Logger

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

/** QueryCostTracker is a utility to track the cost of database queries.
  *
  * We inject the query cost tracker into our copy of the async executor of slick.
  *
  * The executor has "execute", "beforeExecute", "afterExecute".
  * The public API is "execute". But if you register a runnable there, it just gets put into a queue.
  * Just before the threadpool actually starts the tasks, it will invoke "beforeExecute". So the time between beforeExecute and execute is the time the db tasks has been waiting in the queue.
  *
  * So if the executor has been closed, then execute will throw an exception when you attempt to schedule a task, but never ever invoke beforeExecute.
  * That's why we need failed to unregister a runnable. This can e.g. happen if the threadpool is closed before the task is started.
  *
  * So the flow is:
  * execute: increase queued
  * scheduled: decrease queued, increase running
  * completed: decrease running
  *
  * Maybe "scheduled" is misleading. It really means: the runnable is now starting to run on a thread.
  */
sealed trait QueryCostTracker {

  def scheduled(runnable: Runnable): Unit
  def register(runnable: Runnable): Unit
  def failed(runnable: Runnable): Unit
  def completed(runnable: Runnable): Unit
  def shutdownNow(): Unit

}

final class QueryCostTrackerImpl(
    logQueryCost: Option[QueryCostMonitoringConfig],
    metrics: DbQueueMetrics,
    scheduler: Option[ScheduledExecutorService],
    warnOnSlowQueryO: Option[PositiveFiniteDuration],
    warnInterval: PositiveFiniteDuration = PositiveFiniteDuration.tryOfSeconds(5),
    numThreads: Int,
    logger: Logger,
) extends QueryCostTracker {

  // currently running queries tracked for stats and slow query detection
  private val stats = TrieMap[Runnable, QueryInfo]()
  // fifo used to track slow queries
  private val running = new ConcurrentLinkedQueue[QueryInfo]()
  private val countRunning = new AtomicInteger(0)
  // last report time and query cost aggregation
  private val cost =
    new AtomicReference[(CantonTimestamp, QueryCostAccumulator, Map[String, QueryCostAccumulator])](
      (CantonTimestamp.now(), QueryCostAccumulator(), Map())
    )
  // slow query warning configuration
  private val (warnOnSlowQuery, warnOnSlowQueryMs): (Boolean, Long) = warnOnSlowQueryO match {
    case Some(duration) => (true, duration.duration.toMillis)
    case None => (false, 20000)
  }
  private val warnIntervalMs = warnInterval.duration.toMillis
  private val lastAlert = new AtomicReference[Long](0)
  // schedule background check for slow queries
  private val backgroundChecker =
    if (warnOnSlowQuery)
      scheduler.map(
        _.scheduleAtFixedRate(
          () => {
            cleanupAndAlert(System.nanoTime())
          },
          1000L, // initial delay
          1000L, // period
          TimeUnit.MILLISECONDS,
        )
      )
    else None

  override def scheduled(runnable: Runnable): Unit =
    stats.get(runnable).foreach(_.scheduled())

  override def register(runnable: Runnable): Unit = {
    val tr = if (logQueryCost.nonEmpty) {
      // find call site
      Thread
        .currentThread()
        .getStackTrace
        .find { e =>
          QueryCostTrackerImpl.ignore.forall(pack => !e.getClassName.startsWith(pack))
        }
        .map(_.toString)
        .getOrElse(
          "<unknown>"
        ) // if we can't find the call-site, then it's usually some transactionally
    } else "query-tracking-disabled"
    // initialize statistics gathering
    stats.put(runnable, QueryInfo(tr)).discard
  }

  override def failed(runnable: Runnable): Unit =
    stats.remove(runnable).foreach(_.failed())

  override def completed(runnable: Runnable): Unit =
    stats.remove(runnable).foreach(_.completed())

  override def shutdownNow(): Unit = {
    backgroundChecker.foreach(_.cancel(true))
    running.clear()
  }

  /** per query cost accumulator */
  private case class QueryCostAccumulator(
      fst: Long = 0L,
      count: Long = 0L,
      totalDiff: Long = 0L,
      totalDiffSq: Double = 0,
  ) {
    def observe(runningTime: Long): QueryCostAccumulator = {
      // We are shifting the value measured by the first one to make the algorithm
      // a bit more numerically stable. This is a common technique in online variance.
      // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
      val theFst = if (count == 0) runningTime else fst
      copy(
        fst = theFst,
        count = count + 1,
        totalDiff = totalDiff + (runningTime - theFst),
        totalDiffSq = totalDiffSq + (runningTime - theFst) * (runningTime - theFst),
      )
    }

    def meanNanos: Long = (fst + totalDiff / Math.max(count, 1))
    def meanMs: Double = meanNanos / 1e6
    def stdDevNanos: Double = if (count > 2) {
      Math
        .sqrt(
          (totalDiffSq - (totalDiff * totalDiff) / count) / (count - 1)
        )
    } else 0
    def stdDevMs: Double = stdDevNanos / 1e6
    def totalNanos: Long = (totalDiff + count * fst)
    def totalSecs: Double = totalNanos / 1e9

    def render(name: String): String =
      f"count=$count%7d mean=$meanMs%7.2f ms, stddev=$stdDevMs%7.2f ms, total=$totalSecs%5.1f s $name%s"

  }

  private case class QueryInfo(callsite: String) {

    private val added = System.nanoTime()
    private val scheduledNanos = new AtomicReference[Long](0)
    private val done = new AtomicBoolean(false)
    private val reportedAsSlow = new AtomicBoolean(false)
    private implicit val metricsContext: MetricsContext = MetricsContext("query" -> callsite)

    // increase queue counter on creation
    metrics.queue.inc()

    def scheduled(): Unit = {
      metrics.queue.dec()
      updateRunning(1)
      val tm = System.nanoTime()
      metrics.waitTimer.update(tm - added, TimeUnit.NANOSECONDS)
      scheduledNanos.set(tm)
      if (warnOnSlowQuery) {
        running.add(this).discard
      }
    }

    private def updateRunning(count: Int): Unit = {
      val cur = countRunning.updateAndGet(_ + count)
      metrics.running.updateValue(cur)
      // we are configuring the pools with numThreads == numConnections
      metrics.load.updateValue(cur / numThreads.toDouble)
    }

    def reportAsSlow(): Unit =
      reportedAsSlow.set(true)

    def isDone: Boolean = done.get()

    def getScheduledNanos: Long = scheduledNanos.get()

    def completed(): Unit = {
      val tm = System.nanoTime()
      done.set(true)
      val started = scheduledNanos.get()
      if (started > 0) {
        updateRunning(-1)
        metrics.execTimer.update(tm - started, TimeUnit.NANOSECONDS)
        track(callsite, tm - started)
      } else {
        track(s"$callsite - missing start time", tm - added)
      }
      if (reportedAsSlow.get()) {
        logger.warn(
          s"Slow database query $callsite finished after ${TimeUnit.NANOSECONDS.toMillis(tm - started)} ms"
        )
      }
      cleanupAndAlert(tm)
    }

    def failed(): Unit = {
      done.set(true)
      metrics.queue.dec()
      cleanupAndAlert(System.nanoTime())
    }

    private def track(trace: String, runningTime: Long): Unit =
      logQueryCost.filter(_ => logger.underlying.isInfoEnabled).foreach { config =>
        val now = CantonTimestamp.now()
        val (lastReportTime, total, reports) = cost.getAndUpdate { case (lastReport, total, tmp) =>
          if (lastReport.plusMillis(config.every.unwrap.toMillis) < now) {
            // Reset cost tracking
            (now, QueryCostAccumulator(), Map())
          } else {
            val costAccumulator: QueryCostAccumulator = tmp.getOrElse(trace, QueryCostAccumulator())
            val updatedCost = (tmp + (trace -> costAccumulator.observe(runningTime)))
            (lastReport, total.observe(runningTime), updatedCost)
          }
        }
        val emitReport = lastReportTime.plusMillis(config.every.unwrap.toMillis) < now
        val sortingFunction: QueryCostAccumulator => Long = config.sortBy match {
          case QueryCostSortBy.Count => _.count
          case QueryCostSortBy.Mean => _.meanNanos
          case QueryCostSortBy.Total => _.totalNanos
          case QueryCostSortBy.StdDev => _.stdDevNanos.toLong
        }

        if (emitReport) {
          val items = reports.toSeq
            .sortBy(x => -sortingFunction(x._2))
            .take(config.logLines.value)
            .map { case (name, cost) => cost.render(name) }
            .mkString("\n  ")
          val lastReportDeltaMs = (now - lastReportTime).toMillis
          val load = f"${metrics.load.getValue}%1.2f"
          val averageLoad =
            if (lastReportDeltaMs > 0 && numThreads > 0) {
              val tmp = total.totalSecs / (numThreads.toLong * lastReportDeltaMs / 1e3)
              f"$tmp%1.2f"
            } else "invalid"
          logger.info(
            s"Here is our list of the ${config.logLines} most expensive database queries for ${metrics.prefix} with load now=$load, avg=$averageLoad\n  " +
              total.render("total") + "\n  " + items
          )
        }
      }
  }

  @tailrec
  private def cleanupAndAlert(now: Long): Unit = if (warnOnSlowQuery) {
    Option(running.poll()) match {
      // if item is done, drop it and iterate
      case Some(item) if item.isDone => cleanupAndAlert(now)
      case None => ()
      // item is not done, check if we should warn
      case Some(item) =>
        // determine if we should warn again
        val last = lastAlert.get()

        def isSlow: Boolean =
          TimeUnit.NANOSECONDS.toMillis(now - item.getScheduledNanos) > warnOnSlowQueryMs

        def alert: Boolean = TimeUnit.NANOSECONDS.toMillis(now - last) > warnIntervalMs
        // if item is expired and if this warning process isn't running concurrently, emit a new warning
        if (isSlow && alert && lastAlert.compareAndSet(last, now)) {
          item.reportAsSlow()
          import scala.jdk.CollectionConverters.*
          val queries = (running
            .iterator()
            .asScala
            .filterNot(_.isDone)
            .toSeq :+ item)
            .sortBy(_.getScheduledNanos)
            .map(x =>
              s"${x.callsite} running-for=${TimeUnit.NANOSECONDS.toMillis(now - x.getScheduledNanos)} ms"
            )
            .mkString("\n  ")
          if (queries.nonEmpty) {
            logger.warn("Very slow or blocked queries detected:\n  " + queries)
          }
        }
        // put it back
        running.add(item).discard
    }

  }
}

object QueryCostTrackerImpl {
  private val ignore =
    Seq(
      "slick",
      "java.",
      "scala.",
      "cats.",
      "com.daml.metrics",
      "com.daml.executors",
      "com.digitalasset.canton.resource",
      "com.digitalasset.canton.resource.DbStorageMulti",
      "com.digitalasset.canton.util.retry",
      "com.digitalasset.canton.metrics",
      "com.daml.executors",
      "com.digitalasset.canton.store.db.DbBulkUpdateProcessor",
      "com.digitalasset.canton.lifecycle",
      LoggerUtil.getClass.getName.dropRight(1), // Drop Scala's trailing $
      MonadUtil.getClass.getName.dropRight(1), // Drop Scala's trailing $
    )
}

object QueryCostTracker {
  lazy val NoOp = new QueryCostTracker {
    override def scheduled(runnable: Runnable): Unit = {}
    override def register(runnable: Runnable): Unit = {}
    override def failed(runnable: Runnable): Unit = {}
    override def completed(runnable: Runnable): Unit = {}
    override def shutdownNow(): Unit = {}
  }

}
