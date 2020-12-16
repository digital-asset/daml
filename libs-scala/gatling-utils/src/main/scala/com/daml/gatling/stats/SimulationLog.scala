// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) 2019, Digital Asset (Switzerland) GmbH and/or its affiliates.
// All rights reserved.

package com.daml.gatling.stats

import java.io.File

import scalaz._
import Scalaz._

import scala.collection.compat._
import scala.collection.immutable.ListMap

import com.daml.gatling.stats.util.NonEmptySyntax._
import com.daml.gatling.stats.util.ReadFileSyntax._
import com.daml.gatling.stats.OutputFormattingHelpers._
import SimulationLog._

case class SimulationLog(simulation: String, scenarios: List[ScenarioStats]) {
  import Scalaz._

  lazy val requestsByType: Map[String, RequestTypeStats] =
    scenarios.foldRight(Map.empty[String, RequestTypeStats])(_.requestsByType |+| _)

  def toCsvString: String =
    toCsv
      .foldRight(Option.empty[Seq[String]]) { (row, result) =>
        result
          .orElse(List(row.keys.mkString(",")).some)
          .map(_ :+ row.values.map(_.toString.filterNot(_ == ',')).mkString(","))
      }
      .map(_.mkString("", System.lineSeparator(), System.lineSeparator()))
      .getOrElse("")

  private def toCsv: List[ListMap[String, String]] = {
    scenarios
      .flatMap { scenario =>
        scenario.requestsByType.map {
          case (requestType, stats) =>
            ListMap(
              "simulation" -> simulation.toString,
              "scenario" -> scenario.label,
              "maxUsers" -> scenario.maxUsers.toString,
              "request" -> requestType,
              "start" -> format(stats.successful.start),
              "duration" -> format(stats.successful.duration.map(_.toDouble / 1000)),
              "end" -> format(stats.successful.end),
              "count" -> stats.count.toString,
              "successCount" -> stats.successful.count.toString,
              "errorCount" -> stats.failed.count.toString,
              "min" -> format(stats.successful.percentile(0.0)),
              "p90" -> format(stats.successful.percentile(0.9)),
              "p95" -> format(stats.successful.percentile(0.95)),
              "p99" -> format(stats.successful.percentile(0.99)),
              "p999" -> format(stats.successful.percentile(0.999)),
              "max" -> format(stats.successful.percentile(1.0)),
              "mean" -> format(stats.successful.geometricMean.map(math.round)),
              "avg" -> format(stats.successful.mean.map(math.round)),
              "stddev" -> format(stats.successful.stdDev.map(math.round)),
              "rps" -> format(stats.successful.requestsPerSecond)
            )
        }
      }
  }
}

object SimulationLog {
  type Timestamp = Long

  private def format[A: Numeric: Show](fa: Option[A]): String = {
    import scalaz.syntax.show._
    val num = implicitly[Numeric[A]]
    fa.getOrElse(num.zero).shows
  }

  case class ScenarioStats(
      label: String,
      maxUsers: Int = 0,
      requestsByType: Map[String, RequestTypeStats] = Map.empty)

  case class DurationStatistics(
      durations: Seq[Int],
      start: Option[Timestamp],
      end: Option[Timestamp]) {

    def count: Int = durations.size
    def mean: Option[Double] = durations.nonEmptyOpt.map(ds => ds.sum.toDouble / ds.size)
    def geometricMean: Option[Double] =
      durations.nonEmptyOpt.map(ds => math.exp(ds.map(d => math.log(d.toDouble)).sum / ds.size))
    def duration: Option[Int] = for { s <- start; e <- end } yield (e - s).toInt
    def requestsPerSecond: Option[Double] = duration.map(count.toDouble / _.toDouble * 1000)

    def stdDev: Option[Double] =
      for {
        avg <- mean
        variance <- durations.nonEmptyOpt.map(ds => ds.map(d => math.pow(d - avg, 2)).sum / ds.size)
      } yield math.sqrt(variance)

    def percentile(p: Double): Option[Int] = {
      require(p >= 0.0 && p <= 1.0, "Percentile must be between zero and one, inclusive.")
      sortedDurations.nonEmptyOpt.map(ds => ds(Math.round((ds.size - 1).toDouble * p).toInt))
    }

    private lazy val sortedDurations = durations.toIndexedSeq.sorted
  }

  object DurationStatistics {
    implicit val durationStatisticsMonoid: Monoid[DurationStatistics] =
      new Monoid[DurationStatistics] {
        override def zero: DurationStatistics = DurationStatistics(Seq.empty, None, None)

        override def append(s1: DurationStatistics, s2: => DurationStatistics): DurationStatistics =
          DurationStatistics(
            s1.durations ++ s2.durations,
            min(s1.start, s2.start),
            max(s1.end, s2.end)
          )
      }

    private def min[A](fa: Option[A], fb: Option[A])(
        implicit ev: scala.math.Ordering[A]): Option[A] = (fa, fb) match {
      case (Some(x), Some(y)) => Some(ev.min(x, y))
      case (None, x @ Some(_)) => x
      case (x @ Some(_), None) => x
      case (_, _) => None
    }

    private def max[A](fa: Option[A], fb: Option[A])(
        implicit ev: scala.math.Ordering[A]): Option[A] = (fa, fb) match {
      case (Some(x), Some(y)) => Some(ev.max(x, y))
      case (None, x @ Some(_)) => x
      case (x @ Some(_), None) => x
      case (_, _) => None
    }
  }

  case class RequestTypeStats(successful: DurationStatistics, failed: DurationStatistics) {
    def count: Int = successful.count + failed.count

    // takes a function that calculates a metric for DurationStatistics, and generates a Count for all/successful/failed
    // based on that function
    def attribute[T](f: DurationStatistics => Option[T])(implicit N: Numeric[T]): Count[T] =
      Count(f(all).getOrElse(N.zero), f(successful).getOrElse(N.zero), f(failed).getOrElse(N.zero))

    def durationGroup(from: Option[Int], to: Option[Int]) = {
      val title = from.map(v => s"$v ms < ").getOrElse("") + "t" + to
        .map(v => s" < $v ms")
        .getOrElse("")
      val count = successful.durations.count(d => !from.exists(d < _) && !to.exists(d >= _))
      StatGroup(
        title,
        count,
        all.durations.nonEmptyOpt.map(ds => count.toDouble / ds.size * 100).getOrElse(0.0))
    }

    def formatted(
        title: String,
        bracket1millis: Int = 5000,
        bracket2millis: Int = 30000
    ): String = {
      require(bracket1millis < bracket2millis)
      List(
        "=" * lineLength,
        subtitle(title),
        attribute(_.count.some).formatted("Number of requests"),
        attribute(_.durations.nonEmptyOpt.map(_.min)).formatted("Min. response time"),
        attribute(_.durations.nonEmptyOpt.map(_.max)).formatted("Max. response time"),
        attribute(_.mean.map(math.round)).formatted("Mean response time"),
        attribute(_.stdDev.map(math.round)).formatted("Std. deviation"),
        attribute(_.percentile(0.9)).formatted("response time 90th percentile"),
        attribute(_.percentile(0.95)).formatted("response time 95th percentile"),
        attribute(_.percentile(0.99)).formatted("response time 99th percentile"),
        attribute(_.percentile(0.999)).formatted("response time 99.9th percentile"),
        attribute(_.requestsPerSecond).formatted("Mean requests/second"),
        subtitle("Response time distribution"),
        durationGroup(None, bracket1millis.some).formatted,
        durationGroup(bracket1millis.some, bracket2millis.some).formatted,
        durationGroup(bracket2millis.some, None).formatted,
        StatGroup(
          "failed",
          failed.durations.size,
          all.durations.nonEmptyOpt
            .map(ds => failed.durations.size.toDouble / ds.size * 100)
            .getOrElse(0.0)).formatted,
        "=" * lineLength
      ).mkString(System.lineSeparator)
    }

    lazy val all = successful |+| failed
  }

  object RequestTypeStats {
    def fromRequestStats(requests: Seq[RequestStats]): RequestTypeStats = {
      val successful = Map(true -> Seq(), false -> Seq()) ++ requests.groupBy(_.successful)
      val start = requests.nonEmptyOpt.map(_.map(_.start).min)
      RequestTypeStats(
        successful = DurationStatistics(
          successful(true).map(_.duration),
          start,
          successful(true).map(_.end).nonEmptyOpt.map(_.max)),
        failed = DurationStatistics(
          successful(false).map(_.duration),
          start,
          successful(false).map(_.end).nonEmptyOpt.map(_.max))
      )
    }

    implicit val requestTypeStatsMonoid: Monoid[RequestTypeStats] = new Monoid[RequestTypeStats] {
      override def zero: RequestTypeStats =
        RequestTypeStats(mzero[DurationStatistics], mzero[DurationStatistics])

      override def append(s1: RequestTypeStats, s2: => RequestTypeStats): RequestTypeStats =
        RequestTypeStats(s1.successful |+| s2.successful, s1.failed |+| s2.failed)
    }
  }

  case class RequestStats(
      userId: Int,
      requestLabel: String,
      start: Timestamp,
      end: Timestamp,
      successful: Boolean
  ) {
    def duration: Int = (end - start).toInt
  }

  def fromFile(file: File): String \/ SimulationLog =
    for {
      content <- file.contentsAsString.leftMap(_.getMessage)
      simulation <- fromString(content)
    } yield simulation

  def fromString(content: String): String \/ SimulationLog =
    for {
      rowsByType <- groupRowsByType(content)
      requests <- processRequests(rowsByType.getOrElse("REQUEST", List.empty))
      scenarios <- processScenarios(requests.groupBy(_.userId))(
        rowsByType.getOrElse("USER", List.empty))
      simulation <- processSimulation(scenarios)(rowsByType.getOrElse("RUN", List.empty))
    } yield simulation

  private def groupRowsByType(fileContent: String): String \/ Map[String, List[Seq[String]]] =
    \/.fromTryCatchNonFatal {
      fileContent
        .split('\n')
        .map(_.trim.split('\t').toSeq)
        .collect { case rowType +: fields => rowType -> fields }
        .toList
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2))
        .toMap
    }.leftMap(_.getMessage)

  def processScenarios(requestsByUser: Map[Int, Seq[RequestStats]])(
      userRows: List[Seq[String]]): String \/ List[ScenarioStats] =
    if (userRows.isEmpty) "Could not find any USER rows.".left
    else
      userRows
        .collect { case Seq(label, userId, "START", _*) => userId -> label }
        .foldRight(Map.empty[String, ScenarioStats]) {
          case ((userId, label), result) =>
            val requestsByType: Map[String, RequestTypeStats] = requestsByUser
              .getOrElse(userId.toInt, Seq.empty)
              .groupBy(_.requestLabel)
              .view
              .mapValues(RequestTypeStats.fromRequestStats)
              .toMap
            val s = result.getOrElse(label, ScenarioStats(label))
            result + (label -> s.copy(
              maxUsers = s.maxUsers + 1,
              requestsByType = s.requestsByType |+| requestsByType))
        }
        .values
        .toList
        .right

  private def processSimulation(scenarios: List[ScenarioStats])(
      runRows: List[Seq[String]]): String \/ SimulationLog =
    if (runRows.size != 1) s"Expected one RUN row in log, but found ${runRows.size}.".left
    else
      runRows.head match {
        case Seq(_, simulation, _*) => SimulationLog(simulation, scenarios).right
        case _ => "Found illegal RUN row.".left
      }

  private def processRequests(requestRows: List[Seq[String]]): String \/ Seq[RequestStats] =
    requestRows.traverseU {
      case Seq(userId, _, scenarioName, start, end, status, _*) =>
        \/.fromTryCatchNonFatal( // .toInt/Long throws if column non-numeric
          RequestStats(userId.toInt, scenarioName, start.toLong, end.toLong, status == "OK"))
          .leftMap(_.getMessage)
      case _ =>
        "Received REQUEST row with illegal number of fields".left
    }
}
