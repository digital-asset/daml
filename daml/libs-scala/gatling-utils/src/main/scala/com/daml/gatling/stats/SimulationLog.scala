// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

import java.nio.file.Path

import scalaz._

import scala.collection.immutable.ListMap
import spray.json._

import com.daml.gatling.stats.util.ReadFileSyntax._
import com.daml.gatling.stats.OutputFormattingHelpers._
import SimulationLog._

case class SimulationLog(simulation: String, requests: Map[String, RequestTypeStats]) {
  import Scalaz._

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
    requests.view.map { case (requestType, stats) =>
      ListMap(
        "simulation" -> simulation.toString,
        "request" -> requestType,
        "successCount" -> stats.numberOfRequests.ok.toString,
        "errorCount" -> stats.numberOfRequests.ko.toString,
        "min" -> stats.minResponseTime.ok.toString,
        "p90" -> stats.percentiles1.ok.toString,
        "p95" -> stats.percentiles2.ok.toString,
        "p99" -> stats.percentiles3.ok.toString,
        "p999" -> stats.percentiles4.ok.toString,
        "max" -> stats.maxResponseTime.ok.toString,
        "avg" -> stats.meanResponseTime.ok.toString,
        "stddev" -> stats.standardDeviation.ok.toString,
        "rps" -> stats.meanNumberOfRequestsPerSecond.ok.toString,
      )
    }.toList
  }
}

object SimulationLog {
  type Timestamp = Long

  case class AssertionsFile(
      simulationId: String
  )

  case class StatsFile(
      contents: Map[String, Request]
  )

  case class Request(
      stats: RequestTypeStats
  )

  case class RequestTypeStats(
      name: String,
      numberOfRequests: Count[Int],
      minResponseTime: Count[Double],
      maxResponseTime: Count[Double],
      meanResponseTime: Count[Double],
      standardDeviation: Count[Double],
      percentiles1: Count[Double],
      percentiles2: Count[Double],
      percentiles3: Count[Double],
      percentiles4: Count[Double],
      group1: StatGroup,
      group2: StatGroup,
      group3: StatGroup,
      group4: StatGroup,
      meanNumberOfRequestsPerSecond: Count[Double],
  ) {

    def formatted(
        title: String,
        bracket1millis: Int = 5000,
        bracket2millis: Int = 30000,
    ): String = {
      require(bracket1millis < bracket2millis)
      List(
        "=" * lineLength,
        subtitle(title),
        numberOfRequests.formatted("Number of requests"),
        minResponseTime.formatted("Min. response time"),
        maxResponseTime.formatted("Max. response time"),
        meanResponseTime.formatted("Mean response time"),
        standardDeviation.formatted("Std. deviation"),
        percentiles1.formatted("response time 90th percentile"),
        percentiles2.formatted("response time 95th percentile"),
        percentiles3.formatted("response time 99th percentile"),
        percentiles4.formatted("response time 99.9th percentile"),
        meanNumberOfRequestsPerSecond.formatted("Mean requests/second"),
        subtitle("Response time distribution"),
        group1.formatted,
        group2.formatted,
        group3.formatted,
        group4.formatted,
        "=" * lineLength,
      ).mkString(System.lineSeparator)
    }
  }

  object JsonProtocol extends DefaultJsonProtocol {
    implicit def countFormat[T: JsonFormat]: JsonFormat[Count[T]] = jsonFormat3(Count.apply[T])
    implicit val statGroupFormat: JsonFormat[StatGroup] = jsonFormat3(StatGroup)
    implicit val requestTypeStatsFormat: JsonFormat[RequestTypeStats] = jsonFormat15(
      RequestTypeStats
    )
    implicit val requestFormat: JsonFormat[Request] = jsonFormat1(Request)
    implicit val statsFileFormat: JsonFormat[StatsFile] = jsonFormat1(StatsFile)
    implicit val assertionsFileFormat: JsonFormat[AssertionsFile] = jsonFormat1(AssertionsFile)
  }

  def fromFiles(statsFile: Path, assertionsFile: Path): String \/ SimulationLog =
    for {
      statsContent <- statsFile.contentsAsString
      assertionsContent <- assertionsFile.contentsAsString
      simulation <- fromFileStrings(statsContent, assertionsContent)
    } yield simulation

  def fromFileStrings(statsContent: String, assertionsContent: String): String \/ SimulationLog = {
    import JsonProtocol._
    for {
      statsFile <- \/.attempt(statsContent.parseJson.convertTo[StatsFile])(_.toString)
      assertionsFile <- \/.attempt(
        assertionsContent.parseJson.convertTo[AssertionsFile]
      )(_.toString)
    } yield {
      val stats = statsFile.contents.view.values.map { case Request(stats) =>
        stats.name -> stats
      }.toMap
      SimulationLog(assertionsFile.simulationId, stats)
    }
  }
}
