// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

import java.io.File

import org.scalactic.TypeCheckedTripleEquals
import scalaz.-\/
import scalaz.Scalaz._

import scala.util.Random
import com.daml.gatling.stats.util.ReadFileSyntax._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.bazeltools.BazelRunfiles.requiredResource

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SimulationLogSpec extends AnyFlatSpec with Matchers with TypeCheckedTripleEquals {
  import SimulationLog._

  behavior of "SimulationLog"

  private val simulationLog = "libs-scala/gatling-utils/src/test/resources/simulation-log"

  private def resultFor(fileName: String) =
    SimulationLog.fromFile(requiredResource(s"$simulationLog/$fileName.txt"))

  it should "fail if file does not exist" in {
    SimulationLog.fromFile(new File("DOES-NOT-EXIST-OgUzdJsvKHc9TtfNiLXA")) shouldBe a[-\/[_]]
  }

  it should "fail if no RUN entry" in {
    resultFor("no-run") shouldBe a[-\/[_]]
  }

  it should "fail if no USER entry" in {
    resultFor("no-user") shouldBe a[-\/[_]]
  }

  it should "fail if multiple RUN entries" in {
    resultFor("multiple-run") shouldBe a[-\/[_]]
  }

  it should "return correct result for minimal log" in {
    val request = RequestStats(
      userId = 1,
      requestLabel = "two-steps-sync",
      start = 1587679987299L,
      end = 1587680007510L,
      successful = true
    )
    val expected = SimulationLog(
      simulation = "standardtwostepssimulation",
      scenarios = ScenarioStats(
        "two-steps.standard",
        maxUsers = 1,
        requestsByType = Map("two-steps-sync" -> RequestTypeStats.fromRequestStats(request :: Nil))
      ) :: Nil
    )

    resultFor("minimal") should ===(expected.right)
  }

  it should "process requests without status message" in {
    val request = RequestStats(
      userId = 1,
      requestLabel = "two-steps-sync",
      start = 1587679987299L,
      end = 1587680007510L,
      successful = true
    )
    val expected = SimulationLog(
      simulation = "standardtwostepssimulation",
      scenarios = ScenarioStats(
        label = "two-steps.standard",
        maxUsers = 1,
        requestsByType = Map("two-steps-sync" -> RequestTypeStats.fromRequestStats(request :: Nil))
      ) :: Nil
    )

    resultFor("missing-error-message") should ===(expected.right)
  }

  it should "group requests by type, with failing requests and multiple users" in {
    val expected = SimulationLog(
      simulation = "foobar",
      scenarios = ScenarioStats(
        "two-steps.standard",
        maxUsers = 2,
        requestsByType = Map(
          "sync" -> RequestTypeStats(
            DurationStatistics(Seq(99, 98), Some(1000000000001L), Some(1000000000100L)),
            mzero[DurationStatistics].copy(start = Some(1000000000001L))),
          "desync" -> RequestTypeStats(
            DurationStatistics(Seq(95), Some(1000000000004L), Some(1000000000100L)),
            DurationStatistics(Seq(96), Some(1000000000004L), Some(1000000000100L))),
          "async" -> RequestTypeStats(
            DurationStatistics(Seq(97), Some(1000000000003L), Some(1000000000100L)),
            mzero[DurationStatistics].copy(start = Some(1000000000003L)))
        )
      ) :: Nil
    )
    resultFor("multiple-requests") should ===(expected.right)
  }

  it should "group requests by multiple scenarios and types" in {
    val expected = SimulationLog(
      simulation = "foo",
      scenarios = List(
        ScenarioStats(
          "third",
          maxUsers = 1,
          requestsByType = Map(
            "dummy" -> RequestTypeStats(
              DurationStatistics(Seq(98), Some(3000000000002L), Some(3000000000100L)),
              mzero[DurationStatistics].copy(start = Some(3000000000002L))
            )
          )
        ),
        ScenarioStats(
          "second",
          maxUsers = 3,
          requestsByType = Map(
            "sync" -> RequestTypeStats(
              DurationStatistics(Seq(100, 99), Some(2000000000001L), Some(2000000000101L)),
              mzero[DurationStatistics].copy(start = Some(2000000000001L))
            ),
            "nosync" -> RequestTypeStats(
              DurationStatistics(Seq(98), Some(2000000000002L), Some(2000000000100L)),
              mzero[DurationStatistics].copy(start = Some(2000000000002L))
            )
          )
        ),
        ScenarioStats(
          "first",
          maxUsers = 2,
          requestsByType = Map(
            "sync" -> RequestTypeStats(
              DurationStatistics(Seq(99, 98), Some(1000000000001L), Some(1000000000100L)),
              mzero[DurationStatistics].copy(start = Some(1000000000001L))
            ),
            "desync" -> RequestTypeStats(
              DurationStatistics(Seq(95), Some(1000000000004L), Some(1000000000100L)),
              DurationStatistics(Seq(96), Some(1000000000004L), Some(1000000000100L))
            ),
            "async" -> RequestTypeStats(
              DurationStatistics(Seq(97), Some(1000000000003L), Some(1000000000100L)),
              mzero[DurationStatistics].copy(start = Some(1000000000003L))
            )
          )
        )
      )
    )
    resultFor("multiple-scenarios") should ===(expected.right)
  }

  it should "ignore unknown entry types" in {
    resultFor("with-unknown-entries") should ===(resultFor("minimal"))
  }

  it should "produce correct CSV" in {
    val expected =
      requiredResource(s"$simulationLog/multiple-scenarios-expected-csv.csv").contentsAsString
    resultFor("multiple-scenarios")
      .map(_.toCsvString)
      .getOrElse(throw new AssertionError()) should ===(
      expected.getOrElse(throw new AssertionError()))
  }

  behavior of "DurationStatistics"

  it should "calculate correct metrics for empty result" in {
    val stats = DurationStatistics(Seq.empty, None, None)
    stats should ===(mzero[DurationStatistics])
    stats.duration should ===(None)
    stats.requestsPerSecond should ===(None)
    stats.count should ===(0)
    stats.percentile(0.0) should ===(None)
    stats.percentile(1.0) should ===(None)
    stats.mean should ===(None)
    stats.geometricMean should ===(None)
  }

  it should "calculate correct metrics for start time only" in {
    val stats = DurationStatistics(
      start = Some(1000L),
      end = None,
      durations = Seq()
    )
    stats.duration should ===(None)
    stats.requestsPerSecond should ===(None)
    stats.count should ===(0)
    stats.percentile(0.0) should ===(None)
    stats.percentile(1.0) should ===(None)
    stats.mean should ===(None)
    stats.geometricMean should ===(None)
  }

  it should "calculate correct metrics for single successful result" in {
    val stats = DurationStatistics(
      start = Some(5000L),
      end = Some(7000L),
      durations = Seq(2000)
    )
    stats.duration should ===(Some(2000))
    stats.requestsPerSecond should ===(Some(0.5))
    stats.count should ===(1)
    stats.percentile(0.0) should ===(Some(2000))
    stats.percentile(1.0) should ===(Some(2000))
    stats.mean should ===(Some(2000.0))
    stats.geometricMean.get should ===(2000.0 +- 0.01)
  }

  it should "calculate correct metrics for multiple successful results" in {
    val stats = DurationStatistics(
      start = Some(1000L),
      end = Some(5000L),
      durations = Seq(2000, 1000, 3000)
    )
    stats.duration should ===(Some(4000))
    stats.requestsPerSecond should ===(Some(3.0 / 4.0))
    stats.count should ===(3)
    stats.percentile(0.0) should ===(Some(1000))
    stats.percentile(0.5) should ===(Some(2000))
    stats.percentile(1.0) should ===(Some(3000))
    stats.mean should ===(Some(2000.0))
    stats.stdDev.get should ===(816.5 +- 0.1)
    stats.geometricMean.get should ===(1817.12 +- 0.01)
  }

  it should "calculate correct metrics for mixed results" in {
    val stats = DurationStatistics(
      start = Some(1000L),
      end = Some(5000L),
      durations = Seq(2000, 1000, 3000)
    )
    stats.duration should ===(Some(4000))
    stats.requestsPerSecond should ===(Some(3.0 / 4.0))
    stats.percentile(0.0) should ===(Some(1000))
    stats.percentile(0.5) should ===(Some(2000))
    stats.percentile(1.0) should ===(Some(3000))
    stats.stdDev.get should ===(816.5 +- 0.1)
    stats.geometricMean.get should ===(1817.12 +- 0.01)
  }

  it should "calculate correct percentiles for empty result" in {
    val stats = mzero[DurationStatistics]
    stats.percentile(0.0) should ===(None)
    stats.percentile(0.1) should ===(None)
    stats.percentile(0.5) should ===(None)
    stats.percentile(1.0) should ===(None)
  }

  it should "calculate correct percentiles and stdDev for single result" in {
    val stats = DurationStatistics(
      start = Some(1000),
      end = Some(4000),
      durations = Seq(3000)
    )
    stats.stdDev.get should ===(0.0)
    stats.percentile(0.0).get should ===(3000)
    stats.percentile(0.1).get should ===(3000)
    stats.percentile(0.5).get should ===(3000)
    stats.percentile(1.0).get should ===(3000)
  }

  it should "calculate correct standard deviation and percentiles" in {
    val stats = DurationStatistics(
      start = Some(1000),
      end = Some(4000),
      durations = Seq(100, 500, 1000, 2000, 3000)
    )
    stats.stdDev.get should ===(1053.38 +- 0.1)
    stats.mean.get should ===(1320.0)
    stats.percentile(0.0).get should ===(100)
    stats.percentile(0.1).get should ===(100)
    stats.percentile(0.25).get should ===(500)
    stats.percentile(0.5).get should ===(1000)
    stats.percentile(0.70).get should ===(2000)
    stats.percentile(0.75).get should ===(2000)
    stats.percentile(0.8).get should ===(2000)
    stats.percentile(0.9).get should ===(3000)
    stats.percentile(1.0).get should ===(3000)
  }

  it should "calculate correct percentiles, mean and stddev for large number of requests" in {
    val stats = DurationStatistics(
      start = Some(1000),
      end = Some(101000),
      durations = Random.shuffle(0 to 10000)
    )
    stats.stdDev.get should ===(2887.04 +- 0.0001)
    stats.mean.get should ===(10000.0 / 2)
    Range
      .BigDecimal(BigDecimal("0.0"), BigDecimal("1.0"), BigDecimal("0.001"))
      .foreach(p => stats.percentile(p.toDouble).get should ===((p * 10000).toInt))
  }

  behavior of "RequestTypeStats"

  it should "compose correct stats for empty result" in {
    RequestTypeStats.fromRequestStats(Nil) should ===(
      RequestTypeStats(
        successful = mzero[DurationStatistics],
        failed = mzero[DurationStatistics]
      ))
  }

  it should "compose correct stats for single failed result" in {
    val stats = RequestTypeStats.fromRequestStats(
      RequestStats(1, "foo", 1000, 2000, successful = false) :: Nil)
    stats should ===(
      RequestTypeStats(
        successful = mzero[DurationStatistics].copy(start = Some(1000L)),
        failed = DurationStatistics(
          start = Some(1000L),
          end = Some(2000L),
          durations = Seq(1000)
        )
      ))
  }

  it should "compose correct stats for single successful result" in {
    val stats = RequestTypeStats.fromRequestStats(
      RequestStats(1, "foo", 5000, 7000, successful = true) :: Nil)
    stats should ===(
      RequestTypeStats(
        successful = DurationStatistics(
          start = Some(5000L),
          end = Some(7000L),
          durations = Seq(2000)
        ),
        failed = mzero[DurationStatistics].copy(start = Some(5000L))
      )
    )
  }

  it should "compose correct stats for multiple successful results" in {
    val stats = RequestTypeStats.fromRequestStats(
      List(
        RequestStats(1, "foo", 2000, 4000, successful = true),
        RequestStats(1, "foo", 1000, 2000, successful = true),
        RequestStats(1, "foo", 2000, 5000, successful = true)
      )
    )
    stats should ===(
      RequestTypeStats(
        successful = DurationStatistics(
          start = Some(1000L),
          end = Some(5000L),
          durations = Seq(2000, 1000, 3000)
        ),
        failed = mzero[DurationStatistics].copy(start = Some(1000L))
      )
    )
  }

  it should "compose correct stats for mixed results" in {
    val stats = RequestTypeStats.fromRequestStats(
      List(
        RequestStats(1, "foo", 1000, 4000, successful = false),
        RequestStats(1, "foo", 2000, 4000, successful = true),
        RequestStats(1, "foo", 1500, 2500, successful = true),
        RequestStats(1, "foo", 2000, 5000, successful = true),
        RequestStats(1, "foo", 9000, 10000, successful = false)
      )
    )
    stats should ===(
      RequestTypeStats(
        successful = DurationStatistics(
          start = Some(1000L),
          end = Some(5000L),
          durations = Seq(2000, 1000, 3000)
        ),
        failed = DurationStatistics(
          start = Some(1000L),
          end = Some(10000L),
          durations = Seq(3000, 1000)
        )
      )
    )
  }

  it should "be monoidal" in {
    val stats1 = RequestStats(1, "foo", 1000, 2000, successful = true)
    val stats2 = RequestStats(1, "foo", 2000, 4000, successful = true)
    val stats3 = RequestStats(1, "foo", 2000, 5000, successful = false)
    val stats4 = RequestStats(1, "foo", 5000, 5500, successful = true)

    RequestTypeStats.fromRequestStats(Nil) |+| RequestTypeStats.fromRequestStats(Nil) should be(
      RequestTypeStats.fromRequestStats(Nil))

    RequestTypeStats.fromRequestStats(Nil) |+| RequestTypeStats.fromRequestStats(stats1 :: Nil) should be(
      RequestTypeStats.fromRequestStats(stats1 :: Nil))

    RequestTypeStats.fromRequestStats(stats1 :: Nil) |+| RequestTypeStats.fromRequestStats(Nil) should be(
      RequestTypeStats.fromRequestStats(stats1 :: Nil))

    RequestTypeStats.fromRequestStats(stats1 :: Nil) |+| RequestTypeStats.fromRequestStats(
      stats2 :: Nil) should be(RequestTypeStats.fromRequestStats(stats1 :: stats2 :: Nil))

    RequestTypeStats.fromRequestStats(stats3 :: Nil) |+| RequestTypeStats.fromRequestStats(
      stats2 :: Nil) should be(RequestTypeStats.fromRequestStats(stats3 :: stats2 :: Nil))

    RequestTypeStats.fromRequestStats(stats1 :: stats2 :: Nil) |+| RequestTypeStats
      .fromRequestStats(stats3 :: stats4 :: Nil) should be {
      RequestTypeStats.fromRequestStats(stats1 :: stats2 :: stats3 :: stats4 :: Nil)
    }
  }

  it should "render correctly when populated" in {
    RequestTypeStats(
      DurationStatistics(
        start = Some(1000),
        end = Some(101000),
        durations = Random.shuffle((1000 until 191000 by 100).toVector)
      ),
      DurationStatistics(
        start = Some(1000),
        end = Some(201000),
        durations = Random.shuffle((2000 until 2100).toVector)
      )
    ).formatted("foobar") should ===(
      """================================================================================
        |---- foobar --------------------------------------------------------------------
        |> Number of requests                                  2000 (OK=1900   KO=100   )
        |> Min. response time                                  1000 (OK=1000   KO=2000  )
        |> Max. response time                                190900 (OK=190900 KO=2099  )
        |> Mean response time                                 91255 (OK=95950  KO=2050  )
        |> Std. deviation                                     57243 (OK=54848  KO=29    )
        |> response time 90th percentile                     170900 (OK=171900 KO=2089  )
        |> response time 95th percentile                     180900 (OK=181400 KO=2094  )
        |> response time 99th percentile                     188900 (OK=189000 KO=2098  )
        |> response time 99.9th percentile                   190700 (OK=190700 KO=2099  )
        |> Mean requests/second                                  10 (OK=19     KO=0.5   )
        |---- Response time distribution ------------------------------------------------
        |> t < 5000 ms                                           40 (  2%)
        |> 5000 ms < t < 30000 ms                               250 (12.5%)
        |> 30000 ms < t                                        1610 (80.5%)
        |> failed                                               100 (  5%)
        |================================================================================""".stripMargin)
  }

  it should "render correctly when not populated" in {
    RequestTypeStats(mzero[DurationStatistics], mzero[DurationStatistics])
      .formatted("foobarbaz") should ===(
      """================================================================================
        |---- foobarbaz -----------------------------------------------------------------
        |> Number of requests                                     - (OK=-      KO=-     )
        |> Min. response time                                     - (OK=-      KO=-     )
        |> Max. response time                                     - (OK=-      KO=-     )
        |> Mean response time                                     - (OK=-      KO=-     )
        |> Std. deviation                                         - (OK=-      KO=-     )
        |> response time 90th percentile                          - (OK=-      KO=-     )
        |> response time 95th percentile                          - (OK=-      KO=-     )
        |> response time 99th percentile                          - (OK=-      KO=-     )
        |> response time 99.9th percentile                        - (OK=-      KO=-     )
        |> Mean requests/second                                   - (OK=-      KO=-     )
        |---- Response time distribution ------------------------------------------------
        |> t < 5000 ms                                            0 (  -%)
        |> 5000 ms < t < 30000 ms                                 0 (  -%)
        |> 30000 ms < t                                           0 (  -%)
        |> failed                                                 0 (  -%)
        |================================================================================""".stripMargin)
  }
}
