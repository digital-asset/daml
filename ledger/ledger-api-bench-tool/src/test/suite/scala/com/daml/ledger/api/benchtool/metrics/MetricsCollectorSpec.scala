// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import akka.actor.testkit.typed.scaladsl.{BehaviorTestKit, ScalaTestWithActorTestKit}
import akka.actor.typed.{ActorRef, Behavior}
import com.daml.clock.AdjustableClock
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.{Clock, Duration, Instant, ZoneId}
import scala.util.Random

class MetricsCollectorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {
  import MetricsCollector.Message
  import MetricsCollector.Response

  "The MetricsCollector" should {
    "respond with empty periodic report" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.PeriodicReportResponse]()

      collector ! Message.PeriodicReportRequest(probe.ref)

      probe.expectMessage(
        Response.PeriodicReport(
          values = List(
            TestMetricValue("PERIODIC:")
          )
        )
      )
    }

    "respond with correct periodic report" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.PeriodicReportResponse]()

      collector ! Message.NewValue("banana")
      collector ! Message.NewValue("mango")
      collector ! Message.PeriodicReportRequest(probe.ref)

      probe.expectMessage(
        Response.PeriodicReport(
          values = List(
            TestMetricValue("PERIODIC:banana-mango")
          )
        )
      )
    }

    "not respond with a periodic report when requests are too frequent" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.PeriodicReportResponse]()

      collector ! Message.NewValue("banana")
      collector ! Message.NewValue("mango")
      collector ! Message.PeriodicReportRequest(probe.ref)

      probe.expectMessageType[Response.PeriodicReport]

      clock.fastForward(Duration.ofSeconds(1))
      collector ! Message.PeriodicReportRequest(probe.ref)

      probe.expectMessage(Response.ReportNotReady)
    }

    "include objective-violating values in periodic report" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.PeriodicReportResponse]()

      collector ! Message.NewValue("banana")
      collector ! Message.NewValue(TestObjective.TestViolatingValue)
      collector ! Message.NewValue("mango")
      collector ! Message.PeriodicReportRequest(probe.ref)

      probe.expectMessage(
        Response.PeriodicReport(
          values = List(
            TestMetricValue("PERIODIC:banana-tomato-mango")
          )
        )
      )
    }

    "respond with empty final report" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.FinalReport]()

      collector ! Message.FinalReportRequest(probe.ref)

      probe.expectMessage(
        Response.FinalReport(
          metricsData = List(
            Response.MetricFinalReportData(
              name = "Test Metric",
              value = TestMetricValue("FINAL:"),
              violatedObjectives = Nil,
            )
          ),
          totalDuration = Duration.ofSeconds(10),
        )
      )
    }

    "respond with correct final report" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.FinalReport]()

      collector ! Message.NewValue("mango")
      collector ! Message.NewValue("banana")
      collector ! Message.NewValue("cherry")
      collector ! Message.FinalReportRequest(probe.ref)

      probe.expectMessage(
        Response.FinalReport(
          metricsData = List(
            Response.MetricFinalReportData(
              name = "Test Metric",
              value = TestMetricValue("FINAL:mango-banana-cherry"),
              violatedObjectives = Nil,
            )
          ),
          totalDuration = Duration.ofSeconds(10),
        )
      )
    }

    "include information about violated objective in the final report" in new CollectorFixture {
      val probe = testKit.createTestProbe[Response.FinalReport]()

      collector ! Message.NewValue("mango")
      collector ! Message.NewValue(TestObjective.TestViolatingValue)
      collector ! Message.NewValue("cherry")
      collector ! Message.FinalReportRequest(probe.ref)

      probe.expectMessage(
        Response.FinalReport(
          metricsData = List(
            Response.MetricFinalReportData(
              name = "Test Metric",
              value = TestMetricValue("FINAL:mango-tomato-cherry"),
              violatedObjectives = List(
                (
                  TestObjective,
                  TestMetricValue(TestObjective.TestViolatingValue),
                )
              ),
            )
          ),
          totalDuration = Duration.ofSeconds(10),
        )
      )
    }

    "stop after receiving final report request" in {
      val probe = testKit.createTestProbe[Response.FinalReport]()
      val behaviorTestKit = BehaviorTestKit(behavior)

      behaviorTestKit.isAlive shouldBe true

      behaviorTestKit.run(Message.FinalReportRequest(probe.ref))

      behaviorTestKit.isAlive shouldBe false
    }
  }

  private class CollectorFixture {
    private val now = Clock.systemUTC().instant()
    private val tenSecondsAgo = now.minusSeconds(10)
    private val minimumReportInterval = Duration.ofSeconds(5)
    val clock = AdjustableClock(
      baseClock = Clock.fixed(now, ZoneId.of("UTC")),
      offset = Duration.ZERO,
    )
    val collector: ActorRef[Message] =
      spawnWithFixedClock(clock, tenSecondsAgo, tenSecondsAgo, minimumReportInterval)
  }

  private def spawnWithFixedClock(
      clock: Clock,
      startTime: Instant,
      lastPeriodicCheck: Instant,
      minimumTimePeriodBetweenSubsequentReports: Duration,
  ) = {
    val behavior =
      new MetricsCollector[String](None, minimumTimePeriodBetweenSubsequentReports, clock)
        .handlingMessages(
          metrics = List(TestMetric()),
          lastPeriodicCheck = lastPeriodicCheck,
          startTime = startTime,
        )
    testKit.spawn(
      behavior = behavior,
      name = Random.alphanumeric.take(10).mkString,
    )
  }

  private def behavior: Behavior[Message] = {
    MetricsCollector[String](
      metrics = List(TestMetric()),
      exposedMetrics = None,
    )
  }

  private case class TestMetricValue(value: String) extends MetricValue

  private case object TestObjective extends ServiceLevelObjective[TestMetricValue] {
    val TestViolatingValue = "tomato"

    override def isViolatedBy(metricValue: TestMetricValue): Boolean =
      metricValue.value == TestViolatingValue
  }

  private case class TestMetric(
      processedElems: List[String] = List.empty
  ) extends Metric[String] {
    override type V = TestMetricValue
    override type Objective = TestObjective.type

    override def name: String = "Test Metric"

    override def onNext(value: String): Metric[String] = {
      this.copy(processedElems = processedElems :+ value)
    }

    override def periodicValue(periodDuration: Duration): (Metric[String], TestMetricValue) = {
      (this, TestMetricValue(s"PERIODIC:${processedElems.mkString("-")}"))
    }

    override def finalValue(totalDuration: Duration): TestMetricValue = {
      TestMetricValue(s"FINAL:${processedElems.mkString("-")}")
    }

    override def violatedPeriodicObjectives: List[(TestObjective.type, TestMetricValue)] =
      if (processedElems.contains(TestObjective.TestViolatingValue))
        List(TestObjective -> TestMetricValue(TestObjective.TestViolatingValue))
      else
        Nil

    override def violatedFinalObjectives(
        totalDuration: Duration
    ): List[(TestObjective.type, TestMetricValue)] = Nil

  }

}
