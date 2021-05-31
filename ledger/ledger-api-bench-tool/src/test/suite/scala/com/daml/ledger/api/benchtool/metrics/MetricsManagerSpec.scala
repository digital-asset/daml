// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import akka.actor.testkit.typed.scaladsl.{
  BehaviorTestKit,
  LoggingTestKit,
  ManualTime,
  ScalaTestWithActorTestKit,
  TestProbe,
}
import akka.actor.typed.{ActorRef, Behavior}
import com.daml.ledger.api.benchtool.metrics.MetricsManager.Message
import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective
import com.daml.ledger.api.benchtool.metrics.{Metric, MetricValue, MetricsManager}
import com.daml.ledger.api.benchtool.util.MetricReporter
import org.scalatest.wordspec.AnyWordSpecLike

import java.time.Duration
import scala.concurrent.duration._
import scala.util.Random

class MetricsManagerSpec extends ScalaTestWithActorTestKit(ManualTime.config) with AnyWordSpecLike {

  "The MetricsManager" should {

    val manualTime: ManualTime = ManualTime()

    "log periodic report" in {
      val logInterval = 100.millis
      val manager = spawnManager(logInterval)

      val first = "first"
      val second = "second"

      manager ! MetricsManager.Message.NewValue(first)
      manager ! MetricsManager.Message.NewValue(second)

      manualTime.timePasses(logInterval - 1.milli)

      LoggingTestKit
        .info(s"$first-$second")
        .withOccurrences(1)
        .expect {
          manualTime.timePasses(10.milli)
        }
    }

    "respond with metrics result on StreamCompleted message" in {
      val manager = spawnManager()
      val probe = aTestProbe()

      manager ! MetricsManager.Message.StreamCompleted(probe.ref)
      probe.expectMessage(MetricsManager.Message.MetricsResult.Ok)
    }

    "respond with information about violated objectives" in {
      val manager = spawnManager()
      val probe = aTestProbe()

      manager ! MetricsManager.Message.NewValue("a value")
      manager ! MetricsManager.Message.NewValue(TestObjective.TestViolatingValue)
      manager ! MetricsManager.Message.StreamCompleted(probe.ref)

      probe.expectMessage(MetricsManager.Message.MetricsResult.ObjectivesViolated)
    }

    "stop when the stream completes" in {
      val probe = aTestProbe()
      val behaviorTestKit = BehaviorTestKit(managerBehavior())

      behaviorTestKit.isAlive shouldBe true

      behaviorTestKit.run(MetricsManager.Message.StreamCompleted(probe.ref))

      behaviorTestKit.isAlive shouldBe false
    }
  }

  private def aTestProbe(): TestProbe[Message.MetricsResult] =
    testKit.createTestProbe[MetricsManager.Message.MetricsResult]()

  private def spawnManager(
      logInterval: FiniteDuration = 100.millis
  ): ActorRef[MetricsManager.Message] =
    testKit.spawn(
      behavior = managerBehavior(logInterval),
      name = Random.alphanumeric.take(10).mkString,
    )

  private def managerBehavior(
      logInterval: FiniteDuration = 100.millis
  ): Behavior[MetricsManager.Message] =
    MetricsManager[String](
      streamName = "testStream",
      metrics = List(TestMetric()),
      logInterval = logInterval,
      reporter = TestReporter,
    )

  private case class TestMetricValue(value: String) extends MetricValue

  private case object TestObjective extends ServiceLevelObjective[TestMetricValue] {
    val TestViolatingValue = "BOOM"

    override def isViolatedBy(metricValue: TestMetricValue): Boolean =
      metricValue.value == TestViolatingValue
  }

  private object TestReporter extends MetricReporter {
    override def formattedValues(values: List[MetricValue]): String =
      values
        .map { case v: TestMetricValue =>
          v.value
        }
        .mkString(", ")

    override def finalReport(
        streamName: String,
        metrics: List[Metric[_]],
        duration: Duration,
    ): String = ""
  }

  private case class TestMetric(
      processedElems: List[String] = List.empty
  ) extends Metric[String] {
    override type V = TestMetricValue
    override type Objective = TestObjective.type

    override def onNext(value: String): Metric[String] = {
      this.copy(processedElems = processedElems :+ value)
    }

    override def periodicValue(periodDuration: Duration): (Metric[String], TestMetricValue) = {
      (this, TestMetricValue(s"PERIODIC: ${processedElems.mkString("-")}"))
    }

    override def finalValue(totalDuration: Duration): TestMetricValue = {
      TestMetricValue(s"FINAL: ${processedElems.mkString("-")}")
    }

    override def violatedObjective: Option[(TestObjective.type, TestMetricValue)] =
      if (processedElems.contains(TestObjective.TestViolatingValue))
        Some(TestObjective -> TestMetricValue(TestObjective.TestViolatingValue))
      else
        None

  }

}
