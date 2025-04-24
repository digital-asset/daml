// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config.declarative

import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.console.declarative.DeclarativeApi
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{DeclarativeApiMetrics, MetricValue, MetricsUtils}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, config}
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class DeclarativeApiTest
    extends BaseTestWordSpec
    with HasTestCloseContext
    with HasExecutionContext
    with MetricsUtils {

  class TestApi(
      removeExcess: Boolean = true,
      checkSelfConsistent: Boolean = true,
  )(implicit val executionContext: ExecutionContext)
      extends DeclarativeApi[Map[String, Int], Int] {

    override val metrics =
      new DeclarativeApiMetrics(MetricName("test"), metricsFactory(new HistogramInventory()))(
        MetricsContext.Empty
      )

    val state = new mutable.HashMap[String, Int]()
    private val want = new AtomicReference[Map[String, Int]](Map.empty)

    // (action, key) => Return argument for the "add", "upd", and "rm" functions
    // used to test failures
    private val responses =
      new AtomicReference[Map[(String, String), Either[String, Unit]]](Map.empty)

    def runTest(
        want: Map[String, Int],
        have: Option[Map[String, Int]] = None,
        responses: Map[(String, String), Either[String, Unit]] = Map.empty,
        expect: Boolean = true,
        items: Int = 0,
        errors: Int = 0,
    ): Assertion = {
      this.responses.set(responses)
      this.want.set(want)
      super.runSync(
      ) shouldBe expect
      this.state.toMap shouldBe have.getOrElse(want)
      checkMetric("test.declarative_api.items", items)
      checkMetric("test.declarative_api.errors", errors)
    }

    private def checkMetric(str: String, value: Int): Assertion =
      getMetricValues[MetricValue.LongPoint](str).map(_.value).toList match {
        case one :: Nil => one shouldBe value
        case Nil if value == 0 => succeed
        case Nil => fail(s"Expected metric $str to be $value, but it was not found")
        case one :: rest => fail("More than one metric item found")
      }

    override protected def name: String = "test"
    override protected def closeContext: CloseContext = DeclarativeApiTest.this.testCloseContext
    override protected def activeAdminToken: Option[CantonAdminToken] = Some(
      CantonAdminToken("test")
    )
    override protected def consistencyTimeout: config.NonNegativeDuration =
      config.NonNegativeDuration.ofSeconds(1)
    override protected def prepare(config: Map[String, Int])(implicit
        traceContext: TraceContext
    ): Either[String, Int] = Right(1)

    private def responseOr(str: String, k: String)(
        default: => Unit
    ): Either[String, Unit] =
      responses
        .get()
        .getOrElse(
          (str, k), {
            default
            Right(())
          },
        )
    override protected def sync(config: Map[String, Int], prep: Int)(implicit
        traceContext: TraceContext
    ): Either[String, DeclarativeApi.UpdateResult] = run[String, Int](
      name = "test",
      removeExcess,
      checkSelfConsistent,
      want.get().toSeq,
      fetch = _ => responseOr("fetch", "")(()).map(_ => state.toSeq),
      add = { case (k, v) =>
        responseOr("add", k) {
          state.put(k, v)
        }
      },
      upd = { case (k, desired, _) =>
        responseOr("update", k) {
          state.put(k, desired)
        }
      },
      rm = { (k, _) =>
        responseOr("rm", k) {
          state.remove(k)
        }
      },
    )

    override protected def loggerFactory: NamedLoggerFactory =
      DeclarativeApiTest.this.loggerFactory
  }

  "successful operations" should {

    "add, update and remove" in {
      val f = new TestApi()
      f.runTest(Map("a" -> 1), items = 1)
      f.runTest(Map("a" -> 2, "b" -> 2), items = 2)
      f.runTest(Map("a" -> 1, "b" -> 2, "c" -> 4), items = 3)
      f.runTest(Map("a" -> 1, "b" -> 2), items = 2)
      f.runTest(Map("a" -> 1), items = 1)
    }

    "add, update but do not remove existing" in {
      val f = new TestApi(removeExcess = false)
      f.runTest(Map("a" -> 1), items = 1)
      f.runTest(Map("b" -> 2), have = Some(Map("a" -> 1, "b" -> 2)), items = 1)
      f.runTest(
        Map("b" -> 1, "c" -> 2, "d" -> 3),
        have = Some(Map("a" -> 1, "b" -> 1, "c" -> 2, "d" -> 3)),
        items = 3,
      )
      f.runTest(
        Map(),
        have = Some(Map("a" -> 1, "b" -> 1, "c" -> 2, "d" -> 3)),
        items = 0,
      )
      f.runTest(
        Map("a" -> 99),
        have = Some(Map("a" -> 99, "b" -> 1, "c" -> 2, "d" -> 3)),
        items = 1,
      )
    }

  }

  "failures should be properly logged for" should {
    "add" in {
      val f = new TestApi()
      loggerFactory.assertLogs(
        f.runTest(
          Map("a" -> 1),
          have = Some(Map.empty),
          responses = Map(("add", "a") -> Left("NOT GOOD")),
          errors = 1,
          items = 1,
        ),
        _.warningMessage should include("Operation=add failed"),
      )
    }
    "update" in {
      val f = new TestApi()
      f.runTest(Map("a" -> 1), items = 1)
      loggerFactory.assertLogs(
        f.runTest(
          Map("a" -> 2),
          have = Some(Map("a" -> 1)),
          responses = Map(("update", "a") -> Left("NOT GOOD")),
          errors = 1,
          items = 1,
        ),
        _.warningMessage should include("Operation=update failed"),
      )
    }
    "remove" in {
      val f = new TestApi()
      f.runTest(Map("a" -> 1), items = 1)
      loggerFactory.assertLogs(
        f.runTest(
          Map(),
          have = Some(Map("a" -> 1)),
          responses = Map(("rm", "a") -> Left("NOT GOOD")),
          errors = 1,
          items = 0,
        ),
        _.warningMessage should include("Operation=remove failed"),
      )
    }
    "fetch" in {
      val f = new TestApi()
      loggerFactory.assertLogs(
        f.runTest(
          Map("a" -> 1),
          have = Some(Map()),
          responses = Map(("fetch", "") -> Left("NOT GOOD")),
          errors = -3,
          items = 0,
          expect = false,
        ),
        _.warningMessage should include("NOT GOOD"),
      )
    }

  }

}
