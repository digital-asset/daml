// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.codahale.metrics.{Gauge, MetricRegistry}
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.CollectorRegistry
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should._
import org.scalatest.LoneElement._
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.jdk.CollectionConverters._

class ExtendedDropwizardExportsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "ExtendedDropwizardExports"

  it should "collect Counter samples" in new Fixture {
    metricRegistry.counter("test_counter").inc()

    sampleValue("test_counter").value shouldBe 1.0
  }

  it should "collect Gauge samples" in new Fixture {
    val integerGauge: Gauge[Int] = () => 1234
    val doubleGague: Gauge[Double] = () => 123.4
    val longGauge: Gauge[Long] = () => 1234L
    val booleanGauge: Gauge[Boolean] = () => true
    metricRegistry.register("integer_gauge", integerGauge)
    metricRegistry.register("double_gauge", doubleGague)
    metricRegistry.register("long_gauge", longGauge)
    metricRegistry.register("boolean_gauge", booleanGauge)

    sampleValue("integer_gauge").value shouldBe 1234
    sampleValue("double_gauge").value shouldBe 123.4
    sampleValue("long_gauge").value shouldBe 1234L
    sampleValue("boolean_gauge").value shouldBe 1.0
  }

  it should "collect null value when invalid Gauge type parameter used" in new Fixture {
    val invalidGauge: Gauge[String] = () => "boom"
    metricRegistry.register("invalid_gauge", invalidGauge)

    sampleValue("string_gauge") shouldBe None
  }

  it should "collect null value for Gauge returning null value" in new Fixture {
    val nullGauge: Gauge[String] = () => null
    metricRegistry.register("invalid_gauge", nullGauge)

    sampleValue("invalid_gauge") shouldBe None
  }

  it should "collect Histogram samples" in new Fixture {
    val histogram = metricRegistry.histogram("hist")

    (0 until 100).foreach(histogram.update)

    sampleValue("hist_count").value shouldBe 100.0
    sampleValue("hist_mean").value shouldBe 49.5
    sampleValue("hist_min").value shouldBe 0.0
    sampleValue("hist_max").value shouldBe 99.0
    List(0.75, 0.95, 0.98, 0.99).foreach { d =>
      sampleValue("hist", Map("quantile" -> d.toString)).value shouldBe ((d - 0.01) * 100)
    }
    sampleValue("hist", Map("quantile" -> "0.999")).value shouldBe 99
  }

  it should "collect Meter samples" in new Fixture {
    val meter = metricRegistry.meter("meter")

    meter.mark()
    meter.mark()

    sampleValue("meter_total").value shouldBe 2
  }

  it should "collect Timer samples" in new Fixture {
    val timer = metricRegistry.timer("timer")

    val context = timer.time()
    Thread.sleep(33)
    context.stop()

    sampleValue("timer", Map("quantile" -> "0.999")).value should be > 0.033
    sampleValue("timer_count").value shouldBe 1.0
    sampleValue("timer_min").value should be > 0.033
    sampleValue("timer_mean").value should be > 0.033
    sampleValue("timer_max").value should be > 0.033
  }

  it should "use Dropwizard original metric name in help" in new Fixture {
    metricRegistry.timer("daml.component.timer1")
    metricRegistry.counter("daml.component.counter1")
    metricRegistry.meter("daml.component.meter1")
    metricRegistry.histogram("daml.component.histogram1")
    metricRegistry.register("daml.component.gauge1", new TestGauge)

    collected().map(_.name) should contain theSameElementsAs List(
      "daml_component_timer1",
      "daml_component_counter1",
      "daml_component_meter1",
      "daml_component_histogram1",
      "daml_component_gauge1",
    )

    List(
      "daml.component.timer1",
      "daml.component.counter1",
      "daml.component.meter1",
      "daml.component.histogram1",
      "daml.component.gauge1",
    ).foreach { dropwizardName =>
      collected().exists(sample => sample.help.contains(dropwizardName)) shouldBe true
    }
  }

  it should "collect metrics with labels" in new LabelExtractionFixture {
    metricRegistry
      .counter("daml.component.counter1#application_id#testapp123")
      .inc()

    val expectedSample = new MetricFamilySamples.Sample(
      "daml_component_counter1",
      List("application_id").asJava,
      List("testapp123").asJava,
      1.0,
    )

    collected().loneElement.name shouldBe "daml_component_counter1"
    collected().loneElement.samples.loneElement should matchTo(expectedSample)
  }

  private class TestGauge extends Gauge[Double] {
    override def getValue: Double = 123.0
  }

  private class Fixture(
      labelsExtractor: ExtendedDropwizardExports.LabelsExtractor =
        ExtendedDropwizardExports.NoOpLabelsExtractor
  ) {
    val metricRegistry = new MetricRegistry()

    def sampleValue(
        sampleName: String,
        labels: Map[String, String] = Map.empty,
    ): Option[Double] = {
      // Explicitly wrapping into Option to prevent implicit null -> Double(0.0) conversion
      Option(
        collectorRegistry.getSampleValue(
          sampleName,
          labels.keys.toArray,
          labels.values.toArray,
        )
      ).map(_.toDouble)
    }

    def collected(): List[MetricFamilySamples] = exports.collect().asScala.toList

    private val collectorRegistry = new CollectorRegistry()
    private val exports: ExtendedDropwizardExports =
      new ExtendedDropwizardExports(metricRegistry, labelsExtractor)
        .register[ExtendedDropwizardExports](collectorRegistry)
  }

  private def extractTestLabel(metricName: String): (String, Map[String, String]) = {
    val parts = metricName.split("#")
    parts(0) -> Map(parts(1) -> parts(2))
  }

  private class LabelExtractionFixture extends Fixture(extractTestLabel)

  private final class SampleMatcher(right: MetricFamilySamples.Sample)
      extends Matcher[MetricFamilySamples.Sample] {
    override def apply(left: MetricFamilySamples.Sample): MatchResult = {
      val matches =
        left.value == right.value && left.name == right.name && left.labelNames.containsAll(
          right.labelNames
        ) && left.labelValues.containsAll(right.labelValues)
      MatchResult(
        matches = matches,
        rawFailureMessage = s"$left did not match $right",
        rawNegatedFailureMessage = s"$left matched $right",
      )
    }
  }

  private def matchTo(sample: MetricFamilySamples.Sample): SampleMatcher = new SampleMatcher(sample)
}
