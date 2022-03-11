// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import akka.actor.{ActorSystem, Cancellable}
import com.daml.ledger.resources.ResourceOwner
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.CollectorRegistry

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

object ConsoleReporter {
  def owner(actorSystem: ActorSystem) = {
    val resource: () => Cancellable = () => schedule(actorSystem)
    ResourceOwner.forCancellable(resource)
  }

  private def schedule(actorSystem: ActorSystem): Cancellable = {
    val reporter = new ConsoleReporter
    val registry = CollectorRegistry.defaultRegistry
    actorSystem.scheduler.scheduleAtFixedRate(5.seconds, 5.second)(() => reporter.report(registry))(
      scala.concurrent.ExecutionContext.global
    )
  }

}

object CustomReporter {}

object MetricValuesCollector {
  final case class Label(
      name: String,
      value: String,
  )
  final case class Sample[T](
      timestampMs: Long,
      name: String,
      labels: List[Label],
      value: T,
  )

  final case class Samples(
      name: String,
      help: String,
      samples: List[Sample[Any]],
  )

  def collect(registry: CollectorRegistry): Iterator[Samples] = {
    registry.metricFamilySamples().asIterator().asScala.map { samples: MetricFamilySamples =>
      val s: List[Sample[Any]] =
        samples.samples.asScala.view.toList.map { sample: MetricFamilySamples.Sample =>
          val labels = sample.labelNames.asScala
            .zip(sample.labelValues.asScala)
            .map { case (labelName, labelValue) =>
              Label(labelName, labelValue)
            }
            .toList
          Sample(
            timestampMs = sample.timestampMs,
            name = sample.name,
            labels = labels,
            value = sample.value,
          )
        }
      Samples(
        name = samples.name,
        help = samples.help,
        samples = s,
      )
    }
  }
}

class ConsoleReporter() {

  def report(registry: CollectorRegistry): Unit =
    printToConsole(MetricValuesCollector.collect(registry))

  private def printToConsole(dataToReport: Iterator[MetricValuesCollector.Samples]): Unit =
    dataToReport.foreach { samples =>
      println(formatSamples(samples))
    }

  private def formatSamples(samples: MetricValuesCollector.Samples) = {
    val s = samples.samples
      .map { sample =>
        val labels = sample.labels
          .map { label =>
            s"${label.name}:${label.value}"
          }
          .mkString(",")
        s"""[Name] ${sample.name} [Labels] $labels [Timestamp] ${sample.timestampMs} [Value] ${sample.value}"""
      }
      .mkString("\n")
    s"""-----
       |Name: ${samples.name}
       |Help: ${samples.help}
       |$s
       |-----
       |""".stripMargin
  }
}
