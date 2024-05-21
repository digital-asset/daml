// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import io.opentelemetry.sdk.metrics.`export`.MetricExporter

import java.io.{BufferedWriter, File, FileWriter}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.blocking
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class CsvReporter(config: MetricsReporterConfig.Csv, val loggerFactory: NamedLoggerFactory)
    extends MetricExporter
    with NamedLogging
    with NoTracing {

  private val running = new AtomicBoolean(true)
  private val files = new TrieMap[String, (FileWriter, BufferedWriter)]
  private val lock = new Object()

  def getAggregationTemporality(instrumentType: InstrumentType): AggregationTemporality =
    AggregationTemporality.CUMULATIVE

  override def flush(): CompletableResultCode = {
    tryOrStop {
      files.foreach { case (_, (_, bufferedWriter)) =>
        bufferedWriter.flush()
      }
    }
    CompletableResultCode.ofSuccess()
  }

  override def shutdown(): CompletableResultCode = {
    running.set(false)
    tryOrStop {
      files.foreach { case (_, (file, bufferedWriter)) =>
        bufferedWriter.close()
        file.close()
      }
    }
    CompletableResultCode.ofSuccess()
  }

  def `export`(metrics: util.Collection[MetricData]): CompletableResultCode = {
    blocking {
      lock.synchronized {
        if (running.get()) {
          val ts = CantonTimestamp.now()
          MetricValue.allFromMetricData(metrics.asScala).foreach { case (value, metadata) =>
            writeRow(ts, value, metadata)
          }
        }
      }
    }
    CompletableResultCode.ofSuccess()
  }

  private def tryOrStop(res: => Unit): Unit = {
    Try(res) match {
      case Success(_) =>
      case Failure(exception) =>
        logger.warn("Failed to write metrics to csv file. Turning myself off", exception)
        running.set(false)
    }
  }

  private def writeRow(ts: CantonTimestamp, value: MetricValue, data: MetricData): Unit = if (
    running.get()
  ) {
    val knownKeys = config.contextKeys.filter(key => value.attributes.contains(key))
    val prefix = knownKeys
      .flatMap { key =>
        value.attributes.get(key).toList
      }
      .mkString(".")
    val name =
      ((if (prefix.isEmpty) Seq.empty else Seq(prefix)) ++ Seq(data.getName, "csv")).mkString(".")
    tryOrStop {
      val (_, bufferedWriter) = files.getOrElseUpdate(
        name, {
          val file = new File(config.directory, name)
          logger.info(
            s"Creating new csv file ${file} for metric using keys=${knownKeys} from attributes=${value.attributes.keys}"
          )
          file.getParentFile.mkdirs()
          val writer = new FileWriter(file, true)
          val bufferedWriter = new BufferedWriter(writer)
          if (file.length() == 0) {
            bufferedWriter.append(value.toCsvHeader(data))
            bufferedWriter.newLine()
          }
          (writer, bufferedWriter)
        },
      )
      bufferedWriter.append(value.toCsvRow(ts, data))
      bufferedWriter.newLine()
    }
  }
}
