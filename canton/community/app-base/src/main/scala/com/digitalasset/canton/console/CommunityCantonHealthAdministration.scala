// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import cats.data.NonEmptyList
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.data.{CantonStatus, CommunityCantonStatus}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{NonNegativeDuration, Password}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.FutureInstances.*
import io.circe.{Encoder, Json, KeyEncoder, jawn}
import io.opentelemetry.exporter.internal.otlp.metrics.ResourceMetricsMarshaler
import io.opentelemetry.sdk.metrics.data.MetricData

import java.io.ByteArrayOutputStream
import java.time.Instant
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object CantonHealthAdministrationEncoders {

  /** Wraps the standardized log writer from OpenTelemetry, that outputs the metrics as JSON
    * Source: https://github.com/open-telemetry/opentelemetry-java/blob/main/exporters/logging-otlp/src/main/java/io/opentelemetry/exporter/logging/otlp/OtlpJsonLoggingMetricExporter.java
    * The encoder is not the most efficient as we first use the OpenTelemetry JSON serializer to write as a String,
    * and then use the Circe Jawn decoder to transform the string into a circe.Json object.
    * This is fine as the encoder is used only for on demand health dumps.
    */
  implicit val openTelemetryMetricDataEncoder: Encoder[Seq[MetricData]] =
    Encoder.encodeSeq[Json].contramap[Seq[MetricData]] { metrics =>
      val resourceMetrics = ResourceMetricsMarshaler.create(metrics.asJava)
      resourceMetrics.toSeq.map { resource =>
        val byteArrayOutputStream = new ByteArrayOutputStream()
        resource.writeJsonTo(byteArrayOutputStream)
        jawn
          .decode[Json](byteArrayOutputStream.toString)
          .fold(
            error => Json.fromString(s"Failed to decode metrics: $error"),
            identity,
          )
      }
    }

  implicit val traceElemEncoder: Encoder[StackTraceElement] =
    Encoder.encodeString.contramap(_.toString)
  implicit val threadKeyEncoder: KeyEncoder[Thread] = (thread: Thread) => thread.getName

  implicit val domainIdEncoder: KeyEncoder[DomainId] = (ref: DomainId) => ref.toString

  implicit val encodePort: Encoder[Port] = Encoder.encodeInt.contramap[Port](_.unwrap)

  // We do not want to serialize the password to JSON, e.g., as part of a config dump.
  implicit val encoder: Encoder[Password] = Encoder.encodeString.contramap(_ => "****")
}

object CantonHealthAdministration {
  def defaultHealthDumpName: File = {
    // Replace ':' in the timestamp as they are forbidden on windows
    val name = s"canton-dump-${Instant.now().toString.replace(':', '-')}.zip"
    File(name)
  }
}

trait CantonHealthAdministration[Status <: CantonStatus]
    extends Helpful
    with NamedLogging
    with NoTracing {
  protected val consoleEnv: ConsoleEnvironment
  implicit private val ec: ExecutionContext = consoleEnv.environment.executionContext
  override val loggerFactory: NamedLoggerFactory = consoleEnv.environment.loggerFactory

  protected def statusMap[A <: InstanceReferenceCommon](
      nodes: NodeReferences[A, _, _]
  ): Map[String, () => NodeStatus[A#Status]] = {
    nodes.all.map { node => node.name -> (() => node.health.status) }.toMap
  }

  def status(): Status

  @Help.Summary("Generate and write a health dump of Canton's state for a bug report")
  @Help.Description(
    "Gathers information about the current Canton process and/or remote nodes if using the console" +
      " with a remote config. The outputFile argument can be used to write the health dump to a specific path." +
      " The timeout argument can be increased when retrieving large health dumps from remote nodes." +
      " The chunkSize argument controls the size of the byte chunks streamed back from remote nodes. This can be used" +
      " if encountering errors due to gRPC max inbound message size being too low."
  )
  def dump(
      outputFile: File = CantonHealthAdministration.defaultHealthDumpName,
      timeout: NonNegativeDuration = consoleEnv.commandTimeouts.ledgerCommand,
      chunkSize: Option[Int] = None,
  ): String = {
    val remoteDumps = consoleEnv.nodes.remote.toList.parTraverse { n =>
      Future {
        n.health.dump(
          File.newTemporaryFile(s"remote-${n.name}-"),
          timeout,
          chunkSize,
        )
      }
    }

    // Try to get a local dump by going through the local nodes and returning the first one that succeeds
    def getLocalDump(nodes: NonEmptyList[InstanceReferenceCommon]): Future[String] = {
      Future {
        nodes.head.health.dump(
          File.newTemporaryFile(s"local-"),
          timeout,
          chunkSize,
        )
      }.recoverWith { case NonFatal(e) =>
        NonEmptyList.fromList(nodes.tail) match {
          case Some(tail) =>
            logger.info(
              s"Could not get health dump from ${nodes.head.name}, trying the next local node",
              e,
            )
            getLocalDump(tail)
          case None => Future.failed(e)
        }
      }
    }

    val localDump = NonEmptyList
      // The sorting is not necessary but makes testing easier
      .fromList(consoleEnv.nodes.local.toList.sortBy(_.name))
      .traverse(getLocalDump)
      .map(_.toList)

    consoleEnv.run {
      val zippedHealthDump = List(remoteDumps, localDump).flatSequence.map { allDumps =>
        outputFile.zipIn(allDumps.map(File(_)).iterator).pathAsString
      }
      Try(Await.result(zippedHealthDump, timeout.duration)) match {
        case Success(result) => CommandSuccessful(result)
        case Failure(e: TimeoutException) =>
          CommandErrors.ConsoleTimeout.Error(timeout.asJavaApproximation)
        case Failure(exception) => CommandErrors.CommandInternalError.ErrorWithException(exception)
      }
    }
  }
}

class CommunityCantonHealthAdministration(override val consoleEnv: ConsoleEnvironment)
    extends CantonHealthAdministration[CommunityCantonStatus] {

  @Help.Summary("Aggregate status info of all participants and domains")
  def status(): CommunityCantonStatus = {
    CommunityCantonStatus.getStatus(
      statusMap[SequencerNodeReferenceX](consoleEnv.sequencersX),
      statusMap[MediatorReferenceX](consoleEnv.mediatorsX),
      statusMap[ParticipantReferenceX](consoleEnv.participantsX),
    )
  }
}
