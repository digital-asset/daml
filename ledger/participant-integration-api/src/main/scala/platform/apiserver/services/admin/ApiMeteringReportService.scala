// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.v1.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportService
import com.daml.ledger.api.v1.admin.metering_report_service._
import com.daml.ledger.participant.state.index.v2.MeteringStore
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.apiserver.services.admin.ApiMeteringReportService._
import com.daml.platform.server.api.ValidationLogger
import com.google.protobuf.timestamp.{Timestamp => ProtoTimestamp}
import io.grpc.ServerServiceDefinition
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}

import com.daml.ledger.api.validation.ValidationErrors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

private[apiserver] final class ApiMeteringReportService(
    participantId: Ref.ParticipantId,
    store: MeteringStore,
    clock: () => ProtoTimestamp = () => toProtoTimestamp(Timestamp.now()),
)(implicit
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends MeteringReportService
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val generator = new MeteringReportGenerator(participantId)

  override def bindService(): ServerServiceDefinition =
    MeteringReportServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  override def getMeteringReport(
      request: GetMeteringReportRequest
  ): Future[GetMeteringReportResponse] = {
    logger.info(s"Received metering report request: $request")

    (for {
      protoFrom <- request.from.toRight("from date must be specified")
      from <- toTimestamp(protoFrom)
      to <- request.to.fold[Either[String, Option[Timestamp]]](Right(None))(t =>
        toTimestamp(t).map(Some.apply)
      )
      applicationId <- toOption(request.applicationId)
        .fold[Either[String, Option[Ref.ApplicationId]]](Right(None))(t =>
          Ref.ApplicationId.fromString(t).map(Some.apply)
        )
    } yield {
      val reportTime = clock()
      store.getMeteringReportData(from, to, applicationId).map { reportData =>
        generator.generate(request, reportData, reportTime)
      }
    }) match {
      case Right(f) => f
      case Left(error) =>
        Future.failed(
          ValidationLogger.logFailure(request, ValidationErrors.invalidArgument(error))
        )
    }
  }
}

private[apiserver] object ApiMeteringReportService {

  def toOption(protoString: String): Option[String] = {
    if (protoString.nonEmpty) Some(protoString) else None
  }

  def toProtoTimestamp(ts: Timestamp): ProtoTimestamp = {
    ts.toInstant.pipe { i => ProtoTimestamp.of(i.getEpochSecond, i.getNano) }
  }

  def toTimestamp(ts: ProtoTimestamp): Either[String, Timestamp] = {
    val utcTs =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(ts.seconds, ts.nanos.toLong), ZoneOffset.UTC)
    for {
      ts <-
        if (utcTs.truncatedTo(ChronoUnit.HOURS) == utcTs)
          Timestamp.fromInstant(utcTs.toInstant)
        else Left(s"Timestamp must be rounded to the hour: $utcTs")
    } yield ts
  }

  class MeteringReportGenerator(participantId: Ref.ParticipantId) {
    def generate(
        request: GetMeteringReportRequest,
        reportData: ReportData,
        generationTime: ProtoTimestamp,
    ): GetMeteringReportResponse = {

      val applicationReports = reportData.applicationData.toList
        .sortBy(_._1)
        .map((ApplicationMeteringReport.apply _).tupled)

      val report = ParticipantMeteringReport(
        participantId,
        isFinal = reportData.isFinal,
        applicationReports,
      )

      GetMeteringReportResponse(
        request = Some(request),
        participantReport = Some(report),
        reportGenerationTime = Some(generationTime),
      )

    }
  }

}
