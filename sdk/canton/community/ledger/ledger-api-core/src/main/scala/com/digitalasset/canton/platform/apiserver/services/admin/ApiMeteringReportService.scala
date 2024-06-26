// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.admin.metering_report_service.MeteringReportServiceGrpc.MeteringReportService
import com.daml.ledger.api.v2.admin.metering_report_service.*
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.ValidationErrors
import com.digitalasset.canton.ledger.error.groups.AdminServiceErrors
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.apiserver.meteringreport.{
  MeteringReportGenerator,
  MeteringReportKey,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiMeteringReportService.*
import com.google.protobuf.timestamp.Timestamp as ProtoTimestamp
import io.grpc.{ServerServiceDefinition, StatusRuntimeException}

import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

private[apiserver] final class ApiMeteringReportService(
    participantId: Ref.ParticipantId,
    store: MeteringStore,
    meteringReportKey: MeteringReportKey,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
    clock: () => ProtoTimestamp = () => toProtoTimestamp(Timestamp.now()),
)(implicit
    executionContext: ExecutionContext
) extends MeteringReportService
    with GrpcApiService
    with NamedLogging {

  private val generator = new MeteringReportGenerator(participantId, meteringReportKey.key)

  override def bindService(): ServerServiceDefinition =
    MeteringReportServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()

  private def validateRequest(
      request: GetMeteringReportRequest
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, (Timestamp, Option[Timestamp], Option[ApplicationId])] = {
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
    } yield (from, to, applicationId)).left.map(ValidationErrors.invalidArgument)
  }

  private def generateReport(
      request: GetMeteringReportRequest,
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
      reportData: ReportData,
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, GetMeteringReportResponse] = {
    generator.generate(request, from, to, applicationId, reportData, clock()).left.map { e =>
      AdminServiceErrors.InternallyInvalidKey.Reject(e).asGrpcError
    }
  }

  override def getMeteringReport(
      request: GetMeteringReportRequest
  ): Future[GetMeteringReportResponse] = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    logger.info(s"Received metering report request: $request.")

    implicit class WrapEither[T](either: Either[StatusRuntimeException, T]) {
      def toFuture: Future[T] = either.fold(
        e => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, e)),
        Future.successful,
      )
    }

    for {
      (from, to, applicationId) <- validateRequest(request).toFuture
      reportData <- store.getMeteringReportData(from, to, applicationId)
      report <- generateReport(request, from, to, applicationId, reportData).toFuture
    } yield report
  }
}

private[apiserver] object ApiMeteringReportService {

  private def toOption(protoString: String): Option[String] = {
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

}
