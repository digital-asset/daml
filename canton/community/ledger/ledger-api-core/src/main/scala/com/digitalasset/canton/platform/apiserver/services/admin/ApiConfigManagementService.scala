// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.config_management_service.*
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.util.DurationConversion
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.logging.LoggingContextUtil.createLoggingContext
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.*
import com.digitalasset.canton.platform.apiserver.services.logging
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiConfigManagementService private (
    index: IndexConfigManagementService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends ConfigManagementService
    with GrpcApiService
    with NamedLogging {

  private implicit val loggingContext: LoggingContext =
    createLoggingContext(loggerFactory)(identity)

  override def close(): Unit = {}

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, executionContext)

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)

    logger.info("Getting time model.")
    index
      .lookupConfiguration()
      .flatMap {
        case Some((_, configuration)) =>
          Future.successful(configurationToResponse(configuration))
        case None =>
          Future.failed(
            RequestValidationErrors.NotFound.LedgerConfiguration
              .Reject()(
                ErrorLoggingContext(
                  logger,
                  loggingContext.toPropertiesMap,
                  loggingContext.traceContext,
                )
              )
              .asGrpcError
          )
      }
      .andThen(logger.logErrorsOnCall[GetTimeModelResponse])
  }

  private def configurationToResponse(configuration: Configuration): GetTimeModelResponse = {
    val timeModel = configuration.timeModel
    GetTimeModelResponse(
      configurationGeneration = configuration.generation,
      timeModel = Some(
        TimeModel(
          avgTransactionLatency = Some(DurationConversion.toProto(timeModel.avgTransactionLatency)),
          minSkew = Some(DurationConversion.toProto(timeModel.minSkew)),
          maxSkew = Some(DurationConversion.toProto(timeModel.maxSkew)),
        )
      ),
    )
  }

  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] =
    withEnrichedLoggingContext(telemetry)(
      logging.submissionId(request.submissionId)
    ) { implicit loggingContext =>
      logger.info(s"Setting time model, ${loggingContext.serializeFiltered("submissionId")}.")
      Future.failed(TransactionError.NotSupported.exception)
    }
}

private[apiserver] object ApiConfigManagementService {

  def createApiService(
      readBackend: IndexConfigManagementService,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): ConfigManagementServiceGrpc.ConfigManagementService & GrpcApiService =
    new ApiConfigManagementService(
      readBackend,
      telemetry,
      loggerFactory,
    )
}
