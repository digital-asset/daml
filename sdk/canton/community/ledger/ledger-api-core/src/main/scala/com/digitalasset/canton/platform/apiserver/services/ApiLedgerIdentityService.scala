// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService as GrpcLedgerIdentityService
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{BindableService, ServerServiceDefinition}
import scalaz.syntax.tag.*

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiLedgerIdentityService private (
    getLedgerId: () => Future[LedgerId],
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends GrpcLedgerIdentityService
    with GrpcApiService
    with NamedLogging {

  val closed: AtomicBoolean = new AtomicBoolean(false)

  @nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] = {
    implicit val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())

    logger.info(s"Received request for ledger identity: $request.")
    if (closed.get())
      Future.failed(CommonErrors.ServiceNotRunning.Reject("Ledger Identity Service").asGrpcError)
    else
      getLedgerId()
        .map(ledgerId => GetLedgerIdentityResponse(ledgerId.unwrap))
        .andThen(logger.logErrorsOnCall[GetLedgerIdentityResponse])
  }

  override def close(): Unit = closed.set(true)

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, executionContext): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )
}

private[apiserver] object ApiLedgerIdentityService {
  def create(ledgerId: LedgerId, telemetry: Telemetry, loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext
  ): ApiLedgerIdentityService with BindableService = {
    new ApiLedgerIdentityService(() => Future.successful(ledgerId), telemetry, loggerFactory)
  }
}
