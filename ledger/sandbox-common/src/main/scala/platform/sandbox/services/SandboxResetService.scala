// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.concurrent.atomic.AtomicBoolean
import com.daml.dec.{DirectExecutionContext => DE}
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.protobuf.empty.Empty
import io.grpc.ServerCall.Listener
import io.grpc._

import scala.concurrent.Future

class SandboxResetService(
    ledgerId: LedgerId,
    resetAndRestartServer: () => Future[Unit],
    authorizer: Authorizer,
    errorFactories: ErrorFactories,
)(implicit loggingContext: LoggingContext)
    extends ResetServiceGrpc.ResetService
    with BindableService
    with ServerInterceptor {

  private val logger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  private val resetInitialized = new AtomicBoolean(false)

  override def bindService(): ServerServiceDefinition =
    ResetServiceGrpc.bindService(this, DE)

  override def reset(request: ResetRequest): Future[Empty] =
    authorizer.requireAdminClaims(doReset)(request)

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      metadata: Metadata,
      serverCallHandler: ServerCallHandler[ReqT, RespT],
  ): Listener[ReqT] = {
    if (resetInitialized.get) {
      throw errorFactories
        .serviceIsBeingReset(Status.Code.UNAVAILABLE.value())("Sandbox server")
    }

    serverCallHandler.startCall(serverCall, metadata)
  }

  // to reset:
  // * initiate a graceful shutdown -- note that this won't kill the in-flight requests, including
  //   the reset request itself we're serving;
  // * serve the response to the reset request;
  // * then, close all the services so hopefully the graceful shutdown will terminate quickly...
  // * ...but not before serving the request to the reset request itself, which we've already done.
  private def doReset(request: ResetRequest): Future[Empty] =
    Either
      .cond(
        ledgerId == LedgerId(request.ledgerId),
        request.ledgerId,
        errorFactories.ledgerIdMismatch(ledgerId, LedgerId(request.ledgerId), None),
      )
      .fold(Future.failed[Empty], _ => actuallyReset().map(_ => Empty())(DE))

  private def actuallyReset() = {
    logger.info("Initiating server reset.")

    if (!resetInitialized.compareAndSet(false, true)) {
      throw errorFactories
        .serviceIsBeingReset(Status.Code.FAILED_PRECONDITION.value())("Sandbox server")
    }

    logger.info(s"Stopping and starting the server.")
    resetAndRestartServer()
  }
}
