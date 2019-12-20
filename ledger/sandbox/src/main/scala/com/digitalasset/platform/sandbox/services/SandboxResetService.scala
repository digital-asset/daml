// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.util.concurrent.atomic.AtomicBoolean

import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.dec.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.google.protobuf.empty.Empty
import io.grpc.ServerCall.Listener
import io.grpc._

import scala.concurrent.{ExecutionContext, Future, Promise}

class SandboxResetService(
    ledgerId: LedgerId,
    getEc: () => ExecutionContext,
    resetAndRestartServer: () => Future[Unit],
    authorizer: Authorizer,
    loggerFactory: NamedLoggerFactory)
    extends ResetServiceGrpc.ResetService
    with BindableService
    with ServerInterceptor {

  private val logger = loggerFactory.getLogger(this.getClass)

  private val resetInitialized = new AtomicBoolean(false)

  override def bindService(): ServerServiceDefinition =
    ResetServiceGrpc.bindService(this, DE)

  override def reset(request: ResetRequest): Future[Empty] =
    authorizer.requireAdminClaims(doReset)(request)

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      metadata: Metadata,
      serverCallHandler: ServerCallHandler[ReqT, RespT]): Listener[ReqT] = {
    if (resetInitialized.get) {
      throw new StatusRuntimeException(
        Status.UNAVAILABLE.withDescription("Sandbox server is currently being resetted"))
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
        ErrorFactories.ledgerIdMismatch(ledgerId, LedgerId(request.ledgerId)))
      .fold(Future.failed[Empty], _ => actuallyReset().map(_ => Empty())(DE))

  private def actuallyReset() = {
    logger.info("Initiating server reset.")

    if (!resetInitialized.compareAndSet(false, true))
      throw new StatusRuntimeException(
        Status.FAILED_PRECONDITION.withDescription("Sandbox server is currently being resetted"))

    val servicesAreDown = Promise[Unit]()
    // We need to run this asynchronously since otherwise we have a deadlock: `buildAndStartServer` will block
    // until all the in flight requests have been served, so we need to schedule this in another thread so that
    // the code that clears the in flight request is not in an in flight request itself.
    getEc().execute({ () =>
      logger.info(s"Stopping and starting the server.")
      servicesAreDown.completeWith(resetAndRestartServer())
    })
    servicesAreDown.future
  }
}
