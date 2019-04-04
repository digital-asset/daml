// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services
import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.ApiException
import com.digitalasset.platform.server.api.validation.CommandValidations
import com.google.protobuf.empty.Empty
import io.grpc.{BindableService, Server, ServerServiceDefinition, Status}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}

class SandboxResetService(
    getLedgerId: () => String,
    getServer: () => Server,
    getEc: () => ExecutionContext,
    closeAllServices: () => Unit,
    buildAndStartServer: () => Unit)
    extends ResetServiceGrpc.ResetService
    with BindableService {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def bindService(): ServerServiceDefinition =
    ResetServiceGrpc.bindService(this, DirectExecutionContext)

  override def reset(request: ResetRequest): Future[Empty] = {

    // to reset:
    // * initiate a graceful shutdown -- note that this won't kill the in-flight requests, including
    //   the reset request itself we're serving;
    // * serve the response to the reset request;
    // * then, close all the services so hopefully the graceful shutdown will terminate quickly...
    // * ...but not before serving the request to the reset request itself, which we've already done.
    CommandValidations
      .matchLedgerId(getLedgerId())(request.ledgerId)
      .fold(
        Future.failed[Empty], { _ =>
          Option(getServer())
            .fold(Future.failed[Empty](
              new ApiException(Status.ABORTED.withDescription("Server is not live"))))({
              actuallyReset
            })
        }
      )

  }
  private def actuallyReset(server: Server) = {
    logger.info("Initiating server reset.")
    server.shutdown()
    logger.info("Closing all services...")
    closeAllServices()

    // If completing the returned future races with andThen (or its execution), the shutdown will hang.
    // This is why we complete the Future after the callback.
    // The root cause of this behavior has been investigated yet.
    val p = Promise[Empty]()
    p.future
      .andThen({
        case _ =>
          logger.info("Awaiting termination...")
          if (!server.awaitTermination(1L, TimeUnit.SECONDS)) {
            logger.warn(
              "Server did not terminate gracefully in one second. " +
                "Clients probably did not disconnect. " +
                "Proceeding with forced termination.")
            server.shutdownNow()
          }
          logger.info("Rebuilding server...")
          buildAndStartServer()
          logger.info("Server reset complete.")
      })(getEc())
    p.success(Empty())
    p.future
  }
}
