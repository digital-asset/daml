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

import scala.concurrent.{ExecutionContext, Future}

class SandboxResetService(
    getLedgerId: () => String,
    getServer: () => Server,
    getEc: () => ExecutionContext,
    closeAllServices: () => Unit,
    resetAndStartServer: () => Unit)
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
              new ApiException(Status.ABORTED.withDescription("Server is not live"))))({ server =>
              actuallyReset(server)
              Future.successful(new Empty())
            })
        }
      )

  }
  private def actuallyReset(server: Server) = {
    logger.info("Initiating server reset.")
    server.shutdown()
    logger.info("Closing all services...")
    closeAllServices()

    // We need to run this asynchronously since otherwise we have a deadlock: `buildAndStartServer` will block
    // until all the in flight requests have been served, so we need to schedule this in another thread so that
    // the code that clears the in flight request is not in an in flight request itself.
    getEc().execute({ () =>
      logger.info(s"Awaiting termination...")
      if (!server.awaitTermination(1L, TimeUnit.SECONDS)) {
        logger.warn(
          "Server did not terminate gracefully in one second. " +
            "Clients probably did not disconnect. " +
            "Proceeding with forced termination.")
        server.shutdownNow()
      }
      logger.info(s"Rebuilding server...")
      resetAndStartServer()
      logger.info(s"Server reset complete.")
    })
  }
}
