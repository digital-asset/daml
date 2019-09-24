// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.util.concurrent.TimeUnit

import com.daml.ledger.api.testtool.infrastructure.{LedgerServices, RetryStrategy}
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndRequest
import io.grpc.ManagedChannel
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

private[participant] final class ParticipantSession(
    val config: ParticipantSessionConfiguration,
    channel: ManagedChannel,
    eventLoopGroup: NioEventLoopGroup)(implicit val executionContext: ExecutionContext) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  private[this] val services: LedgerServices = new LedgerServices(channel)

  private[this] val eventually = RetryStrategy.exponentialBackoff(10, 10.millis)

  private[testtool] def createTestContext(
      endpointId: String,
      applicationId: String,
      identifierSuffix: String): Future[ParticipantTestContext] =
    eventually { attempt =>
      logger.debug(s"Creating ledger context (attempt #$attempt)...")
      for {
        id <- services.identity.getLedgerIdentity(new GetLedgerIdentityRequest).map(_.ledgerId)
        end <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(id)).map(_.getOffset)
      } yield
        new ParticipantTestContext(
          id,
          endpointId,
          applicationId,
          identifierSuffix,
          end,
          services,
          config.commandTtlFactor)
    }

  private[testtool] def close(): Unit = {
    logger.info(s"Disconnecting from participant at ${config.host}:${config.port}...")
    channel.shutdownNow()
    if (!channel.awaitTermination(10L, TimeUnit.SECONDS)) {
      sys.error("Channel shutdown stuck. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to participant at ${config.host}:${config.port} shut down.")
    if (!eventLoopGroup
        .shutdownGracefully(0, 0, TimeUnit.SECONDS)
        .await(10L, TimeUnit.SECONDS)) {
      sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to participant at ${config.host}:${config.port} closed.")
  }

}
