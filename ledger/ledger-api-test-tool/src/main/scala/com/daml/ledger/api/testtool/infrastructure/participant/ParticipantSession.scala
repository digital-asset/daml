// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.LedgerServices
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSession.logger
import com.daml.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest
import com.daml.timer.RetryStrategy
import io.grpc.ManagedChannel
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

private[infrastructure] final class ParticipantSession(
    config: ParticipantSessionConfiguration,
    channel: ManagedChannel,
    eventLoopGroup: NioEventLoopGroup,
    services: LedgerServices,
    // The ledger ID is retrieved only once when the participant session is created.
    // Changing the ledger ID during a session can result in unexpected consequences.
    // The test tool is designed to run tests in an isolated environment but changing the
    // global state of the ledger breaks this assumption, no matter what.
    ledgerId: String,
)(implicit val executionContext: ExecutionContext) {

  private[testtool] def createInitContext(
      applicationId: String,
      identifierSuffix: String,
  ): Future[ParticipantTestContext] =
    createTestContext("init", applicationId, identifierSuffix)

  private[testtool] def createTestContext(
      endpointId: String,
      applicationId: String,
      identifierSuffix: String,
  ): Future[ParticipantTestContext] =
    for {
      end <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(ledgerId)).map(_.getOffset)
    } yield
      new ParticipantTestContext(
        ledgerId,
        endpointId,
        applicationId,
        identifierSuffix,
        end,
        services,
        config.partyAllocation,
      )

  private[testtool] def close(): Unit = {
    logger.info(s"Disconnecting from participant at ${config.host}:${config.port}...")
    channel.shutdownNow()
    if (!channel.awaitTermination(10L, SECONDS)) {
      sys.error("Channel shutdown stuck. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to participant at ${config.host}:${config.port} shut down.")
    if (!eventLoopGroup
        .shutdownGracefully(0, 0, SECONDS)
        .await(10L, SECONDS)) {
      sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to participant at ${config.host}:${config.port} closed.")
  }
}

object ParticipantSession {
  private val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  def apply(
      config: ParticipantSessionConfiguration,
      channel: ManagedChannel,
      eventLoopGroup: NioEventLoopGroup,
  )(implicit executionContext: ExecutionContext): Future[ParticipantSession] = {
    val services = new LedgerServices(channel)
    for {
      ledgerId <- RetryStrategy.exponentialBackoff(10, 10.millis) { (attempt, wait) =>
        logger.debug(
          s"Fetching ledgerId to create context (attempt #$attempt, next one in $wait)...")
        services.identity.getLedgerIdentity(new GetLedgerIdentityRequest).map(_.ledgerId)
      }
    } yield new ParticipantSession(config, channel, eventLoopGroup, services, ledgerId)
  }
}
