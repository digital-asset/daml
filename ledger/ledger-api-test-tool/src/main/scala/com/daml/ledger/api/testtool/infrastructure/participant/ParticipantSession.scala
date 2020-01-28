// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import java.time.Duration

import com.daml.ledger.api.testtool.infrastructure.LedgerServices
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
}
import com.digitalasset.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.digitalasset.ledger.api.v1.transaction_service.GetLedgerEndRequest
import com.digitalasset.platform.testing.StreamConsumer
import com.digitalasset.timer.RetryStrategy
import io.grpc.ManagedChannel
import io.netty.channel.nio.NioEventLoopGroup
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, SECONDS}
import scala.concurrent.{ExecutionContext, Future}

private[participant] final class ParticipantSession(
    val config: ParticipantSessionConfiguration,
    channel: ManagedChannel,
    eventLoopGroup: NioEventLoopGroup,
)(implicit val executionContext: ExecutionContext) {

  private[this] val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  private[this] val services: LedgerServices = new LedgerServices(channel)

  // The ledger identifier is retrieved only once when the participant session is created
  // Changing the ledger identifier during the execution of a session can result in unexpected consequences
  // The test tool is designed to run tests in an isolated environment but changing the
  // global state of the ledger breaks this assumption, no matter what
  private[this] val ledgerIdF =
    RetryStrategy.exponentialBackoff(10, 10.millis) { (attempt, wait) =>
      logger.debug(s"Fetching ledgerId to create context (attempt #$attempt, next one in $wait)...")
      services.identity.getLedgerIdentity(new GetLedgerIdentityRequest).map(_.ledgerId)
    }

  // The time-to-live for commands defaults to the maximum value as defined by the ledger
  // configuration and can be adjusted down. Regardless the output of the adjustment, the
  // value is always going to be clipped by the minimum and maximum configured values.
  // Note that this value is going to never change after it's read the first time, so changing
  // ledger configuration across tests may have wildly unexpected consequences. In general the
  // test tool is designed to have tests work in isolation and tests addressing changes to the
  // global state of the ledger should be isolated in their own test runs.
  private[this] val ttlF: Future[Duration] =
    ledgerIdF
      .flatMap { id =>
        new StreamConsumer[GetLedgerConfigurationResponse](
          services.configuration
            .getLedgerConfiguration(new GetLedgerConfigurationRequest(id), _),
        ).first()
          .map(_.get.getLedgerConfiguration)
      }
      .map { configuration =>
        val factor = config.commandTtlFactor
        val min = configuration.getMinTtl.asScala
        val max = configuration.getMaxTtl.asScala
        val ttl = (max * factor).min(max).max(min)
        logger.info(s"Command TTL is $ttl (min: $min, max: $max, factor: $factor)")
        Duration.ofNanos(ttl.toNanos)
      }

  private[testtool] def createTestContext(
      endpointId: String,
      applicationId: String,
      identifierSuffix: String,
  ): Future[ParticipantTestContext] =
    for {
      ledgerId <- ledgerIdF
      ttl <- ttlF
      end <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(ledgerId)).map(_.getOffset)
    } yield new ParticipantTestContext(
      ledgerId,
      endpointId,
      applicationId,
      identifierSuffix,
      end,
      services,
      ttl,
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
