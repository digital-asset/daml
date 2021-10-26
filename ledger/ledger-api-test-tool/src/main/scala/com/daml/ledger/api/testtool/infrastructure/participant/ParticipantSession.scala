// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.{
  ChannelEndpoint,
  Endpoint,
  Errors,
  LedgerServices,
  PartyAllocationConfiguration,
}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.daml.ledger.api.v1.transaction_service.GetLedgerEndRequest
import com.daml.ledger.api.v1.version_service.GetLedgerApiVersionRequest
import com.daml.timer.RetryStrategy
import io.grpc.ClientInterceptor
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.NonFatal

private[infrastructure] final class ParticipantSession private (
    partyAllocation: PartyAllocationConfiguration,
    services: LedgerServices,
    // The ledger ID is retrieved only once when the participant session is created.
    // Changing the ledger ID during a session can result in unexpected consequences.
    // The test tool is designed to run tests in an isolated environment but changing the
    // global state of the ledger breaks this assumption, no matter what.
    ledgerId: String,
    ledgerEndpoint: Endpoint,
    val features: Features,
)(implicit val executionContext: ExecutionContext) {

  private[testtool] def createInitContext(
      applicationId: String,
      identifierSuffix: String,
      clientTlsConfiguration: Option[TlsConfiguration],
      features: Features,
  ): Future[ParticipantTestContext] =
    createTestContext(
      "init",
      applicationId,
      identifierSuffix,
      clientTlsConfiguration = clientTlsConfiguration,
      features = features,
    )

  private[testtool] def createTestContext(
      endpointId: String,
      applicationId: String,
      identifierSuffix: String,
      clientTlsConfiguration: Option[TlsConfiguration],
      features: Features,
  ): Future[ParticipantTestContext] =
    for {
      end <- services.transaction.getLedgerEnd(new GetLedgerEndRequest(ledgerId)).map(_.getOffset)
    } yield new ParticipantTestContext(
      ledgerId = ledgerId,
      endpointId = endpointId,
      applicationId = applicationId,
      identifierSuffix = identifierSuffix,
      referenceOffset = end,
      services = services,
      partyAllocation = partyAllocation,
      ledgerEndpoint = ledgerEndpoint,
      clientTlsConfiguration = clientTlsConfiguration,
      features = features,
    )
}

object ParticipantSession {
  private val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  def apply(
      partyAllocation: PartyAllocationConfiguration,
      participants: Vector[ChannelEndpoint],
      maxConnectionAttempts: Int,
      commandInterceptors: Seq[ClientInterceptor],
  )(implicit
      executionContext: ExecutionContext
  ): Future[Vector[ParticipantSession]] =
    Future.traverse(participants) { participant: ChannelEndpoint =>
      val services = new LedgerServices(participant.channel, commandInterceptors)
      for {
        ledgerId <- RetryStrategy
          .exponentialBackoff(attempts = maxConnectionAttempts, 100.millis) { (attempt, wait) =>
            services.identity
              .getLedgerIdentity(new GetLedgerIdentityRequest)
              .map(_.ledgerId)
              .andThen { case Failure(_) =>
                logger.info(
                  s"Could not connect to the participant (attempt #$attempt). Trying again in $wait..."
                )
              }
          }
          .recoverWith { case NonFatal(exception) =>
            Future.failed(new Errors.ParticipantConnectionException(exception))
          }
        features <-
          services.version
            .getLedgerApiVersion(new GetLedgerApiVersionRequest(ledgerId))
            .map(Features.fromApiVersionResponse)
            .recover { case failure =>
              // TODO feature descriptors: Remove once all Ledger API implementations respond successfully on VersionService endpoint
              logger.warn(
                s"Failure in retrieving the feature descriptors from the version service: $failure"
              )
              Features(Seq.empty)
            }
      } yield new ParticipantSession(
        partyAllocation = partyAllocation,
        services = services,
        ledgerId = ledgerId,
        ledgerEndpoint = participant.endpoint,
        features = features,
      )
    }
}
