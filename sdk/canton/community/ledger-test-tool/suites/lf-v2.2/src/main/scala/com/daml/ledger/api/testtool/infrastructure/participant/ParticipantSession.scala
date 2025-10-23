// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.ChannelEndpoint.JsonApiEndpoint
import com.daml.ledger.api.testtool.infrastructure.{
  ChannelEndpoint,
  Errors,
  LedgerServices,
  PartyAllocationConfiguration,
  RetryingGetConnectedSynchronizersForParty,
}
import com.daml.ledger.api.v2.admin.party_management_service.GetParticipantIdRequest
import com.daml.ledger.api.v2.state_service.GetLedgerEndRequest
import com.daml.ledger.api.v2.version_service.GetLedgerApiVersionRequest
import com.daml.timer.RetryStrategy
import io.grpc.ClientInterceptor
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.NonFatal

/** Represents a running participant server exposing a set of services.
  */
private[infrastructure] final class ParticipantSession private (
    partyAllocationConfig: PartyAllocationConfiguration,
    services: LedgerServices,
    ledgerEndpoint: Either[JsonApiEndpoint, ChannelEndpoint],
    adminEndpoint: ChannelEndpoint,
    val features: Features,
    timeoutScaleFactor: Double,
)(implicit val executionContext: ExecutionContext) {

  private[testtool] def createInitContext(
      userId: String,
      identifierSuffix: String,
      features: Features,
  ): Future[ParticipantTestContext] =
    createTestContext(
      "init",
      userId,
      identifierSuffix,
      features = features,
    )

  private[testtool] def createTestContext(
      endpointId: String,
      userId: String,
      identifierSuffix: String,
      features: Features,
  ): Future[ParticipantTestContext] =
    for {
      end <- services.state
        .getLedgerEnd(new GetLedgerEndRequest())
        .map(_.offset)
      participantId <- services.partyManagement
        .getParticipantId(new GetParticipantIdRequest())
        .map(_.participantId)
    } yield new TimeoutParticipantTestContext(
      timeoutScaleFactor,
      new SingleParticipantTestContext(
        endpointId = endpointId,
        userId = userId,
        identifierSuffix = identifierSuffix,
        referenceOffset = end,
        services = services,
        partyAllocationConfig = partyAllocationConfig,
        ledgerEndpoint = ledgerEndpoint,
        adminEndpoint = adminEndpoint,
        features = features,
        participantId = participantId,
      ),
    )
}

object ParticipantSession {
  private val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  def createSessions(
      partyAllocationConfig: PartyAllocationConfiguration,
      participantChannels: Either[Vector[JsonApiEndpoint], Vector[ChannelEndpoint]],
      participantAdminChannels: Vector[ChannelEndpoint],
      maxConnectionAttempts: Int,
      commandInterceptors: Seq[ClientInterceptor],
      timeoutScaleFactor: Double,
      darList: List[String],
      connectedSynchronizers: Int,
  )(implicit
      executionContext: ExecutionContext
  ): Future[Vector[ParticipantSession]] = {
    val participantChannelsS = participantChannels match {
      case Left(value) => value.map(Left(_))
      case Right(value) => value.map(Right(_))
    }
    Future.traverse(participantChannelsS.zip(participantAdminChannels)) {
      case (endpoint: Either[JsonApiEndpoint, ChannelEndpoint], adminEndpoint) =>
        val services = LedgerServices(endpoint.map(_.channel), commandInterceptors, darList)
        for {
          features <- RetryStrategy
            .exponentialBackoff(attempts = maxConnectionAttempts, 100.millis) { (attempt, wait) =>
              services.version
                .getLedgerApiVersion(new GetLedgerApiVersionRequest())
                .map(Features.fromApiVersionResponse)
                .andThen { case Failure(_) =>
                  logger.info(
                    s"Could not connect to the participant (attempt #$attempt). Trying again in $wait..."
                  )
                }
            }
            .recoverWith { case NonFatal(exception) =>
              Future.failed(new Errors.ParticipantConnectionException(exception))
            }
          // There's no Ledger API endpoint that allows querying the connected synchronizers of a participant
          // Instead, use the participant's admin party
          participantId <- services.partyManagement.getParticipantId(GetParticipantIdRequest())
          synchronizers <- RetryingGetConnectedSynchronizersForParty(
            services,
            participantId.participantId,
            minSynchronizers = connectedSynchronizers,
          )
        } yield {
          if (synchronizers.sizeIs != connectedSynchronizers)
            throw new RuntimeException(
              s"Configured for $connectedSynchronizers connected synchronizers, but found ${synchronizers.size})"
            )
          new ParticipantSession(
            partyAllocationConfig = partyAllocationConfig,
            services = services,
            ledgerEndpoint = endpoint,
            adminEndpoint = adminEndpoint,
            features = features,
            timeoutScaleFactor = timeoutScaleFactor,
          )
        }
    }
  }
}
