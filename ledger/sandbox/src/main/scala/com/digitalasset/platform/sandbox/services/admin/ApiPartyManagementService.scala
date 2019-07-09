// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.admin

import akka.stream.Materializer
import com.daml.ledger.participant.state.index.v2.IndexPartyManagementService
import com.daml.ledger.participant.state.v2.{PartyAllocationResult, WritePartyService}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.{DirectExecutionContext => DE}
import com.digitalasset.platform.server.api.validation.ErrorFactories
import io.grpc.ServerServiceDefinition
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future, blocking}

class ApiPartyManagementService private (
    partyManagementService: IndexPartyManagementService,
    writeService: WritePartyService
) extends PartyManagementService
    with GrpcApiService {

  protected val logger = LoggerFactory.getLogger(this.getClass)

  override def close(): Unit = ()

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DE)

  override def getParticipantId(
      request: GetParticipantIdRequest): Future[GetParticipantIdResponse] =
    partyManagementService
      .getParticipantId()
      .map(pid => GetParticipantIdResponse(pid.toString))(DE)

  private[this] def mapPartyDetails(
      details: com.digitalasset.ledger.api.domain.PartyDetails): PartyDetails =
    PartyDetails(details.party, details.displayName.getOrElse(""), details.isLocal)

  override def listKnownParties(
      request: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    partyManagementService
      .listParties()
      .map(ps => ListKnownPartiesResponse(ps.map(mapPartyDetails)))(DE)

  /**
    * Continuously polls the party management service to check if a party has been persisted.
    *
    * Despite the `go` inner function not being stack-safe per se, only one stack frame will be on
    * the stack at any given time since the "recursive" invocation happens on a different thread.
    *
    * The backoff waiting time are applied after the first poll returns without a result (i.e. the first call is not delayed).
    *
    * @param party The party whose persistence we're waiting for
    * @param minWait The minimum waiting time - will not be enforced if less than `maxWait`
    *                Does not make sense to set this lower than the OS scheduler threshold
    *                Anyway always padded to 50 milliseconds
    * @param maxWait The maximum waiting time - takes precedence over `minWait` and `backoffProgression`
    *                Does not make sense to set this lower than the OS scheduler threshold
    *                Anyway always padded to 50 milliseconds
    * @param backoffProgression How the following backoff time is computed as a function of the current one - `maxWait` takes precedence though
    * @return The number of attempts before the party was found wrapped in a [[Future]]
    */
  private def pollUntilPersisted(
      party: String,
      minWait: Duration,
      maxWait: Duration,
      backoffProgression: Duration => Duration): Future[Int] = {
    def go(party: String, attempt: Int, waitTime: Duration): Future[Int] = {
      logger.debug(s"Polling for party '$party' being persisted (attempt #$attempt)...")
      partyManagementService
        .listParties()
        .flatMap {
          case persisted if persisted.exists(_.party == party) => Future.successful(attempt)
          case _ =>
            logger.debug(s"Party '$party' not yet persisted, backing off for $waitTime...")
            Future(blocking { Thread.sleep(waitTime.toMillis) })(DE).flatMap(
              _ =>
                go(
                  party,
                  attempt + 1,
                  backoffProgression(waitTime).min(maxWait).max(50.milliseconds)))(DE)
        }(DE)
    }
    go(party, 1, minWait.min(maxWait).max(50.milliseconds))
  }

  /**
    * Wraps a call [[pollUntilPersisted]] so that it can be chained on the party allocation with a `flatMap`.
    *
    * Checks invariants and forwards the original result after the party is found to be persisted.
    *
    * @param result The result of the party allocation
    * @return The result of the party allocation received originally, wrapped in a [[Future]]
    */
  private def pollUntilPersisted(
      result: AllocatePartyResponse,
      minWait: Duration,
      maxWait: Duration,
      iteration: Duration => Duration): Future[AllocatePartyResponse] = {
    require(result.partyDetails.isDefined, "Party allocation response must have the party details")
    pollUntilPersisted(result.partyDetails.get.party, minWait, maxWait, iteration).map {
      numberOfAttempts =>
        logger.debug(s"Party available, read after $numberOfAttempts attempt(s)")
        result
    }(DE)
  }

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] = {
    val party = if (request.partyIdHint.isEmpty) None else Some(request.partyIdHint)
    val displayName = if (request.displayName.isEmpty) None else Some(request.displayName)

    FutureConverters
      .toScala(writeService
        .allocateParty(party, displayName))
      .flatMap {
        case PartyAllocationResult.Ok(details) =>
          Future.successful(AllocatePartyResponse(Some(mapPartyDetails(details))))
        case r @ PartyAllocationResult.AlreadyExists =>
          Future.failed(ErrorFactories.invalidArgument(r.description))
        case r @ PartyAllocationResult.InvalidName(_) =>
          Future.failed(ErrorFactories.invalidArgument(r.description))
        case r @ PartyAllocationResult.ParticipantNotAuthorized =>
          Future.failed(ErrorFactories.permissionDenied(r.description))
      }(DE)
      .flatMap(pollUntilPersisted(_, 50.milliseconds, 500.milliseconds, (d: Duration) => d * 2))(DE)
  }

}

object ApiPartyManagementService {
  def createApiService(readBackend: IndexPartyManagementService, writeBackend: WritePartyService)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer): GrpcApiService =
    new ApiPartyManagementService(readBackend, writeBackend) with PartyManagementServiceLogging

}
