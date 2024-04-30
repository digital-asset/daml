// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.config.PartyNotificationConfig
import com.digitalasset.canton.participant.store.ParticipantNodeEphemeralState
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyManagerOps,
}
import com.digitalasset.canton.topology.TopologyManagerError.MappingAlreadyExists
import com.digitalasset.canton.topology.{Identifier, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerSubmissionId, LfPartyId, LfTimestamp}
import io.opentelemetry.api.trace.Tracer

import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.chaining.*

private[sync] class PartyAllocation(
    participantId: ParticipantId,
    participantNodeEphemeralState: ParticipantNodeEphemeralState,
    topologyManagerOps: ParticipantTopologyManagerOps,
    partyNotifier: LedgerServerPartyNotifier,
    parameters: ParticipantNodeParameters,
    isActive: () => Boolean,
    connectedDomainsLookup: ConnectedDomainsLookup,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, val tracer: Tracer)
    extends Spanning
    with NamedLogging {
  def allocate(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): CompletionStage[SubmissionResult] = {
    withSpan("CantonSyncService.allocateParty") { implicit traceContext => span =>
      span.setAttribute("submission_id", rawSubmissionId)

      allocateInternal(hint, displayName, rawSubmissionId)
    }.asJava
  }

  private def allocateInternal(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
    def reject(reason: String, result: SubmissionResult): SubmissionResult = {
      publishReject(reason, rawSubmissionId, displayName, result)
      result
    }

    val partyName = hint.getOrElse(s"party-${UUID.randomUUID().toString}")
    val protocolVersion = ProtocolVersion.latest

    val result =
      for {
        _ <- EitherT
          .cond[Future](isActive(), (), SyncServiceError.Synchronous.PassiveNode)
          .leftWiden[SubmissionResult]
        id <- Identifier
          .create(partyName)
          .leftMap(SyncServiceError.Synchronous.internalError)
          .toEitherT[Future]
        partyId = PartyId(id, participantId.namespace)
        validatedDisplayName <- displayName
          .traverse(n => String255.create(n, Some("DisplayName")))
          .leftMap(SyncServiceError.Synchronous.internalError)
          .toEitherT[Future]
        validatedSubmissionId <- EitherT.fromEither[Future](
          String255
            .fromProtoPrimitive(rawSubmissionId, "LedgerSubmissionId")
            .leftMap(err => SyncServiceError.Synchronous.internalError(err.toString))
        )
        // Allow party allocation via ledger API only if notification is Eager or the participant is connected to a domain
        // Otherwise the gRPC call will just timeout without a meaning error message
        _ <- EitherT.cond[Future](
          parameters.partyChangeNotification == PartyNotificationConfig.Eager ||
            connectedDomainsLookup.snapshot.nonEmpty,
          (),
          SubmissionResult.SynchronousError(
            SyncServiceError.PartyAllocationNoDomainError.Error(rawSubmissionId).rpcStatus()
          ),
        )
        _ <- partyNotifier
          .expectPartyAllocationForNodes(
            partyId,
            participantId,
            validatedSubmissionId,
            validatedDisplayName,
          )
          .leftMap[SubmissionResult] { err =>
            reject(err, SubmissionResult.Acknowledged)
          }
          .toEitherT[Future]
        _ <- topologyManagerOps
          .allocateParty(validatedSubmissionId, partyId, participantId, protocolVersion)
          .leftMap[SubmissionResult] {
            case IdentityManagerParentError(e) if e.code == MappingAlreadyExists =>
              reject(
                show"Party already exists: party $partyId is already allocated on this node",
                SubmissionResult.Acknowledged,
              )
            case IdentityManagerParentError(e) => reject(e.cause, SubmissionResult.Acknowledged)
            case e => reject(e.toString, SyncServiceError.Synchronous.internalError(e.toString))
          }
          .leftMap { x =>
            partyNotifier.expireExpectedPartyAllocationForNodes(
              partyId,
              participantId,
              validatedSubmissionId,
            )
            x
          }
          .onShutdown(Left(SyncServiceError.Synchronous.shutdownError))

      } yield SubmissionResult.Acknowledged

    result.fold(
      _.tap { l =>
        logger.info(
          s"Failed to allocate party $partyName::${participantId.namespace}: ${l.toString}"
        )
      },
      _.tap { _ =>
        logger.debug(s"Allocated party $partyName::${participantId.namespace}")
      },
    )
  }

  private def publishReject(
      reason: String,
      rawSubmissionId: LedgerSubmissionId,
      displayName: Option[String],
      result: SubmissionResult,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    FutureUtil.doNotAwait(
      participantNodeEphemeralState.participantEventPublisher
        .publish(
          LedgerSyncEvent.PartyAllocationRejected(
            rawSubmissionId,
            participantId.toLf,
            recordTime =
              LfTimestamp.Epoch, // The actual record time will be filled in by the ParticipantEventPublisher
            rejectionReason = reason,
          )
        )
        .onShutdown(
          logger.debug(s"Aborted publishing of party allocation rejection due to shutdown")
        ),
      s"Failed to publish allocation rejection for party $displayName",
    )
  }

}
