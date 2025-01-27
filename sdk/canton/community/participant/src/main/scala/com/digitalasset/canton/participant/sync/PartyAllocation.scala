// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.parallelInstanceFutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.{LedgerServerPartyNotifier, PartyOps}
import com.digitalasset.canton.topology.TopologyManagerError.MappingAlreadyExists
import com.digitalasset.canton.topology.{ParticipantId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerSubmissionId, LfPartyId}
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.CompletionStage
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*
import scala.util.chaining.*

private[sync] class PartyAllocation(
    participantId: ParticipantId,
    partyOps: PartyOps,
    partyNotifier: LedgerServerPartyNotifier,
    isActive: () => Boolean,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, val tracer: Tracer)
    extends Spanning
    with NamedLogging {
  def allocate(
      hint: LfPartyId,
      rawSubmissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): CompletionStage[SubmissionResult] =
    withSpan("CantonSyncService.allocateParty") { implicit traceContext => span =>
      span.setAttribute("submission_id", rawSubmissionId)

      allocateInternal(hint, rawSubmissionId)
    }.asJava

  private def allocateInternal(
      partyName: LfPartyId,
      rawSubmissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
    import com.google.rpc.status.Status
    import io.grpc.Status.Code

    def reject(reason: String, statusCode: Option[Code]): SubmissionResult.SynchronousError =
      SubmissionResult.SynchronousError(
        Status.of(statusCode.getOrElse(Code.UNKNOWN).value(), reason, Seq())
      )

    val protocolVersion = ProtocolVersion.latest

    val result =
      for {
        _ <- EitherT
          .cond[FutureUnlessShutdown](isActive(), (), SyncServiceError.Synchronous.PassiveNode)
          .leftWiden[SubmissionResult]
        id <- UniqueIdentifier
          .create(partyName, participantId.uid.namespace)
          .leftMap(SyncServiceError.Synchronous.internalError)
          .toEitherT[FutureUnlessShutdown]
        partyId = PartyId(id)
        validatedSubmissionId <- EitherT.fromEither[FutureUnlessShutdown](
          String255
            .fromProtoPrimitive(rawSubmissionId, "LedgerSubmissionId")
            .leftMap(err => SyncServiceError.Synchronous.internalError(err.toString))
        )
        // Allow party allocation via ledger API only if the participant is connected to a synchronizer
        // Otherwise the gRPC call will just timeout without a meaningful error message
        _ <- EitherT.cond[FutureUnlessShutdown](
          connectedSynchronizersLookup.snapshot.nonEmpty,
          (),
          SubmissionResult.SynchronousError(
            SyncServiceError.PartyAllocationNoSynchronizerError.Error(rawSubmissionId).rpcStatus()
          ),
        )
        _ <- partyNotifier
          .expectPartyAllocationForNodes(
            partyId,
            participantId,
            validatedSubmissionId,
          )
          .leftMap[SubmissionResult] { err =>
            reject(err, Some(Code.ABORTED))
          }
          .toEitherT[FutureUnlessShutdown]
        _ <- partyOps
          .allocateParty(partyId, participantId, protocolVersion)
          .leftMap[SubmissionResult] {
            case IdentityManagerParentError(e) if e.code == MappingAlreadyExists =>
              reject(
                show"Party already exists: party $partyId is already allocated on this node",
                e.code.category.grpcCode,
              )
            case IdentityManagerParentError(e) => reject(e.cause, e.code.category.grpcCode)
            case e => reject(e.toString, Some(Code.INTERNAL))
          }
          .leftMap { x =>
            partyNotifier.expireExpectedPartyAllocationForNodes(
              partyId,
              participantId,
              validatedSubmissionId,
            )
            x
          }

        // TODO(i21341) remove this waiting logic once topology events are published on the ledger api
        // wait for parties to be available on the currently connected synchronizers
        waitingSuccessful <- EitherT
          .right[SubmissionResult](connectedSynchronizersLookup.snapshot.toSeq.parTraverse {
            case (synchronizerId, connectedSynchronizer) =>
              connectedSynchronizer.topologyClient
                .awaitUS(
                  _.inspectKnownParties(partyId.filterString, participantId.filterString)
                    .map(_.nonEmpty),
                  timeouts.network.duration,
                )
                .map(synchronizerId -> _)
          })
        _ = waitingSuccessful.foreach { case (synchronizerId, successful) =>
          if (!successful)
            logger.warn(
              s"Waiting for allocation of $partyId on synchronizer $synchronizerId timed out."
            )
        }

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
  }.failOnShutdownToAbortException("Party Allocation")
}
