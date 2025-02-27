// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import cats.syntax.either.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.InspectionServiceErrorGroup
import com.digitalasset.canton.error.{CantonError, ContextualizedCantonError}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{GrpcFUSExtended, wrapErrUS}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.InFlightCount
import com.digitalasset.canton.participant.pruning.{
  CommitmentContractMetadata,
  CommitmentInspectContract,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerAliasManager
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriodState,
  ReceivedAcsCommitment,
  SentAcsCommitment,
  SynchronizerSearchCommitmentPeriod,
}
import com.digitalasset.canton.protocol.{LfContractId, SynchronizerParametersLookup}
import com.digitalasset.canton.pruning.{
  ConfigForSlowCounterParticipants,
  ConfigForSynchronizerThresholds,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.{IndexedStringStore, IndexedSynchronizer}
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import com.digitalasset.daml.lf.data.Bytes
import io.grpc.stub.StreamObserver

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

class GrpcInspectionService(
    syncStateInspection: SyncStateInspection,
    ips: IdentityProvidingServiceClient,
    indexedStringStore: IndexedStringStore,
    synchronizerAliasManager: SynchronizerAliasManager,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v30.InspectionServiceGrpc.InspectionService
    with NamedLogging {

  override def lookupOffsetByTime(
      request: v30.LookupOffsetByTimeRequest
  ): Future[v30.LookupOffsetByTimeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    request.timestamp
      .fold[FutureUnlessShutdown[v30.LookupOffsetByTimeResponse]](
        FutureUnlessShutdown.failed(new IllegalArgumentException(s"""Timestamp not specified"""))
      ) { ts =>
        CantonTimestamp.fromProtoTimestamp(ts) match {
          case Right(cantonTimestamp) =>
            syncStateInspection
              .getOffsetByTime(cantonTimestamp)
              .map(ledgerOffset => v30.LookupOffsetByTimeResponse(ledgerOffset))
          case Left(err) =>
            FutureUnlessShutdown.failed(
              new IllegalArgumentException(s"""Failed to parse timestamp: $err""")
            )
        }
      }
      .asGrpcResponse
  }

  /** Configure metrics for slow counter-participants (i.e., that are behind in sending commitments)
    * and configure thresholds for when a counter-participant is deemed slow.
    *
    * returns error if synchronizer ids re not distinct.
    */
  override def setConfigForSlowCounterParticipants(
      request: v30.SetConfigForSlowCounterParticipantsRequest
  ): Future[v30.SetConfigForSlowCounterParticipantsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result = for {
      mapped <- wrapErrUS(
        request.configs
          .flatMap(ConfigForSlowCounterParticipants.fromProto30)
          .sequence
      )
      allSynchronizerIds = request.configs.flatMap(_.synchronizerIds)

      mappedDistinct <- EitherT.cond[FutureUnlessShutdown](
        allSynchronizerIds.distinct.lengthIs == allSynchronizerIds.length,
        mapped,
        InspectionServiceError.IllegalArgumentError.Error(
          "synchronizerIds are not distinct"
        ),
      )

      mappedThreshold <- wrapErrUS(
        request.configs
          .flatMap(ConfigForSynchronizerThresholds.fromProto30)
          .sequence
      )

      _ <- EitherTUtil
        .fromFuture(
          syncStateInspection.addOrUpdateConfigsForSlowCounterParticipants(
            mappedDistinct,
            mappedThreshold,
          ),
          err => InspectionServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]
    } yield v30.SetConfigForSlowCounterParticipantsResponse()

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** Get the current configuration for metrics for slow counter-participants.
    */
  override def getConfigForSlowCounterParticipants(
      request: v30.GetConfigForSlowCounterParticipantsRequest
  ): Future[v30.GetConfigForSlowCounterParticipantsResponse] =
    TraceContextGrpc.withGrpcTraceContext { implicit traceContext =>
      {
        for {
          allConfigs <- syncStateInspection.getConfigsForSlowCounterParticipants()
          (slowCounterParticipants, synchronizerThresholds) = allConfigs
          filtered =
            if (request.synchronizerIds.isEmpty) slowCounterParticipants
            else
              slowCounterParticipants.filter(config =>
                request.synchronizerIds.contains(config.synchronizerId.toProtoPrimitive)
              )
          filteredWithThreshold = filtered
            .map(cfg =>
              (
                cfg,
                synchronizerThresholds
                  .find(dThresh => dThresh.synchronizerId == cfg.synchronizerId),
              )
            )
            .flatMap {
              case (cfg, Some(thresh)) => Some(cfg.toProto30(thresh))
              case (_, None) => None
            }
            .groupBy(_.synchronizerIds)
            .map { case (synchronizers, configs) =>
              configs.foldLeft(
                new v30.SlowCounterParticipantSynchronizerConfig(
                  Seq.empty,
                  Seq.empty,
                  -1,
                  -1,
                  Seq.empty,
                )
              ) { (next, previous) =>
                new v30.SlowCounterParticipantSynchronizerConfig(
                  synchronizers, // since we have them grouped by synchronizerId, these are the same
                  next.distinguishedParticipantUids ++ previous.distinguishedParticipantUids,
                  Math.max(
                    next.thresholdDistinguished,
                    previous.thresholdDistinguished,
                  ), // we use max since they will have th same value (unless it uses the default from fold)
                  Math.max(
                    next.thresholdDefault,
                    previous.thresholdDefault,
                  ), // we use max since they will have th same value (unless it uses the default from fold)
                  next.participantUidsMetrics ++ previous.participantUidsMetrics,
                )
              }
            }
            .toSeq

        } yield v30.GetConfigForSlowCounterParticipantsResponse(
          filteredWithThreshold
        )
      }.asGrpcResponse
    }

  /** Get the number of intervals that counter-participants are behind in sending commitments. Can
    * be used to decide whether to ignore slow counter-participants w.r.t. pruning.
    */
  override def getIntervalsBehindForCounterParticipants(
      request: v30.GetIntervalsBehindForCounterParticipantsRequest
  ): Future[v30.GetIntervalsBehindForCounterParticipantsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      synchronizers <- wrapErrUS(
        request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      )
      participants <- wrapErrUS(
        request.counterParticipantUids.traverse(
          ParticipantId.fromProtoPrimitive(_, "counter_participant_uid")
        )
      )
      intervals <- EitherTUtil
        .fromFuture(
          syncStateInspection
            .getIntervalsBehindForParticipants(synchronizers, participants),
          err => InspectionServiceError.InternalServerError.Error(err.toString),
        )
        .leftWiden[CantonError]

      mapped = intervals
        .filter(interval =>
          participants.contains(
            interval.participantId
          ) || (participants.isEmpty && request.threshold
            .fold(true)(interval.intervalsBehind.value > _))
        )
        .map(_.toProtoV30)
    } yield {
      v30.GetIntervalsBehindForCounterParticipantsResponse(mapped.toSeq)
    }

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** Look up the ACS commitments computed and sent by a participant
    */
  override def lookupSentAcsCommitments(
      request: v30.LookupSentAcsCommitmentsRequest
  ): Future[v30.LookupSentAcsCommitmentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {

      synchronizerSearchPeriodsF <-
        if (request.timeRanges.isEmpty) fetchDefaultSynchronizerTimeRanges()
        else
          request.timeRanges
            .traverse(dtr => validateSynchronizerTimeRange(dtr))
      result <- for {
        counterParticipantIds <- request.counterParticipantUids.traverse(pId =>
          ParticipantId
            .fromProtoPrimitive(pId, s"counter_participant_uids")
            .leftMap(err => err.message)
        )
        states <- request.commitmentState
          .map(CommitmentPeriodState.fromProtoV30)
          .sequence
          .leftMap(err => err.message)
      } yield {
        for {
          synchronizerSearchPeriods <- Future.sequence(synchronizerSearchPeriodsF)
        } yield syncStateInspection
          .crossSynchronizerSentCommitmentMessages(
            synchronizerSearchPeriods,
            counterParticipantIds,
            states,
            request.verbose,
          )
      }
    } yield result

    result
      .fold(
        string => Future.failed(new IllegalArgumentException(string)),
        _.map {
          case Left(string) => Future.failed(new IllegalArgumentException(string))
          case Right(value) =>
            Future.successful(
              v30.LookupSentAcsCommitmentsResponse(SentAcsCommitment.toProtoV30(value))
            )
        },
      )
      .flatten
  }

  /** List the counter-participants of a participant and their ACS commitments together with the
    * match status TODO(#18749) R1 Can also be used for R1, to fetch commitments that a counter
    * participant received from myself
    */
  override def lookupReceivedAcsCommitments(
      request: v30.LookupReceivedAcsCommitmentsRequest
  ): Future[v30.LookupReceivedAcsCommitmentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      synchronizerSearchPeriodsF <-
        if (request.timeRanges.isEmpty) fetchDefaultSynchronizerTimeRanges()
        else
          request.timeRanges
            .traverse(dtr => validateSynchronizerTimeRange(dtr))

      result <- for {
        counterParticipantIds <- request.counterParticipantUids.traverse(pId =>
          ParticipantId
            .fromProtoPrimitive(pId, s"counter_participant_uids")
            .leftMap(err => err.message)
        )
        states <- request.commitmentState
          .map(CommitmentPeriodState.fromProtoV30)
          .sequence
          .leftMap(err => err.message)
      } yield {
        for {
          synchronizerSearchPeriods <- Future.sequence(synchronizerSearchPeriodsF)
        } yield syncStateInspection
          .crossSynchronizerReceivedCommitmentMessages(
            synchronizerSearchPeriods,
            counterParticipantIds,
            states,
            request.verbose,
          )
      }
    } yield result
    result match {
      case Left(string) => Future.failed(new IllegalArgumentException(string))
      case Right(value) =>
        value.map {
          case Left(string) => Future.failed(new IllegalArgumentException(string))
          case Right(value) =>
            Future.successful(
              v30.LookupReceivedAcsCommitmentsResponse(ReceivedAcsCommitment.toProtoV30(value))
            )
        }.flatten
    }
  }

  private def fetchDefaultSynchronizerTimeRanges()(implicit
      traceContext: TraceContext
  ): Either[String, Seq[Future[SynchronizerSearchCommitmentPeriod]]] = {
    val searchPeriods = synchronizerAliasManager.ids.map(synchronizerId =>
      for {
        synchronizerAlias <- synchronizerAliasManager
          .aliasForSynchronizerId(synchronizerId)
          .toRight(s"No synchronizer alias found for $synchronizerId")
        lastComputed <- syncStateInspection
          .findLastComputedAndSent(synchronizerAlias)
          .toRight(s"No computations done for $synchronizerId")
      } yield {
        for {
          indexedSynchronizer <- IndexedSynchronizer
            .indexed(indexedStringStore)(synchronizerId)
            .failOnShutdownToAbortException("fetchDefaultSynchronizerTimeRanges")
        } yield SynchronizerSearchCommitmentPeriod(
          indexedSynchronizer,
          lastComputed.forgetRefinement,
          lastComputed.forgetRefinement,
        )
      }
    )

    val (lefts, rights) = searchPeriods.partitionMap(identity)

    NonEmpty.from(lefts) match {
      case Some(leftsNe) => Left(leftsNe.head1)
      case None => Right(rights.toSeq)
    }

  }

  private def validateSynchronizerTimeRange(
      timeRange: v30.SynchronizerTimeRange
  )(implicit
      traceContext: TraceContext
  ): Either[String, Future[SynchronizerSearchCommitmentPeriod]] =
    for {
      synchronizerId <- SynchronizerId.fromString(timeRange.synchronizerId)

      synchronizerAlias <- synchronizerAliasManager
        .aliasForSynchronizerId(synchronizerId)
        .toRight(s"No synchronizer alias found for $synchronizerId")
      lastComputed <- syncStateInspection
        .findLastComputedAndSent(synchronizerAlias)
        .toRight(s"No computations done for $synchronizerId")

      times <- timeRange.interval
        // when no timestamp is given we make a TimeRange of (lastComputedAndSentTs,lastComputedAndSentTs), this should give the latest elements since the comparison later on is inclusive.
        .fold(
          Right(
            v30.TimeRange(Some(lastComputed.toProtoTimestamp), Some(lastComputed.toProtoTimestamp))
          )
        )(Right(_))
        .flatMap(interval =>
          for {
            min <- interval.fromExclusive
              .toRight(s"Missing start timestamp for ${timeRange.synchronizerId}")
              .flatMap(ts => CantonTimestamp.fromProtoTimestamp(ts).left.map(_.message))
            max <- interval.toInclusive
              .toRight(s"Missing end timestamp for ${timeRange.synchronizerId}")
              .flatMap(ts => CantonTimestamp.fromProtoTimestamp(ts).left.map(_.message))
          } yield (min, max)
        )
      (start, end) = times

    } yield {
      for {
        indexedSynchronizer <- IndexedSynchronizer
          .indexed(indexedStringStore)(synchronizerId)
          .failOnShutdownToAbortException("validateSynchronizerTimeRange")
      } yield SynchronizerSearchCommitmentPeriod(indexedSynchronizer, start, end)
    }

  /** Request metadata about shared contracts used in commitment computation at a specific time
    * Subject to the data still being available on the participant TODO(#9557) R2
    */
  override def openCommitment(
      request: v30.OpenCommitmentRequest,
      responseObserver: StreamObserver[v30.OpenCommitmentResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => openCommitment(request, out),
      responseObserver,
      byteString => v30.OpenCommitmentResponse(byteString),
    )

  private def openCommitment(
      request: v30.OpenCommitmentRequest,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result =
      for {
        // 1. Check that the commitment to open matches a sent local commitment
        synchronizerId <- CantonGrpcUtil.wrapErrUS(
          SynchronizerId.fromProtoPrimitive(request.synchronizerId, "synchronizerId")
        )

        synchronizerAlias <- EitherT
          .fromOption[FutureUnlessShutdown](
            synchronizerAliasManager.aliasForSynchronizerId(synchronizerId),
            InspectionServiceError.IllegalArgumentError
              .Error(s"Unknown synchronizer id $synchronizerId"),
          )
          .leftWiden[CantonError]

        pv <- EitherTUtil
          .fromFuture(
            syncStateInspection.getProtocolVersion(synchronizerAlias),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        cantonTickTs <- CantonGrpcUtil.wrapErrUS(
          ProtoConverter.parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "periodEndTick",
            request.periodEndTick,
          )
        )

        counterParticipant <- CantonGrpcUtil.wrapErrUS(
          ParticipantId
            .fromProtoPrimitive(
              request.computedForCounterParticipantUid,
              s"computedForCounterParticipantUid",
            )
        )

        computedCmts = syncStateInspection.findComputedCommitments(
          synchronizerAlias,
          cantonTickTs,
          cantonTickTs,
          Some(counterParticipant),
        )

        // 2. Retrieve the contracts for the synchronizer and the time of the commitment

        topologySnapshot <- EitherT.fromOption[FutureUnlessShutdown](
          ips.forSynchronizer(synchronizerId),
          InspectionServiceError.InternalServerError.Error(
            s"Failed to retrieve ips for synchronizer: $synchronizerId"
          ),
        )

        snapshot <- EitherTUtil
          .fromFuture(
            topologySnapshot.awaitSnapshot(cantonTickTs),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        // Check that the given timestamp is a valid tick. We cannot move this check up, because .get(cantonTickTs)
        // would wait if the timestamp is in the future. Here, we already validated that the timestamp is in the past
        // by checking it corresponds to an already computed commitment, and already obtained a snapshot at the
        // given timestamp.
        synchronizerParamsF <- EitherTUtil
          .fromFuture(
            SynchronizerParametersLookup
              .forAcsCommitmentSynchronizerParameters(
                pv,
                topologySnapshot,
                loggerFactory,
              )
              .get(cantonTickTs, warnOnUsingDefaults = false),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        _ <- EitherT.fromEither[FutureUnlessShutdown](
          CantonTimestampSecond
            .fromCantonTimestamp(cantonTickTs)
            .flatMap(ts =>
              Either.cond(
                ts.getEpochSecond % synchronizerParamsF.reconciliationInterval.duration.getSeconds == 0,
                (),
                "",
              )
            )
            .leftMap[CantonError](_ =>
              InspectionServiceError.IllegalArgumentError.Error(
                s"""The participant cannot open commitment ${request.commitment} for participant
                   | ${request.computedForCounterParticipantUid} on synchronizer ${request.synchronizerId} because the given
                   | period end tick ${request.periodEndTick} is not a valid reconciliation interval tick""".stripMargin
              )
            )
        )

        requestCommitment <- EitherT.fromEither[FutureUnlessShutdown](
          AcsCommitment
            .hashedCommitmentTypeFromByteString(request.commitment)
            .leftMap[CantonError](err =>
              InspectionServiceError.IllegalArgumentError
                .Error(s"Failed to parse commitment hash: $err")
            )
        )

        _ <- EitherT.cond[FutureUnlessShutdown](
          computedCmts.exists { case (period, participant, cmt) =>
            period.fromExclusive < cantonTickTs && period.toInclusive >= cantonTickTs && participant == counterParticipant && cmt == requestCommitment
          },
          (),
          InspectionServiceError.IllegalArgumentError.Error(
            s"""The participant cannot open commitment ${request.commitment} for participant
            ${request.computedForCounterParticipantUid} on synchronizer ${request.synchronizerId} and period end
            ${request.periodEndTick} because the participant has not computed such a commitment at the given tick timestamp for the given counter participant""".stripMargin
          ): CantonError,
        )

        counterParticipantParties <- EitherTUtil
          .fromFuture(
            snapshot
              .inspectKnownParties(
                filterParty = "",
                filterParticipant = counterParticipant.filterString,
              ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        contractsAndTransferCounter <- EitherTUtil
          .fromFuture(
            syncStateInspection.activeContractsStakeholdersFilter(
              synchronizerId,
              cantonTickTs,
              counterParticipantParties.map(_.toLf),
            ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        commitmentContractsMetadata = contractsAndTransferCounter.map {
          case (cid, transferCounter) =>
            CommitmentContractMetadata.create(cid, transferCounter)(pv)
        }

      } yield {
        commitmentContractsMetadata.foreach(c => c.writeDelimitedTo(out).foreach(_ => out.flush()))
      }

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  /** TODO(#9557) R2
    */
  override def inspectCommitmentContracts(
      request: v30.InspectCommitmentContractsRequest,
      responseObserver: StreamObserver[v30.InspectCommitmentContractsResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => inspectCommitmentContracts(request, out),
      responseObserver,
      byteString => v30.InspectCommitmentContractsResponse(byteString),
    )

  private def inspectCommitmentContracts(
      request: v30.InspectCommitmentContractsRequest,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result =
      for {
        expectedSynchronizerId <- CantonGrpcUtil.wrapErrUS(
          SynchronizerId.fromProtoPrimitive(request.expectedSynchronizerId, "synchronizerId")
        )

        expectedSynchronizerAlias <- EitherT
          .fromOption[FutureUnlessShutdown](
            synchronizerAliasManager.aliasForSynchronizerId(expectedSynchronizerId),
            InspectionServiceError.IllegalArgumentError
              .Error(s"Synchronizer alias not found for $expectedSynchronizerId"),
          )
          .leftWiden[CantonError]

        cantonTs <- CantonGrpcUtil.wrapErrUS(
          ProtoConverter.parseRequired(
            CantonTimestamp.fromProtoTimestamp,
            "timestamp",
            request.timestamp,
          )
        )

        pv <- EitherTUtil
          .fromFuture(
            syncStateInspection.getProtocolVersion(expectedSynchronizerAlias),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        cids <-
          EitherT.fromEither[FutureUnlessShutdown](
            request.cids
              .traverse(cid => LfContractId.fromBytes(Bytes.fromByteString(cid)))
              .leftMap(InspectionServiceError.IllegalArgumentError.Error(_))
              .leftWiden[CantonError]
          )

        contractStates <- EitherTUtil
          .fromFuture(
            CommitmentInspectContract
              .inspectContractState(
                cids,
                expectedSynchronizerId,
                cantonTs,
                request.downloadPayload,
                syncStateInspection,
                indexedStringStore,
                pv,
              ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

      } yield {
        contractStates.foreach(c => c.writeDelimitedTo(out).foreach(_ => out.flush()))
      }
    CantonGrpcUtil.mapErrNewEUS(result)
  }

  override def countInFlight(
      request: v30.CountInFlightRequest
  ): Future[v30.CountInFlightResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val inFlightCount: EitherT[FutureUnlessShutdown, String, InFlightCount] = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId.fromString(request.synchronizerId)
      )
      synchronizerAlias <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerAliasManager
          .aliasForSynchronizerId(synchronizerId)
          .toRight(s"Not able to find synchronizer alias for ${synchronizerId.toString}")
      )

      count <- syncStateInspection.countInFlight(synchronizerAlias)
    } yield count

    inFlightCount
      .fold(
        err => throw new IllegalArgumentException(err),
        count =>
          v30.CountInFlightResponse(
            count.pendingSubmissions.unwrap,
            count.pendingTransactions.unwrap,
          ),
      )
      .asGrpcResponse

  }
}

object InspectionServiceError extends InspectionServiceErrorGroup {
  sealed trait InspectionServiceError extends ContextualizedCantonError

  @Explanation("""Inspection has failed because of an internal server error.""")
  @Resolution("Identify the error in the server log.")
  object InternalServerError
      extends ErrorCode(
        id = "INTERNAL_INSPECTION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "An error occurred in the inspection service: " + reason
        )
        with InspectionServiceError
  }

  @Explanation("""Inspection has failed because of an illegal argument.""")
  @Resolution(
    "Identify the illegal argument in the error details of the gRPC status message that the call returned."
  )
  object IllegalArgumentError
      extends ErrorCode(
        id = "ILLEGAL_ARGUMENT_INSPECTION_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause = "The inspection service received an illegal argument: " + reason
        )
        with InspectionServiceError
  }
}
