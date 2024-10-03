// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.{toBifunctorOps, toTraverseOps}
import cats.syntax.either.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.participant.v30.InspectionServiceGrpc.InspectionService
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.InspectionServiceErrorGroup
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.InFlightCount
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.pruning.CommitmentContractMetadata
import com.digitalasset.canton.protocol.DomainParametersLookup
import com.digitalasset.canton.protocol.messages.{
  CommitmentPeriodState,
  DomainSearchCommitmentPeriod,
  ReceivedAcsCommitment,
  SentAcsCommitment,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils}
import io.grpc.stub.StreamObserver

import java.io.OutputStream
import scala.concurrent.{ExecutionContext, Future}

class GrpcInspectionService(
    syncStateInspection: SyncStateInspection,
    ips: IdentityProvidingServiceClient,
    indexedStringStore: IndexedStringStore,
    domainAliasManager: DomainAliasManager,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends InspectionService
    with NamedLogging {

  override def lookupOffsetByTime(
      request: LookupOffsetByTime.Request
  ): Future[LookupOffsetByTime.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    request.timestamp.fold[Future[LookupOffsetByTime.Response]](
      Future.failed(new IllegalArgumentException(s"""Timestamp not specified"""))
    ) { ts =>
      CantonTimestamp.fromProtoTimestamp(ts) match {
        case Right(cantonTimestamp) =>
          syncStateInspection
            .getOffsetByTime(cantonTimestamp)
            .map(ledgerOffset => LookupOffsetByTime.Response(ledgerOffset.getOrElse("")))
        case Left(err) =>
          Future.failed(new IllegalArgumentException(s"""Failed to parse timestamp: $err"""))
      }
    }
  }

  /** Configure metrics for slow counter-participants (i.e., that are behind in sending commitments) and
    * configure thresholds for when a counter-participant is deemed slow.
    * TODO(#10436) R7
    */
  override def setConfigForSlowCounterParticipants(
      request: SetConfigForSlowCounterParticipants.Request
  ): Future[SetConfigForSlowCounterParticipants.Response] = ???

  /** Get the current configuration for metrics for slow counter-participants.
    * TODO(#10436) R7
    */
  override def getConfigForSlowCounterParticipants(
      request: GetConfigForSlowCounterParticipants.Request
  ): Future[GetConfigForSlowCounterParticipants.Response] = ???

  /** Get the number of intervals that counter-participants are behind in sending commitments.
    * Can be used to decide whether to ignore slow counter-participants w.r.t. pruning.
    * TODO(#10436) R7
    */
  override def getIntervalsBehindForCounterParticipants(
      request: GetIntervalsBehindForCounterParticipants.Request
  ): Future[GetIntervalsBehindForCounterParticipants.Response] = ???

  /** Look up the ACS commitments computed and sent by a participant
    */
  override def lookupSentAcsCommitments(
      request: LookupSentAcsCommitments.Request
  ): Future[LookupSentAcsCommitments.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {

      domainSearchPeriodsF <-
        if (request.timeRanges.isEmpty) fetchDefaultDomainTimeRanges()
        else
          request.timeRanges
            .traverse(dtr => validateDomainTimeRange(dtr))
      result <- for {
        counterParticipantIds <- request.counterParticipantUids.traverse(pId =>
          ParticipantId
            .fromProtoPrimitive(pId, s"counter_participant_uids")
            .leftMap(err => err.message)
        )
        states <- request.commitmentState
          .map(CommitmentPeriodState.fromProtoV30)
          .foldRight(Right(Seq.empty): ParsingResult[Seq[CommitmentPeriodState]]) { (either, acc) =>
            for {
              xs <- acc
              x <- either
            } yield x +: xs
          }
          .leftMap(err => err.message)
      } yield {
        for {
          domainSearchPeriods <- Future.sequence(domainSearchPeriodsF)
        } yield syncStateInspection
          .crossDomainSentCommitmentMessages(
            domainSearchPeriods,
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
              LookupSentAcsCommitments.Response(SentAcsCommitment.toProtoV30(value))
            )
        },
      )
      .flatten
  }

  /** List the counter-participants of a participant and their ACS commitments together with the match status
    * TODO(#18749) R1 Can also be used for R1, to fetch commitments that a counter participant received from myself
    */
  override def lookupReceivedAcsCommitments(
      request: LookupReceivedAcsCommitments.Request
  ): Future[LookupReceivedAcsCommitments.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      domainSearchPeriodsF <-
        if (request.timeRanges.isEmpty) fetchDefaultDomainTimeRanges()
        else
          request.timeRanges
            .traverse(dtr => validateDomainTimeRange(dtr))

      result <- for {
        counterParticipantIds <- request.counterParticipantUids.traverse(pId =>
          ParticipantId
            .fromProtoPrimitive(pId, s"counter_participant_uids")
            .leftMap(err => err.message)
        )
        states <- request.commitmentState
          .map(CommitmentPeriodState.fromProtoV30)
          .foldRight(Right(Seq.empty): ParsingResult[Seq[CommitmentPeriodState]]) { (either, acc) =>
            for {
              xs <- acc
              x <- either
            } yield x +: xs
          }
          .leftMap(err => err.message)
      } yield {
        for {
          domainSearchPeriods <- Future.sequence(domainSearchPeriodsF)
        } yield syncStateInspection
          .crossDomainReceivedCommitmentMessages(
            domainSearchPeriods,
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
              LookupReceivedAcsCommitments.Response(ReceivedAcsCommitment.toProtoV30(value))
            )
        }.flatten
    }
  }

  private def fetchDefaultDomainTimeRanges()(implicit
      traceContext: TraceContext
  ): Either[String, Seq[Future[DomainSearchCommitmentPeriod]]] = {
    val searchPeriods = domainAliasManager.ids.map(domainId =>
      for {
        domainAlias <- domainAliasManager
          .aliasForDomainId(domainId)
          .toRight(s"No domain alias found for $domainId")
        lastComputed <- syncStateInspection
          .findLastComputedAndSent(domainAlias)
          .toRight(s"No computations done for $domainId")
      } yield {
        for {
          indexedDomain <- IndexedDomain.indexed(indexedStringStore)(domainId)
        } yield DomainSearchCommitmentPeriod(
          indexedDomain,
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

  private def validateDomainTimeRange(
      timeRange: DomainTimeRange
  )(implicit
      traceContext: TraceContext
  ): Either[String, Future[DomainSearchCommitmentPeriod]] =
    for {
      domainId <- DomainId.fromString(timeRange.domainId)

      domainAlias <- domainAliasManager
        .aliasForDomainId(domainId)
        .toRight(s"No domain alias found for $domainId")
      lastComputed <- syncStateInspection
        .findLastComputedAndSent(domainAlias)
        .toRight(s"No computations done for $domainId")

      times <- timeRange.interval
        // when no timestamp is given we make a TimeRange of (lastComputedAndSentTs,lastComputedAndSentTs), this should give the latest elements since the comparison later on is inclusive.
        .fold(
          Right(TimeRange(Some(lastComputed.toProtoTimestamp), Some(lastComputed.toProtoTimestamp)))
        )(Right(_))
        .flatMap(interval =>
          for {
            min <- interval.fromExclusive
              .toRight(s"Missing start timestamp for ${timeRange.domainId}")
              .flatMap(ts => CantonTimestamp.fromProtoTimestamp(ts).left.map(_.message))
            max <- interval.toInclusive
              .toRight(s"Missing end timestamp for ${timeRange.domainId}")
              .flatMap(ts => CantonTimestamp.fromProtoTimestamp(ts).left.map(_.message))
          } yield (min, max)
        )
      (start, end) = times

    } yield {
      for {
        indexedDomain <- IndexedDomain.indexed(indexedStringStore)(domainId)
      } yield DomainSearchCommitmentPeriod(indexedDomain, start, end)
    }

  /** Request metadata about shared contracts used in commitment computation at a specific time
    * Subject to the data still being available on the participant
    * TODO(#9557) R2
    */
  override def openCommitment(
      request: OpenCommitment.Request,
      responseObserver: StreamObserver[OpenCommitment.Response],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => openCommitment(request, out),
      responseObserver,
      byteString => OpenCommitment.Response(byteString),
    )

  private def openCommitment(
      request: OpenCommitment.Request,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val result =
      for {
        // 1. Check that the commitment to open matches a sent local commitment
        domainId <- CantonGrpcUtil.wrapErrUS(
          DomainId.fromProtoPrimitive(request.domainId, "domainId")
        )

        domainAlias <- EitherT
          .fromOption[FutureUnlessShutdown](
            domainAliasManager.aliasForDomainId(domainId),
            InspectionServiceError.IllegalArgumentError
              .Error(s"Unknown domain ID $domainId"),
          )
          .leftWiden[CantonError]

        pv <- EitherTUtil
          .fromFuture(
            FutureUnlessShutdown.outcomeF(syncStateInspection.getProtocolVersion(domainAlias)),
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
          domainAlias,
          cantonTickTs,
          cantonTickTs,
          Some(counterParticipant),
        )

        // 2. Retrieve the contracts for the domain and the time of the commitment

        topologySnapshot <- EitherT.fromOption[FutureUnlessShutdown](
          ips.forDomain(domainId),
          InspectionServiceError.InternalServerError.Error(
            s"Failed to retrieve ips for domain: $domainId"
          ),
        )

        snapshot <- EitherTUtil
          .fromFuture(
            topologySnapshot.awaitSnapshotUS(cantonTickTs),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        // Check that the given timestamp is a valid tick. We cannot move this check up, because .get(cantonTickTs)
        // would wait if the timestamp is in the future. Here, we already validated that the timestamp is in the past
        // by checking it corresponds to an already computed commitment, and already obtained a snapshot at the
        // given timestamp.
        domainParamsF <- EitherTUtil
          .fromFuture(
            FutureUnlessShutdown.outcomeF(
              DomainParametersLookup
                .forAcsCommitmentDomainParameters(
                  pv,
                  topologySnapshot,
                  futureSupervisor,
                  loggerFactory,
                )
                .get(cantonTickTs, false)
            ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        _ <- EitherT.fromEither[FutureUnlessShutdown](
          CantonTimestampSecond
            .fromCantonTimestamp(cantonTickTs)
            .flatMap(ts =>
              Either.cond(
                ts.getEpochSecond % domainParamsF.reconciliationInterval.duration.getSeconds == 0,
                (),
                "",
              )
            )
            .leftMap[CantonError](_ =>
              InspectionServiceError.IllegalArgumentError.Error(
                s"""The participant cannot open commitment ${request.commitment} for participant
                   | ${request.computedForCounterParticipantUid} on domain ${request.domainId} because the given
                   | period end tick ${request.periodEndTick} is not a valid reconciliation interval tick""".stripMargin
              )
            )
        )

        _ <- EitherT.cond[FutureUnlessShutdown](
          computedCmts.exists { case (period, participant, cmt) =>
            period.fromExclusive < cantonTickTs && period.toInclusive >= cantonTickTs && participant == counterParticipant && cmt == request.commitment
          },
          (),
          InspectionServiceError.IllegalArgumentError.Error(
            s"""The participant cannot open commitment ${request.commitment} for participant
            ${request.computedForCounterParticipantUid} on domain ${request.domainId} and period end
            ${request.periodEndTick} because the participant has not computed such a commitment at the given tick timestamp for the given counter participant""".stripMargin
          ): CantonError,
        )

        counterParticipantParties <- EitherTUtil
          .fromFuture(
            FutureUnlessShutdown.outcomeF(
              snapshot
                .inspectKnownParties(
                  filterParty = "",
                  filterParticipant = counterParticipant.filterString,
                )
            ),
            err => InspectionServiceError.InternalServerError.Error(err.toString),
          )
          .leftWiden[CantonError]

        contractsAndTransferCounter <- EitherTUtil
          .fromFuture(
            FutureUnlessShutdown.outcomeF(
              syncStateInspection.activeContractsStakeholdersFilter(
                domainId,
                cantonTickTs,
                counterParticipantParties.map(_.toLf),
              )
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
      request: InspectCommitmentContracts.Request,
      responseObserver: StreamObserver[InspectCommitmentContracts.Response],
  ): Unit = ???

  override def countInFlight(
      request: CountInFlight.Request
  ): Future[CountInFlight.Response] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val inFlightCount: EitherT[Future, String, InFlightCount] = for {
      domainId <- EitherT.fromEither[Future](DomainId.fromString(request.domainId))
      domainAlias <- EitherT.fromEither[Future](
        domainAliasManager
          .aliasForDomainId(domainId)
          .toRight(s"Not able to find domain alias for ${domainId.toString}")
      )

      count <- syncStateInspection.countInFlight(domainAlias)
    } yield {
      count
    }

    inFlightCount.fold(
      err => throw new IllegalArgumentException(err),
      count =>
        CountInFlight.Response(
          count.pendingSubmissions.unwrap,
          count.pendingTransactions.unwrap,
        ),
    )

  }
}

object InspectionServiceError extends InspectionServiceErrorGroup {
  sealed trait InspectionServiceError extends CantonError

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
