// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.participant.v30.InspectionServiceGrpc.InspectionService
import com.digitalasset.canton.admin.participant.v30.{
  CountInFlight,
  DomainTimeRange,
  GetConfigForSlowCounterParticipants,
  GetIntervalsBehindForCounterParticipants,
  InspectCommitmentContracts,
  LookupOffsetByTime,
  LookupReceivedAcsCommitments,
  LookupSentAcsCommitments,
  OpenCommitment,
  SetConfigForSlowCounterParticipants,
  TimeRange,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.InFlightCount
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.protocol.messages.{
  CommitmentPeriodState,
  DomainSearchCommitmentPeriod,
  ReceivedAcsCommitment,
  SentAcsCommitment,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.stub.StreamObserver

import scala.concurrent.{ExecutionContext, Future}

class GrpcInspectionService(
    syncStateInspection: SyncStateInspection,
    indexedStringStore: IndexedStringStore,
    domainAliasManager: DomainAliasManager,
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
  ): Unit = ???

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
