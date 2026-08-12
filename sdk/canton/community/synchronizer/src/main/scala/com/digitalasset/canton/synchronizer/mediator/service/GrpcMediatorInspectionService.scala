// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.service

import cats.syntax.functor.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.pekko.ServerAdapter
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, TransactionView}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30 as mediatorV30
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.protocol.messages.InformeeMessage
import com.digitalasset.canton.synchronizer.mediator.FinalizedResponse
import com.digitalasset.canton.synchronizer.mediator.store.FinalizedResponseStore
import com.digitalasset.canton.time.TimeAwaiter
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.FutureUtil
import io.grpc.Status
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

import GrpcMediatorInspectionService.*

/** The mediator inspection service delivers a stream of finalized verdicts. The verdicts are sorted
  * by the finalization time, which additionally is capped by the watermark tracked via the provided
  * time awaiter. In case the watermark is reached and no new verdicts are found, the stream waits
  * to be notified by the time awaiter when new watermarks are encountered. In practice, the time
  * awaiter follows the observed sequencing.
  *
  * <strong>Notice:</strong>While returning the results in order of the request time would be a more
  * natural way to consume the information, this would add significant complexity to the
  * implementation, because of various reasons:
  *   - a later request could be completed sooner than an earlier requests
  *   - the inspection service would have to interact with the ongoing mediator state to understand
  *     which requests are still pending and not emit verdicts of requests after the oldest pending
  *     request
  *
  * In contrast, using the finalization time for sorting the verdicts is a simple and stable way to
  * deliver all known verdicts purely based on the verdicts persisted in the finalized response
  * store.
  */
class GrpcMediatorInspectionService(
    finalizedResponseStore: FinalizedResponseStore,
    watermarkTracker: TimeAwaiter,
    batchSize: PositiveInt,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, esf: ExecutionSequencerFactory, materializer: Materializer)
    extends mediatorV30.MediatorInspectionServiceGrpc.MediatorInspectionService
    with NamedLogging {

  /** Loads verdicts from the finalized response store, starting with the optional timestamps or the
    * beginning, if not provided. The responses are ordered by the tuple:
    * `(verdict.finalizationTime, verdict.recordTime)`,
    * i.e. the sequencing timestamp of the response that resulted in a finalized response.
    */
  override def verdicts(
      request: mediatorV30.VerdictsRequest,
      responseObserver: StreamObserver[mediatorV30.VerdictsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val startingTimestamp = request.mostRecentlyReceivedRecordTime
      .map(CantonTimestamp.fromProtoTimestamp)
      .getOrElse(Right(CantonTimestamp.MinValue))

    startingTimestamp match {
      case Left(err) => responseObserver.onError(ProtoDeserializationFailure.Wrap(err).asGrpcError)
      case Right(startingRequestTime) =>
        withServerCallStreamObserver(responseObserver) { observer =>
          FutureUtil.doNotAwait(
            loadBatchesAndRespond(
              QueryRange(
                fromRequestExclusive = startingRequestTime,
                toRequestInclusive = watermarkTracker.getCurrentKnownTime(),
              ),
              observer,
            )
              .tapOnShutdown(observer.onError(AbortedDueToShutdown.Error().asGrpcError))
              .onShutdown(()),
            failureMessage = s"verdicts starting from exclusive $startingRequestTime",
            level = Level.INFO,
          )
        }
    }
  }

  /** Load batches of verdicts until the client cancels the response stream
    */
  def loadBatchesAndRespond(
      queryRange: QueryRange,
      responseObserver: ServerCallStreamObserver[mediatorV30.VerdictsResponse],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val sink = ServerAdapter.toSink(
      responseObserver,
      throwable =>
        MediatorError.InternalError
          .Reject(
            cause = "Error during MediatorInspectionService.verdicts",
            throwableO = Some(throwable),
          )
          .asGrpcError,
    )

    val doneF = Source
      .unfoldAsync(
        QueryRange(CantonTimestamp.MinValue, queryRange.fromRequestExclusive) -> Seq
          .empty[FinalizedResponse]
      ) { case (queryRange, previousResponses) =>
        val resultFUS = for {
          nextTimeStamps <- determineNextTimestamps(
            previousResponses,
            queryRange.toRequestInclusive,
          )
          QueryRange(fromRequestTime, toRequestTime) = nextTimeStamps
          _ = logger.debug(
            s"Loading verdicts between ]$fromRequestTime, $toRequestTime]"
          )

          finalizedResponses <- finalizedResponseStore
            .readFinalizedVerdicts(
              fromRequestTime,
              toRequestTime,
              batchSize,
            )
        } yield {
          val verdicts = buildVerdictResponses(finalizedResponses)
          Some((nextTimeStamps, finalizedResponses) -> verdicts)
        }

        resultFUS.onShutdown(None)
      }
      .mapConcat(identity)
      .runWith(sink)

    FutureUnlessShutdown.outcomeF(doneF)
  }

  private def determineNextTimestamps(
      finalizedResponses: Seq[FinalizedResponse],
      currentToInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[QueryRange] = {
    // we're using the latest timestamp from the responses loaded from the database (even though
    // they might contain verdicts for irrelevant requests, e.g. reassignments), so that
    // we properly advance the time window that needs to be checked in the finalized response store
    // and avoid loading data again and again just to discard it afterwards

    val mostRecentTimestamp = finalizedResponses
      .maxByOption(r => r.requestId.unwrap)
      .map(r => r.requestId.unwrap)
    // Use the timestamp from the most recent verdict loaded from the database.
    // If no verdicts were found, use currentToInclusive as the next starting point, because we know
    // that there won't be any verdicts before this timestamp
    val nextFromExclusive = mostRecentTimestamp.getOrElse(currentToInclusive)

    val newWatermark = watermarkTracker.getCurrentKnownTime()
    val possiblyWaitForNextObservedTimestamp = if (newWatermark <= nextFromExclusive) {
      logger.debug(
        s"Waiting to observe a time later than the current watermark $newWatermark"
      )
      watermarkTracker
        .awaitKnownTimestamp(newWatermark.immediateSuccessor)
        // if there is a race and in the meantime a sequenced time > `newWatermark` was observed, we just continue
        .getOrElse(FutureUnlessShutdown.unit)
    } else {
      // no need to wait, since the watermark has moved since last we queried the store
      FutureUnlessShutdown.unit
    }

    possiblyWaitForNextObservedTimestamp
      // fact: there is no verdict until nextFromExclusive, because no responses were found.
      // Therefore, use `nextFromExclusive` as the starting point for the next batch lookup.
      .map(_ =>
        QueryRange(
          fromRequestExclusive = nextFromExclusive,
          toRequestInclusive = watermarkTracker.getCurrentKnownTime(),
        )
      )

  }

  /** Converts the responses to inspection api verdicts
    */
  private def buildVerdictResponses(
      finalizedResponses: Seq[FinalizedResponse]
  )(implicit traceContext: TraceContext): Seq[mediatorV30.VerdictsResponse] =
    NonEmpty
      .from(convertResponses(finalizedResponses))
      .fold(Seq.empty[mediatorV30.VerdictsResponse]) { verdicts =>
        // we're using the latest timestamp from the responses loaded from the database (even though
        // they might contain verdicts for irrelevant requests, e.g. reassignments), so that
        // we can log the full range of the time window considered
        val timestamps = finalizedResponses.map(r => r.requestId.unwrap)
        val minRequestTime = timestamps.headOption
        val maxRequestTime = timestamps.lastOption
        logger.debug(
          s"Responding with ${verdicts.size} verdicts between [$minRequestTime, $maxRequestTime]"
        )
        verdicts
      }

  /** Filters for verdicts for relevant requests (currently only InformeeMessage aka Daml
    * transactions) and convert to the mediator inspection api value.
    */
  private def convertResponses(
      responses: Seq[FinalizedResponse]
  ): Seq[mediatorV30.VerdictsResponse] =
    responses.collect {
      case FinalizedResponse(
            requestId,
            request @ InformeeMessage(fullInformeeTree, _),
            finalizationTime,
            verdict,
          ) =>
        val (flattened, rootNodes) = flattenForrest[TransactionView, mediatorV30.TransactionView](
          fullInformeeTree.tree.rootViews.unblindedElements,
          _.subviews.unblindedElements,
          convertTransactionView,
        )
        val views =
          mediatorV30.TransactionViews(
            views = flattened,
            rootNodes,
          )
        val protoVerdict = mediatorV30.Verdict(
          submittingParties = fullInformeeTree.tree.submitterMetadata.tryUnwrap.actAs.toSeq,
          submittingParticipantUid = request.submittingParticipant.uid.toProtoPrimitive,
          verdict =
            if (verdict.isApprove) mediatorV30.VerdictResult.VERDICT_RESULT_ACCEPTED
            else mediatorV30.VerdictResult.VERDICT_RESULT_REJECTED,
          finalizationTime = Some(finalizationTime.toProtoTimestamp),
          recordTime = Some(requestId.unwrap.toProtoTimestamp),
          mediatorGroup = request.mediator.group.value,
          views = mediatorV30.Verdict.Views.TransactionViews(views),
          updateId = request.rootHash.unwrap.toHexString,
        )
        mediatorV30.VerdictsResponse(Some(protoVerdict))
    }

  private def convertTransactionView(
      view: TransactionView,
      childNodes: Seq[Int],
  ): mediatorV30.TransactionView = {
    val params = view.viewCommonData.tryUnwrap.viewConfirmationParameters
    mediatorV30.TransactionView(
      informees = params.informees.toSeq,
      confirmingParties = params.quorums
        .map(q => mediatorV30.Quorum(q.confirmers.keySet.toSeq, q.threshold.value)),
      subViews = childNodes,
    )
  }

  /** Ensure observer is a ServerCallStreamObserver
    *
    * @param observer
    *   underlying observer
    * @param handler
    *   handler requiring a ServerCallStreamObserver
    */
  private def withServerCallStreamObserver[R](
      observer: StreamObserver[R]
  )(handler: ServerCallStreamObserver[R] => Unit)(implicit traceContext: TraceContext): Unit =
    observer match {
      case serverCallStreamObserver: ServerCallStreamObserver[R] =>
        handler(serverCallStreamObserver)
      case _ =>
        val statusException =
          Status.INTERNAL.withDescription("Unknown stream observer request").asException()
        logger.warn(statusException.getMessage)
        observer.onError(statusException)
    }

}

object GrpcMediatorInspectionService {

  final case class QueryRange(
      fromRequestExclusive: CantonTimestamp,
      toRequestInclusive: CantonTimestamp,
  )

  /** Takes a list of root nodes of type A, a function to determine child nodes, and a conversion
    * function that takes the node of type A and the indices of the children in pre-order traversal
    * to convert to a value of type B
    * @param roots
    *   the root nodes of the forrest
    * @param getChildren
    *   the function to determine child nodes given a value A
    * @param convert
    *   given a value of type A and the child indices, create a value of type B
    * @return
    *   <ul> <li>a map from pre-order traversal index to flatten nodes of type B</li> <li>the
    *   indices of the root nodes</li> </ul>
    */
  def flattenForrest[A, B](
      roots: Seq[A],
      getChildren: A => Seq[A],
      convert: (A, Seq[Int]) => B,
  ): (Map[Int, B], Vector[Int]) = {
    def traversal(elems: Seq[A], nextIndex: Int): (Vector[(Int, B)], Vector[Int], Int) = {
      val (resultWithChildren, childIndices, lastIndex) =
        elems.foldLeft(
          (Vector.empty[(Int, B)], Vector.empty[Int], nextIndex)
        ) { case ((acc, currentChildrenIndices, childIndex), child) =>
          val (resultWithChildren, childIndices, lastIndex) =
            traversal(getChildren(child), childIndex + 1)
          (
            acc ++ Vector(childIndex -> convert(child, childIndices)) ++ resultWithChildren,
            currentChildrenIndices :+ childIndex,
            lastIndex,
          )
        }
      (resultWithChildren, childIndices, lastIndex)
    }

    val (result, rootIndices, _) = traversal(roots, 0)
    (result.toMap, rootIndices)
  }

}
