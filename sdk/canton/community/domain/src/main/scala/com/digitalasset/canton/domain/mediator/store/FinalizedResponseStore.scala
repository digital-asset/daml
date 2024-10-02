// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.OptionT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.mediator.FinalizedResponse
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{
  EnvelopeContent,
  MediatorConfirmationRequest,
  Verdict,
}
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext

/** Stores and retrieves finalized confirmation response aggregations
  */
private[mediator] trait FinalizedResponseStore extends AutoCloseable {

  /** Stores finalized mediator verdict.
    * In the event of a crash we may attempt to store an existing finalized request so the store
    * should behave in an idempotent manner.
    * TODO(#4335): If there is an existing value ensure that it matches the value we want to insert
    */
  def store(
      finalizedResponse: FinalizedResponse
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  /** Fetch previously stored finalized confirmation response aggregation by requestId.
    */
  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      overrideCloseContext: CloseContext,
  ): OptionT[FutureUnlessShutdown, FinalizedResponse]

  /** Remove all responses up to and including the provided timestamp. */
  def prune(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit]

  /** Count how many finalized responses we have stored.
    * Primarily used for testing mediator pruning.
    */
  def count()(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Long]

  /** Locate a timestamp relative to the earliest available finalized response
    * Useful to monitor the progress of pruning and for pruning in batches.
    */
  def locatePruningTimestamp(skip: Int)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]]
}

private[mediator] object FinalizedResponseStore {
  def apply(
      storage: Storage,
      cryptoApi: CryptoPureApi,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FinalizedResponseStore = storage match {
    case _: MemoryStorage => new InMemoryFinalizedResponseStore(loggerFactory)
    case jdbc: DbStorage =>
      new DbFinalizedResponseStore(
        jdbc,
        cryptoApi,
        protocolVersion,
        timeouts,
        loggerFactory,
      )
  }
}

private[mediator] class InMemoryFinalizedResponseStore(
    override protected val loggerFactory: NamedLoggerFactory
) extends FinalizedResponseStore
    with NamedLogging {
  private implicit val ec: ExecutionContext = DirectExecutionContext(noTracingLogger)

  import scala.jdk.CollectionConverters.*
  private val finalizedRequests =
    new ConcurrentHashMap[CantonTimestamp, FinalizedResponse].asScala

  override def store(
      finalizedResponse: FinalizedResponse
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    finalizedRequests.putIfAbsent(finalizedResponse.requestId.unwrap, finalizedResponse).discard
    FutureUnlessShutdown.unit
  }

  override def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      overrideCloseContext: CloseContext,
  ): OptionT[FutureUnlessShutdown, FinalizedResponse] =
    OptionT.fromOption[FutureUnlessShutdown](
      finalizedRequests.get(requestId.unwrap)
    )

  override def prune(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      finalizedRequests.keys
        .filterNot(_.isAfter(timestamp))
        .foreach(finalizedRequests.remove(_).discard[Option[FinalizedResponse]])
    }

  override def count()(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Long] =
    FutureUnlessShutdown.pure(finalizedRequests.size.toLong)

  override def locatePruningTimestamp(
      skip: Int
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    FutureUnlessShutdown.pure {
      import cats.Order.*
      val sortedSet =
        SortedSet.empty[CantonTimestamp] ++ finalizedRequests.keySet
      sortedSet.drop(skip).headOption
    }

  override def close(): Unit = ()
}

private[mediator] class DbFinalizedResponseStore(
    override protected val storage: DbStorage,
    cryptoApi: CryptoPureApi,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext, implicit val traceContext: TraceContext)
    extends FinalizedResponseStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  implicit val getResultRequestId: GetResult[RequestId] =
    GetResult[CantonTimestamp].andThen(ts => RequestId(ts))
  implicit val setParameterRequestId: SetParameter[RequestId] =
    (r: RequestId, pp: PositionedParameters) => SetParameter[CantonTimestamp].apply(r.unwrap, pp)

  private implicit val setParameterVerdict: SetParameter[Verdict] =
    Verdict.getVersionedSetParameter
  private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(protocolVersion)

  implicit val getResultMediatorConfirmationRequest: GetResult[MediatorConfirmationRequest] =
    GetResult(r =>
      EnvelopeContent
        .messageFromByteArray[MediatorConfirmationRequest](protocolVersion, cryptoApi)(
          r.<<[Array[Byte]]
        )
        .valueOr(error =>
          throw new DbDeserializationException(
            s"Error deserializing mediator confirmation request $error"
          )
        )
    )
  implicit val setParameterMediatorConfirmationRequest: SetParameter[MediatorConfirmationRequest] =
    (r: MediatorConfirmationRequest, pp: PositionedParameters) =>
      pp >> EnvelopeContent.tryCreate(r, protocolVersion).toByteArray

  override def store(
      finalizedResponse: FinalizedResponse
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    val insert =
      sqlu"""insert into med_response_aggregations(request_id, mediator_confirmation_request, version, verdict, request_trace_context)
             values (
               ${finalizedResponse.requestId},${finalizedResponse.request},${finalizedResponse.version},${finalizedResponse.verdict},
               ${SerializableTraceContext(finalizedResponse.requestTraceContext)}
             ) on conflict do nothing"""

    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        storage.updateUnlessShutdown_(
          insert,
          operationName = s"${this.getClass}: store request ${finalizedResponse.requestId}",
        )(traceContext, closeContext)
    }
  }

  override def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): OptionT[FutureUnlessShutdown, FinalizedResponse] =
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        storage.querySingleUnlessShutdown(
          sql"""select request_id, mediator_confirmation_request, version, verdict, request_trace_context
              from med_response_aggregations where request_id=${requestId.unwrap}
           """
            .as[
              (
                  RequestId,
                  MediatorConfirmationRequest,
                  CantonTimestamp,
                  Verdict,
                  SerializableTraceContext,
              )
            ]
            .map {
              _.headOption.map {
                case (reqId, mediatorConfirmationRequest, version, verdict, requestTraceContext) =>
                  FinalizedResponse(reqId, mediatorConfirmationRequest, version, verdict)(
                    requestTraceContext.unwrap
                  )
              }
            },
          operationName = s"${this.getClass}: fetch request $requestId",
        )(traceContext, closeContext)
    }

  override def prune(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        for {
          removedCount <- storage.updateUnlessShutdown(
            sqlu"delete from med_response_aggregations where request_id <= $timestamp",
            functionFullName,
          )(traceContext, closeContext)
        } yield logger.debug(s"Removed $removedCount finalized responses")
    }

  override def count()(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Long] =
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        storage.queryUnlessShutdown(
          sql"select count(request_id) from med_response_aggregations".as[Long].head,
          functionFullName,
        )(traceContext, closeContext)
    }

  override def locatePruningTimestamp(
      skip: Int
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        storage
          .queryUnlessShutdown(
            sql"select request_id from med_response_aggregations order by request_id asc #${storage
                .limit(1, skip.toLong)}"
              .as[CantonTimestamp]
              .headOption,
            functionFullName,
          )(traceContext, closeContext)
    }

}
