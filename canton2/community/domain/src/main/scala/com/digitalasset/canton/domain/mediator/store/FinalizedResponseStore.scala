// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.OptionT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.FinalizedResponse
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{EnvelopeContent, MediatorRequest, Verdict}
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

/** Stores and retrieves finalized mediator response aggregations
  */
private[mediator] trait FinalizedResponseStore extends AutoCloseable {

  /** Stores finalized mediator verdict.
    * In the event of a crash we may attempt to store an existing finalized request so the store
    * should behave in an idempotent manner.
    * TODO(#4335): If there is an existing value ensure that it matches the value we want to insert
    */
  def store(
      finalizedResponse: FinalizedResponse
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit]

  /** Fetch previously stored finalized mediator response aggregation by requestId.
    */
  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      overrideCloseContext: CloseContext,
  ): OptionT[Future, FinalizedResponse]

  /** Remove all responses up to and including the provided timestamp. */
  def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit]

  /** Count how many finalized responses we have stored.
    * Primarily used for testing mediator pruning.
    */
  def count()(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Long]

  /** Locate a timestamp relative to the earliest available finalized response
    * Useful to monitor the progress of pruning and for pruning in batches.
    */
  def locatePruningTimestamp(skip: Int)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Option[CantonTimestamp]]
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
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] = {
    finalizedRequests.putIfAbsent(finalizedResponse.requestId.unwrap, finalizedResponse).discard
    Future.unit
  }

  override def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      overrideCloseContext: CloseContext,
  ): OptionT[Future, FinalizedResponse] =
    OptionT.fromOption[Future](
      finalizedRequests.get(requestId.unwrap)
    )

  override def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] =
    Future.successful {
      finalizedRequests.keys
        .filterNot(_.isAfter(timestamp))
        .foreach(finalizedRequests.remove(_).discard[Option[FinalizedResponse]])
    }

  override def count()(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Long] =
    Future.successful(finalizedRequests.size.toLong)

  override def locatePruningTimestamp(
      skip: Int
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Option[CantonTimestamp]] = {
    Future.successful {
      import cats.Order.*
      val sortedSet =
        SortedSet.empty[CantonTimestamp] ++ finalizedRequests.keySet
      sortedSet.drop(skip).headOption
    }
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

  implicit val getResultMediatorRequest: GetResult[MediatorRequest] = GetResult(r =>
    EnvelopeContent
      .messageFromByteArray[MediatorRequest](protocolVersion, cryptoApi)(r.<<[Array[Byte]])
      .valueOr(error =>
        throw new DbDeserializationException(s"Error deserializing mediator request $error")
      )
  )
  implicit val setParameterMediatorRequest: SetParameter[MediatorRequest] =
    (r: MediatorRequest, pp: PositionedParameters) =>
      pp >> EnvelopeContent.tryCreate(r, protocolVersion).toByteArray

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("finalized-response-store")

  override def store(
      finalizedResponse: FinalizedResponse
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] =
    processingTime.event {
      val insert = storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sqlu"""insert
                     /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( response_aggregations ( request_id ) ) */
                     into response_aggregations(request_id, mediator_request, version, verdict, request_trace_context)
                     values (
                       ${finalizedResponse.requestId},${finalizedResponse.request},${finalizedResponse.version},${finalizedResponse.verdict},
                       ${SerializableTraceContext(finalizedResponse.requestTraceContext)}
                     )"""
        case _ =>
          sqlu"""insert into response_aggregations(request_id, mediator_request, version, verdict, request_trace_context)
                     values (
                       ${finalizedResponse.requestId},${finalizedResponse.request},${finalizedResponse.version},${finalizedResponse.verdict},
                       ${SerializableTraceContext(finalizedResponse.requestTraceContext)}
                     ) on conflict do nothing"""
      }

      CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
        closeContext =>
          storage.update_(
            insert,
            operationName = s"${this.getClass}: store request ${finalizedResponse.requestId}",
          )(traceContext, closeContext)
      }
    }

  override def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): OptionT[Future, FinalizedResponse] =
    processingTime.optionTEvent {
      CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
        closeContext =>
          storage.querySingle(
            sql"""select request_id, mediator_request, version, verdict, request_trace_context
              from response_aggregations where request_id=${requestId.unwrap}
           """
              .as[(RequestId, MediatorRequest, CantonTimestamp, Verdict, SerializableTraceContext)]
              .map {
                _.headOption.map {
                  case (reqId, mediatorRequest, version, verdict, requestTraceContext) =>
                    FinalizedResponse(reqId, mediatorRequest, version, verdict)(
                      requestTraceContext.unwrap
                    )
                }
              },
            operationName = s"${this.getClass}: fetch request $requestId",
          )(traceContext, closeContext)
      }
    }

  override def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext, callerCloseContext: CloseContext): Future[Unit] =
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        for {
          removedCount <- storage.update(
            sqlu"delete from response_aggregations where request_id <= ${timestamp}",
            functionFullName,
          )(traceContext, closeContext)
        } yield logger.debug(s"Removed $removedCount finalized responses")
    }

  override def count()(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Long] = {
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        storage.query(
          sql"select count(request_id) from response_aggregations".as[Long].head,
          functionFullName,
        )(traceContext, closeContext)
    }
  }

  override def locatePruningTimestamp(
      skip: Int
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): Future[Option[CantonTimestamp]] = {
    CloseContext.withCombinedContext(callerCloseContext, closeContext, timeouts, logger) {
      closeContext =>
        storage
          .query(
            sql"select request_id from response_aggregations order by request_id asc #${storage
                .limit(1, skip.toLong)}"
              .as[CantonTimestamp]
              .headOption,
            functionFullName,
          )(traceContext, closeContext)
    }
  }

}
