// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.store

import cats.data.OptionT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.{
  EnvelopeContent,
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
}
import com.digitalasset.canton.resource.IdempotentInsert.insertIgnoringConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future, blocking}

trait RegisterTopologyTransactionResponseStore extends AutoCloseable {
  def savePendingResponse(
      response: RegisterTopologyTransactionResponse.Result
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  @VisibleForTesting
  def pendingResponses()(implicit
      traceContext: TraceContext
  ): Future[Seq[RegisterTopologyTransactionResponse.Result]]

  def completeResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def exists(requestId: TopologyRequestId)(implicit traceContext: TraceContext): Future[Boolean]

  def getResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RegisterTopologyTransactionResponseStore.Response]
}

object RegisterTopologyTransactionResponseStore {
  final case class Response(
      response: RegisterTopologyTransactionResponse.Result,
      isCompleted: Boolean,
  )

  def apply(
      storage: Storage,
      cryptoApi: CryptoPureApi,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): RegisterTopologyTransactionResponseStore = storage match {
    case _: MemoryStorage => new InMemoryRegisterTopologyTransactionResponseStore()
    case jdbc: DbStorage =>
      new DbRegisterTopologyTransactionResponseStore(
        jdbc,
        cryptoApi,
        protocolVersion,
        timeouts,
        loggerFactory,
      )
  }
}

class InMemoryRegisterTopologyTransactionResponseStore(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionResponseStore {
  import java.util.concurrent.ConcurrentHashMap
  import scala.jdk.CollectionConverters.*

  private val responseMap =
    new ConcurrentHashMap[
      TopologyRequestId,
      RegisterTopologyTransactionResponseStore.Response,
    ].asScala

  override def savePendingResponse(
      response: RegisterTopologyTransactionResponse.Result
  )(implicit traceContext: TraceContext): Future[Unit] = {
    responseMap
      .put(
        response.requestId,
        RegisterTopologyTransactionResponseStore.Response(response, isCompleted = false),
      )
      .discard
    Future.unit
  }

  override def pendingResponses()(implicit
      traceContext: TraceContext
  ): Future[Seq[RegisterTopologyTransactionResponse.Result]] =
    Future.successful(responseMap.values.filterNot(_.isCompleted).map(_.response).toSeq)

  override def completeResponse(
      requestId: TopologyRequestId
  )(implicit traceContext: TraceContext): Future[Unit] = blocking {
    synchronized {
      responseMap
        .get(requestId)
        .foreach(response => responseMap.put(requestId, response.copy(isCompleted = true)))
      Future.unit
    }
  }

  override def getResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RegisterTopologyTransactionResponseStore.Response] =
    OptionT.fromOption(responseMap.get(requestId))

  override def exists(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    Future.successful(responseMap.contains(requestId))

  override def close(): Unit = ()
}

class DbRegisterTopologyTransactionResponseStore(
    override protected val storage: DbStorage,
    cryptoApi: CryptoPureApi,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends RegisterTopologyTransactionResponseStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  implicit val getRegisterTopologyTransactionRequest
      : GetResult[RegisterTopologyTransactionRequest] = GetResult(r =>
    EnvelopeContent
      .messageFromByteArray[RegisterTopologyTransactionRequest](protocolVersion, cryptoApi)(
        r.<<[Array[Byte]]
      )
      .valueOr(error =>
        throw new DbDeserializationException(
          s"Error deserializing register topology transaction request $error"
        )
      )
  )
  implicit val setParameterRegisterTopologyTransactionRequest
      : SetParameter[RegisterTopologyTransactionRequest] =
    (r: RegisterTopologyTransactionRequest, pp: PositionedParameters) =>
      pp >> EnvelopeContent.tryCreate(r, protocolVersion).toByteString.toByteArray

  implicit val getRegisterTopologyTransactionResponse
      : GetResult[RegisterTopologyTransactionResponse.Result] =
    GetResult(r =>
      EnvelopeContent
        .messageFromByteArray[RegisterTopologyTransactionResponse.Result](
          protocolVersion,
          cryptoApi,
        )(r.<<[Array[Byte]])
        .valueOr(error =>
          throw new DbDeserializationException(
            s"Error deserializing register topology transaction response $error"
          )
        )
    )
  implicit val getPendingRegisterTopologyTransactionRequestStoreResponse
      : GetResult[RegisterTopologyTransactionResponseStore.Response] = GetResult(r =>
    RegisterTopologyTransactionResponseStore.Response(
      getRegisterTopologyTransactionResponse(r),
      r.nextBoolean(),
    )
  )
  implicit val setRegisterTopologyTransactionResponse: SetParameter[
    RegisterTopologyTransactionResponse.Result
  ] =
    (
        r: RegisterTopologyTransactionResponse.Result,
        pp: PositionedParameters,
    ) => pp >> EnvelopeContent.tryCreate(r, protocolVersion).toByteString.toByteArray

  override def savePendingResponse(
      response: RegisterTopologyTransactionResponse.Result
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.update_(
      // request_id is a uuid so can safely assume conflicts will only be caused by retries of this request
      insertIgnoringConflicts(
        storage,
        "register_topology_transaction_responses(request_id)",
        sql"""register_topology_transaction_responses(request_id, response, completed)
                   values(${response.requestId},${response},${false})""",
      ),
      functionFullName,
    )

  override def pendingResponses()(implicit
      traceContext: TraceContext
  ): Future[Seq[RegisterTopologyTransactionResponse.Result]] =
    storage.query(
      sql""" select response from register_topology_transaction_responses where completed = ${false}"""
        .as[RegisterTopologyTransactionResponse.Result],
      functionFullName,
    )

  override def completeResponse(
      requestId: TopologyRequestId
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage
      .update_(
        sqlu"""update register_topology_transaction_responses set completed = ${true} where request_id=$requestId""",
        functionFullName,
      )

  override def exists(
      requestId: TopologyRequestId
  )(implicit traceContext: TraceContext): Future[Boolean] =
    storage.query(
      sql"""select 1 from register_topology_transaction_responses where request_id=$requestId"""
        .as[Int]
        .map(_.nonEmpty),
      functionFullName,
    )

  override def getResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RegisterTopologyTransactionResponseStore.Response] =
    OptionT(
      storage.query(
        sql""" select response, completed from register_topology_transaction_responses where request_id=$requestId"""
          .as[RegisterTopologyTransactionResponseStore.Response]
          .headOption,
        functionFullName,
      )
    )
}
