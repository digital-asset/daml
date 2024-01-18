// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.crypto.store.CryptoPublicStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.topology.admin.v1 as adminProto
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}

class GrpcIdentityInitializationServiceX(
    clock: Clock,
    bootstrap: GrpcIdentityInitializationServiceX.Callback,
    cryptoPublicStore: CryptoPublicStore,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends adminProto.IdentityInitializationServiceXGrpc.IdentityInitializationServiceX
    with NamedLogging {

  override def initId(request: adminProto.InitIdRequest): Future[adminProto.InitIdResponse] = {
    // TODO(#14048) propagate trace context
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val adminProto.InitIdRequest(uidP) = request
    // TODO(#14048) proper error reporting
    for {
      uid <- Future(
        UniqueIdentifier
          .fromProtoPrimitive_(uidP)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLoggingStr(err).asGrpcError)
      )
      _ <- bootstrap
        .initializeWithProvidedId(uid)
        .valueOr(err =>
          throw io.grpc.Status.FAILED_PRECONDITION.withDescription(err).asRuntimeException()
        )
    } yield adminProto.InitIdResponse()
  }

  override def getOnboardingTransactions(
      request: adminProto.GetOnboardingTransactionsRequest
  ): Future[adminProto.GetOnboardingTransactionsResponse] = ???

  override def getId(request: Empty): Future[adminProto.GetIdResponse] = {
    val id = bootstrap.getId
    Future.successful(
      adminProto.GetIdResponse(
        initialized = bootstrap.isInitialized,
        uniqueIdentifier = id.map(_.toProtoPrimitive).getOrElse(""),
      )
    )
  }

  override def currentTime(request: Empty): Future[Timestamp] =
    Future.successful(clock.now.toProtoPrimitive)
}

object GrpcIdentityInitializationServiceX {
  trait Callback {
    def initializeWithProvidedId(uid: UniqueIdentifier): EitherT[Future, String, Unit]
    def getId: Option[UniqueIdentifier]
    def isInitialized: Boolean
  }
}
