// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.topology.admin.v30 as adminProto
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future}

class GrpcIdentityInitializationService(
    clock: Clock,
    bootstrap: GrpcIdentityInitializationService.Callback,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends adminProto.IdentityInitializationServiceGrpc.IdentityInitializationService
    with NamedLogging {

  override def initId(request: adminProto.InitIdRequest): Future[adminProto.InitIdResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val adminProto.InitIdRequest(uidP) = request
    for {
      uid <- Future(
        UniqueIdentifier
          .fromProtoPrimitive_(uidP)
          .valueOr(err => throw ProtoDeserializationFailure.WrapNoLogging(err).asGrpcError)
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

  override def getId(request: adminProto.GetIdRequest): Future[adminProto.GetIdResponse] = {
    val id = bootstrap.getId
    Future.successful(
      adminProto.GetIdResponse(
        initialized = bootstrap.isInitialized,
        uniqueIdentifier = id.map(_.toProtoPrimitive).getOrElse(""),
      )
    )
  }

  override def currentTime(
      request: adminProto.CurrentTimeRequest
  ): Future[adminProto.CurrentTimeResponse] =
    Future.successful(adminProto.CurrentTimeResponse(clock.now.toProtoPrimitive))
}

object GrpcIdentityInitializationService {
  trait Callback {
    def initializeWithProvidedId(uid: UniqueIdentifier)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Unit]
    def getId: Option[UniqueIdentifier]
    def isInitialized: Boolean
  }
}
