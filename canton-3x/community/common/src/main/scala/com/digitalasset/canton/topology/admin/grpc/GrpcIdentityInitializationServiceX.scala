// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import com.digitalasset.canton.crypto.store.CryptoPublicStore
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.topology.admin.v1 as adminProto
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}

class GrpcIdentityInitializationServiceX(
    clock: Clock,
    bootstrap: GrpcIdentityInitializationServiceX.Callback,
    cryptoPublicStore: CryptoPublicStore,
)(implicit ec: ExecutionContext)
    extends adminProto.IdentityInitializationServiceXGrpc.IdentityInitializationServiceX {

  override def initId(request: adminProto.InitIdRequest): Future[adminProto.InitIdResponse] = {
    // TODO(#14048) propagate trace context
    // implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val adminProto.InitIdRequest(uidP) = request
    // TODO(#14048) proper error reporting
    val res = for {
      uid <- EitherT.fromEither[Future](UniqueIdentifier.fromProtoPrimitive_(uidP))
      _ <- bootstrap.initializeWithProvidedId(uid)
    } yield adminProto.InitIdResponse()
    EitherTUtil.toFuture(CantonGrpcUtil.mapErr(res))
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
