// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.base.error.CantonRpcError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.synchronizer.Synchronizer.FailedToInitialiseSynchronizerNode
import com.digitalasset.canton.synchronizer.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorResponse,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future}

/** Hosts the initialization service for the mediator. Upon receiving an initialize request it will
  * the provided `initialize` function.
  */
class GrpcMediatorInitializationService(
    handler: GrpcMediatorInitializationService.Callback,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v30.MediatorInitializationServiceGrpc.MediatorInitializationService
    with NamedLogging {

  override def initializeMediator(
      requestP: v30.InitializeMediatorRequest
  ): Future[v30.InitializeMediatorResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res: EitherT[Future, CantonRpcError, v30.InitializeMediatorResponse] = for {
      request <- EitherT.fromEither[Future](
        InitializeMediatorRequest
          .fromProtoV30(requestP)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      result <- handler
        .initialize(request)
        .leftMap(FailedToInitialiseSynchronizerNode.Failure(_))
        .onShutdown(Left(FailedToInitialiseSynchronizerNode.Shutdown())): EitherT[
        Future,
        CantonRpcError,
        InitializeMediatorResponse,
      ]
    } yield result.toProtoV30
    mapErrNew(res)
  }

}

object GrpcMediatorInitializationService {
  trait Callback {
    def initialize(request: InitializeMediatorRequest)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeMediatorResponse]
  }
}
