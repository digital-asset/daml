// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.mediator.admin.gprc.{
  InitializeMediatorRequest,
  InitializeMediatorResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.tracing.TraceContextGrpc
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Hosts the initialization service for the mediator.
  * Upon receiving an initialize request it will the provided `initialize` function.
  * Will ensure that `initialize` is not called concurrently.
  */
class GrpcMediatorInitializationService(
    initialization: InitializeMediatorRequest => EitherT[
      FutureUnlessShutdown,
      String,
      SigningPublicKey,
    ],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends v0.MediatorInitializationServiceGrpc.MediatorInitializationService
    with NamedLogging {

  /** Initialize a Mediator service
    * If the Mediator is uninitialized it should initialize itself with the provided configuration
    * If the Mediator is already initialized then verify the request is for the domain we're running against,
    * if correct then just return the current key otherwise fail.
    */
  override def initialize(
      requestP: v0.InitializeMediatorRequest
  ): Future[v0.InitializeMediatorResponse] = {
    TraceContextGrpc.withGrpcTraceContext { implicit traceContext =>
      for {
        request <- EitherTUtil.toFuture(
          InitializeMediatorRequest
            .fromProtoV0(requestP)
            .leftMap(err => s"Failed to deserialize request: $err")
            .leftMap(Status.INVALID_ARGUMENT.withDescription)
            .leftMap(_.asRuntimeException)
            .toEitherT[Future]
        )
        response <-
          initialization(request)
            .fold[InitializeMediatorResponse](
              InitializeMediatorResponse.Failure,
              InitializeMediatorResponse.Success,
            )
            .map(_.toProtoV0)
            .asGrpcResponse
      } yield response
    }
  }
}
