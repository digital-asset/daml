// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import com.digitalasset.canton.domain.admin.v0.InitResponse
import com.digitalasset.canton.domain.admin.v0.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.domain.admin.{v0, v2}
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TraceContextGrpc, Traced}
import com.digitalasset.canton.util.EitherTUtil
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Will initialize the sequencer server based using the provided initialize function when called.
  */
class GrpcSequencerInitializationService(
    initialize: Traced[InitializeSequencerRequest] => EitherT[
      FutureUnlessShutdown,
      String,
      InitializeSequencerResponse,
    ],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerInitializationService
    with NamedLogging
    with NoTracing {

  override def initV2(requestP: v2.InitRequest): Future[InitResponse] =
    initInternal(requestP, InitializeSequencerRequest.fromProtoV2)

  /** Process requests sequentially */
  def initInternal[P](
      requestP: P,
      deserializer: P => ParsingResult[InitializeSequencerRequest],
  ): Future[v0.InitResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    // ensure here we don't process initialization requests concurrently
    val result = for {
      request <- EitherT
        .fromEither[FutureUnlessShutdown](deserializer(requestP))
        .leftMap(err => s"Failed to deserialize request: $err")
        .leftMap(Status.INVALID_ARGUMENT.withDescription)
      response <- initialize(Traced(request))
        .leftMap(Status.FAILED_PRECONDITION.withDescription)
      responseP = response.toProtoV0
    } yield responseP
    EitherTUtil.toFutureUnlessShutdown(result.leftMap(_.asRuntimeException())).asGrpcResponse
  }

}
