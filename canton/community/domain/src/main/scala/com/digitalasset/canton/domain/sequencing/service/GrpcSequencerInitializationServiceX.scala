// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.domain.Domain.FailedToInitialiseDomainNode
import com.digitalasset.canton.domain.admin.v2.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.domain.admin.v2.{
  InitializeSequencerRequest,
  InitializeSequencerResponse,
}
import com.digitalasset.canton.domain.sequencing.admin.grpc.{
  InitializeSequencerRequestX,
  InitializeSequencerResponseX,
}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerInitializationServiceX(
    handler: GrpcSequencerInitializationServiceX.Callback,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends SequencerInitializationService
    with NamedLogging {

  override def initialize(
      requestP: InitializeSequencerRequest
  ): Future[InitializeSequencerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res: EitherT[Future, CantonError, InitializeSequencerResponse] = for {
      request <- EitherT.fromEither[Future](
        InitializeSequencerRequestX
          .fromProtoV2(requestP)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      result <- handler
        .initialize(request)
        .leftMap(FailedToInitialiseDomainNode.Failure(_))
        .onShutdown(Left(FailedToInitialiseDomainNode.Shutdown())): EitherT[
        Future,
        CantonError,
        InitializeSequencerResponseX,
      ]
    } yield result.toProtoV2
    mapErrNew(res)
  }
}

object GrpcSequencerInitializationServiceX {
  trait Callback {
    def initialize(request: InitializeSequencerRequestX)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, String, InitializeSequencerResponseX]
  }
}
