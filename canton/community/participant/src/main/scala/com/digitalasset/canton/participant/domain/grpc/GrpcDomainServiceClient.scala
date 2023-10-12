// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain.grpc

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.common.domain.ServiceAgreement
import com.digitalasset.canton.domain.api.v0.DomainServiceGrpc.DomainServiceStub
import com.digitalasset.canton.domain.api.v0.{
  DomainServiceGrpc,
  GetServiceAgreementRequest,
  GetServiceAgreementResponse,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, ClientChannelBuilder}
import com.digitalasset.canton.participant.domain.DomainServiceClient
import com.digitalasset.canton.participant.domain.DomainServiceClient.Error
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TracingConfig

import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future}

@nowarn("cat=deprecation")
private[domain] class GrpcDomainServiceClient(
    traceContextPropagation: TracingConfig.Propagation,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContextExecutor)
    extends DomainServiceClient
    with NamedLogging {

  def builder(sequencerConnection: GrpcSequencerConnection) = {
    val clientChannelBuilder = ClientChannelBuilder(loggerFactory)
    sequencerConnection.mkChannelBuilder(clientChannelBuilder, traceContextPropagation)
  }

  override def getAgreement(domainId: DomainId, sequencerConnection: GrpcSequencerConnection)(
      implicit loggingContext: ErrorLoggingContext
  ): EitherT[Future, Error.GetServiceAgreementError, Option[ServiceAgreement]] =
    for {
      response <- CantonGrpcUtil
        .sendSingleGrpcRequest[DomainServiceStub, GetServiceAgreementResponse](
          domainId.toString,
          "getting service agreement from remote domain",
          builder(sequencerConnection).build(),
          DomainServiceGrpc.stub,
          Duration(20, TimeUnit.SECONDS),
          logger,
          retryPolicy = _ => false,
        )(_.getServiceAgreement(GetServiceAgreementRequest()))(loggingContext.traceContext)
        .leftMap(e => Error.GetServiceAgreementError(e.toString))
      optAgreement <- EitherT.fromEither[Future](
        response.agreement
          .traverse(ag =>
            ServiceAgreement
              .fromProtoV0(ag)
              .leftMap(e => Error.GetServiceAgreementError(e.toString))
          )
      )
    } yield optAgreement
}
