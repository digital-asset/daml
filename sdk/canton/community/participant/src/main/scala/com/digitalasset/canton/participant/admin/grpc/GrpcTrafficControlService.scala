// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.error.{BaseCantonError, CantonError}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.admin.traffic.TrafficStateAdmin
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors.InvalidDomainId
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.NoTracing

import scala.concurrent.{ExecutionContext, Future}

class GrpcTrafficControlService(
    service: CantonSyncService,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends v30.TrafficControlServiceGrpc.TrafficControlService
    with NamedLogging
    with NoTracing {
  override def trafficControlState(
      request: v30.TrafficControlStateRequest
  ): Future[v30.TrafficControlStateResponse] = {
    val result = for {
      domainId <- EitherT
        .fromEither[Future]
        .apply[CantonError, DomainId](
          DomainId
            .fromProtoPrimitive(request.domainId, "domain_id")
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
      syncDomain <- EitherT
        .fromEither[Future](
          service
            .readySyncDomainById(domainId)
            .toRight(InvalidDomainId.Error(request.domainId))
        )
        .leftWiden[BaseCantonError]
      trafficState <- EitherT.liftF[Future, BaseCantonError, TrafficState](
        syncDomain.getTrafficControlState
      )
    } yield {
      v30.TrafficControlStateResponse(Some(TrafficStateAdmin.toProto(trafficState)))
    }

    CantonGrpcUtil.mapErrNew(result)
  }
}
