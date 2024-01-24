// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.traffic.TrafficStateController.TrafficControlError
import com.digitalasset.canton.participant.traffic.TrafficStateController.TrafficControlError.TrafficStateNotFound
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.traffic.MemberTrafficStatus

import scala.concurrent.{ExecutionContext, Future}

class GrpcTrafficControlService(
    service: CantonSyncService,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends TrafficControlServiceGrpc.TrafficControlService
    with NamedLogging
    with NoTracing {
  override def trafficControlState(
      request: TrafficControlStateRequest
  ): Future[TrafficControlStateResponse] = {
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
            .toRight(TrafficControlError.DomainIdNotFound.Error(domainId))
        )
        .leftWiden[CantonError]
      trafficStateOpt <- EitherT.liftF[Future, CantonError, Option[MemberTrafficStatus]](
        syncDomain.getTrafficControlState
      )
      trafficState <- EitherT
        .fromEither[Future](
          trafficStateOpt.toRight(TrafficStateNotFound.Error())
        )
        .leftWiden[CantonError]
    } yield {
      TrafficControlStateResponse(Some(trafficState.toProtoV0))
    }

    CantonGrpcUtil.mapErrNew(result)
  }
}
