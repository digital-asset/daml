// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToDomain
import com.digitalasset.canton.topology.SynchronizerId
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
      synchronizerId <- EitherT
        .fromEither[Future]
        .apply[CantonError, SynchronizerId](
          SynchronizerId
            .fromProtoPrimitive(request.synchronizerId, "synchronizer_id")
            .leftMap(ProtoDeserializationFailure.Wrap(_))
        )
      connectedSynchronizer <- EitherT
        .fromEither[Future](
          service
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight(NotConnectedToDomain.Error(request.synchronizerId))
        )
        .leftWiden[BaseCantonError]
      trafficState <- EitherT.right[BaseCantonError](
        connectedSynchronizer.getTrafficControlState
      )
    } yield {
      v30.TrafficControlStateResponse(Some(TrafficStateAdmin.toProto(trafficState)))
    }

    CantonGrpcUtil.mapErrNew(result)
  }
}
