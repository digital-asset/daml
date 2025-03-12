// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.mediator.admin.v30.{
  MediatorScanServiceGrpc,
  Verdict,
  VerdictsResponse,
}
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

object MediatorScanCommands {
  abstract class BaseScanCommand[Req, Res, Ret] extends GrpcAdminCommand[Req, Res, Ret] {
    override type Svc = MediatorScanServiceGrpc.MediatorScanServiceStub

    override def createService(
        channel: ManagedChannel
    ): MediatorScanServiceGrpc.MediatorScanServiceStub =
      v30.MediatorScanServiceGrpc.stub(channel)

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  final case class MediatorVerdicts(
      mostRecentlyReceivedRecordTimeOfResult: Option[CantonTimestamp] = None,
      mostRecentlyReceivedRecordTimeOfRequest: Option[CantonTimestamp] = None,
      override val observer: StreamObserver[v30.Verdict],
  )(override implicit val loggingContext: ErrorLoggingContext)
      extends BaseScanCommand[v30.VerdictsRequest, AutoCloseable, AutoCloseable]
      with SubscribeBase[v30.VerdictsRequest, v30.VerdictsResponse, v30.Verdict] {

    override def doRequest(
        service: Svc,
        request: v30.VerdictsRequest,
        rawObserver: StreamObserver[v30.VerdictsResponse],
    ): Unit = service.verdicts(request, rawObserver)

    override def extractResults(response: VerdictsResponse): IterableOnce[Verdict] =
      response.verdict

    override protected def createRequest(): Either[String, v30.VerdictsRequest] =
      Right(
        v30.VerdictsRequest(
          mostRecentlyReceivedRecordTimeOfResult.map(_.toProtoTimestamp),
          mostRecentlyReceivedRecordTimeOfRequest.map(_.toProtoTimestamp),
        )
      )
  }

}
