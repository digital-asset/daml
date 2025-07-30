// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.digitalasset.canton.admin.api.client.data.BftPruningStatus
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftPruningAdministrationServiceGrpc.SequencerBftPruningAdministrationServiceStub
import com.digitalasset.canton.sequencer.admin.v30.{
  BftPruneRequest,
  BftPruneResponse,
  BftPruningStatusRequest,
  BftPruningStatusResponse,
  SequencerBftPruningAdministrationServiceGrpc,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import io.grpc.ManagedChannel

import scala.concurrent.Future

object SequencerBftPruningAdminCommands {
  abstract class BaseSequencerBftAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {

    override type Svc =
      SequencerBftPruningAdministrationServiceStub

    override def createService(
        channel: ManagedChannel
    ): SequencerBftPruningAdministrationServiceStub =
      SequencerBftPruningAdministrationServiceGrpc.stub(channel)
  }

  final case class Prune(retention: PositiveDurationSeconds, minBlocksToKeep: Int)
      extends BaseSequencerBftAdministrationCommand[
        BftPruneRequest,
        BftPruneResponse,
        String,
      ] {

    override protected def submitRequest(
        service: SequencerBftPruningAdministrationServiceStub,
        request: BftPruneRequest,
    ): Future[BftPruneResponse] = service.bftPrune(request)

    override protected def createRequest(): Either[String, BftPruneRequest] =
      Right(BftPruneRequest(Some(retention.toProtoPrimitive), minBlocksToKeep))

    override protected def handleResponse(response: BftPruneResponse): Either[String, String] =
      Right(response.message)
  }

  final case class Status()
      extends BaseSequencerBftAdministrationCommand[
        BftPruningStatusRequest,
        BftPruningStatusResponse,
        BftPruningStatus,
      ] {

    override protected def submitRequest(
        service: SequencerBftPruningAdministrationServiceStub,
        request: BftPruningStatusRequest,
    ): Future[BftPruningStatusResponse] =
      service.bftPruningStatus(request)

    override protected def createRequest(): Either[String, BftPruningStatusRequest] = Right(
      BftPruningStatusRequest()
    )

    override protected def handleResponse(
        response: BftPruningStatusResponse
    ): Either[String, BftPruningStatus] = Right(
      BftPruningStatus(EpochNumber(response.lowerBoundEpoch), BlockNumber(response.lowerBoundBlock))
    )
  }

}
