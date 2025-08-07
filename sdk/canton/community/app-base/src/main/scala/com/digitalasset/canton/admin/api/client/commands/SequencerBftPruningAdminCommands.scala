// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import com.digitalasset.canton.admin.pruning.v30.PruningSchedule
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.SequencerBftPruningAdministrationServiceGrpc.SequencerBftPruningAdministrationServiceStub
import com.digitalasset.canton.sequencer.admin.v30.SetMinBlocksToKeepResponse
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.{
  BftOrdererPruningSchedule,
  BftPruningStatus,
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
      v30.SequencerBftPruningAdministrationServiceGrpc.stub(channel)
  }

  final case class Prune(retention: PositiveDurationSeconds, minBlocksToKeep: Int)
      extends BaseSequencerBftAdministrationCommand[
        v30.BftPruneRequest,
        v30.BftPruneResponse,
        String,
      ] {

    override protected def submitRequest(
        service: SequencerBftPruningAdministrationServiceStub,
        request: v30.BftPruneRequest,
    ): Future[v30.BftPruneResponse] = service.bftPrune(request)

    override protected def createRequest(): Either[String, v30.BftPruneRequest] =
      Right(v30.BftPruneRequest(Some(retention.toProtoPrimitive), minBlocksToKeep))

    override protected def handleResponse(response: v30.BftPruneResponse): Either[String, String] =
      Right(response.message)
  }

  final case class Status()
      extends BaseSequencerBftAdministrationCommand[
        v30.BftPruningStatusRequest,
        v30.BftPruningStatusResponse,
        BftPruningStatus,
      ] {

    override protected def submitRequest(
        service: SequencerBftPruningAdministrationServiceStub,
        request: v30.BftPruningStatusRequest,
    ): Future[v30.BftPruningStatusResponse] =
      service.bftPruningStatus(request)

    override protected def createRequest(): Either[String, v30.BftPruningStatusRequest] = Right(
      v30.BftPruningStatusRequest()
    )

    override protected def handleResponse(
        response: v30.BftPruningStatusResponse
    ): Either[String, BftPruningStatus] = for {
      latestBlockTimestamp <- ProtoConverter
        .parseRequired(
          CantonTimestamp.fromProtoTimestamp,
          "latest_block_timestamp",
          response.latestBlockTimestamp,
        )
        .leftMap(_.toString)
    } yield BftPruningStatus(
      latestBlockEpochNumber = EpochNumber(response.latestBlockEpoch),
      latestBlockNumber = BlockNumber(response.latestBlock),
      latestBlockTimestamp = latestBlockTimestamp,
      lowerBoundEpochNumber = EpochNumber(response.lowerBoundEpoch),
      lowerBoundBlockNumber = BlockNumber(response.lowerBoundBlock),
    )
  }

  final case class SetBftSchedule(
      cron: String,
      maxDuration: PositiveDurationSeconds,
      retention: PositiveDurationSeconds,
      minBlocksToKeep: Int,
  ) extends BaseSequencerBftAdministrationCommand[
        v30.SetBftScheduleRequest,
        v30.SetBftScheduleResponse,
        Unit,
      ] {
    override protected def createRequest(): Right[String, v30.SetBftScheduleRequest] =
      Right(
        v30.SetBftScheduleRequest(
          Some(
            v30.BftOrdererPruningSchedule(
              Some(
                PruningSchedule(
                  cron,
                  Some(maxDuration.toProtoPrimitive),
                  Some(retention.toProtoPrimitive),
                )
              ),
              minBlocksToKeep,
            )
          )
        )
      )

    override protected def submitRequest(
        service: Svc,
        request: v30.SetBftScheduleRequest,
    ): Future[v30.SetBftScheduleResponse] =
      service.setBftSchedule(request)

    override protected def handleResponse(
        response: v30.SetBftScheduleResponse
    ): Either[String, Unit] =
      response match {
        case v30.SetBftScheduleResponse() => Either.unit
      }
  }

  final case class GetBftSchedule()
      extends BaseSequencerBftAdministrationCommand[
        v30.GetBftScheduleRequest,
        v30.GetBftScheduleResponse,
        Option[BftOrdererPruningSchedule],
      ] {
    override protected def createRequest(): Right[String, v30.GetBftScheduleRequest] = Right(
      v30.GetBftScheduleRequest()
    )

    override protected def submitRequest(
        service: Svc,
        request: v30.GetBftScheduleRequest,
    ): Future[v30.GetBftScheduleResponse] =
      service.getBftSchedule(request)

    override protected def handleResponse(
        response: v30.GetBftScheduleResponse
    ): Either[String, Option[BftOrdererPruningSchedule]] =
      response.schedule.fold(
        Right(None): Either[String, Option[BftOrdererPruningSchedule]]
      )(BftOrdererPruningSchedule.fromProtoV30(_).bimap(_.message, Some(_)))
  }

  final case class SetMinBlocksToKeep(
      minBlocksToKeep: Int
  ) extends BaseSequencerBftAdministrationCommand[
        v30.SetMinBlocksToKeepRequest,
        v30.SetMinBlocksToKeepResponse,
        Unit,
      ] {
    override protected def createRequest(): Right[String, v30.SetMinBlocksToKeepRequest] =
      Right(v30.SetMinBlocksToKeepRequest(minBlocksToKeep))

    override protected def submitRequest(
        service: Svc,
        request: v30.SetMinBlocksToKeepRequest,
    ): Future[SetMinBlocksToKeepResponse] = service.setMinBlocksToKeep(request)

    override protected def handleResponse(
        response: SetMinBlocksToKeepResponse
    ): Either[String, Unit] =
      response match {
        case v30.SetMinBlocksToKeepResponse() => Either.unit
      }
  }
}
