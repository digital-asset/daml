// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import com.daml.api.util.DurationConversion
import com.daml.ledger.api.v1.admin.config_management_service.{
  ConfigManagementServiceGrpc,
  GetTimeModelRequest,
  SetTimeModelRequest,
  TimeModel => ProtobufTimeModel
}
import com.daml.ledger.participant.state.v1.TimeModel
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Channel

import scala.concurrent.{ExecutionContext, Future}

object TimeModelHelpers {
  def publishATimeModel(channel: Channel)(implicit ec: ExecutionContext): Future[Unit] = {
    val configService = ConfigManagementServiceGrpc.stub(channel)
    for {
      current <- configService.getTimeModel(GetTimeModelRequest())
      generation = current.configurationGeneration
      timeModel = TimeModel.reasonableDefault
      _ <- configService.setTimeModel(
        SetTimeModelRequest(
          "config-submission",
          Some(Timestamp(30, 0)),
          generation,
          Some(ProtobufTimeModel(
            avgTransactionLatency =
              Some(DurationConversion.toProto(timeModel.avgTransactionLatency)),
            minSkew = Some(DurationConversion.toProto(timeModel.minSkew)),
            maxSkew = Some(DurationConversion.toProto(timeModel.maxSkew))
          ))
        ))
    } yield ()
  }
}
