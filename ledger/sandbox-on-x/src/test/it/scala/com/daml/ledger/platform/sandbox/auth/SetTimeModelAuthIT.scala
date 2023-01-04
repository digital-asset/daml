// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.config_management_service.{
  ConfigManagementServiceGrpc,
  GetTimeModelRequest,
  SetTimeModelRequest,
  SetTimeModelResponse,
  TimeModel,
}

import scala.concurrent.Future

final class SetTimeModelAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "ConfigManagementService#SetTimeModel"

  private def setTimeModel(
      token: Option[String],
      timeModel: TimeModel,
      configurationGeneration: Long,
  ): Future[SetTimeModelResponse] = {
    val request = new SetTimeModelRequest(
      newTimeModel = Some(timeModel),
      maximumRecordTime = Some(com.google.protobuf.timestamp.Timestamp.of(100, 0)),
      configurationGeneration = configurationGeneration,
    )
    stub(ConfigManagementServiceGrpc.stub(channel), token).setTimeModel(request)
  }

  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    for {
      response <- stub(ConfigManagementServiceGrpc.stub(channel), token)
        .getTimeModel(new GetTimeModelRequest())
      _ <- setTimeModel(token, response.getTimeModel, response.configurationGeneration)
    } yield ()
  }

}
