// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.config_management_service.{
  ConfigManagementServiceGrpc,
  GetTimeModelRequest,
}

import scala.concurrent.Future

final class GetTimeModelAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "ConfigManagementService#GetTimeModel"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(ConfigManagementServiceGrpc.stub(channel), token).getTimeModel(new GetTimeModelRequest())

}
