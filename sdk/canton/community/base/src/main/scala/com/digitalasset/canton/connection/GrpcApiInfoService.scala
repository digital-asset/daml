// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.connection

import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc.ApiInfoService
import com.digitalasset.canton.connection.v30.{GetApiInfoRequest, GetApiInfoResponse}

import scala.concurrent.Future

class GrpcApiInfoService(name: String) extends ApiInfoService {
  override def getApiInfo(request: GetApiInfoRequest): Future[GetApiInfoResponse] = {
    Future.successful(GetApiInfoResponse(name))
  }
}
