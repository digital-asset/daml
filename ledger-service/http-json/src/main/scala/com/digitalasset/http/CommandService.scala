// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitRequest
}

import scala.concurrent.Future

class CommandService(
    submitAndWaitForTransaction: SubmitAndWaitRequest => Future[
      SubmitAndWaitForTransactionResponse]) {

  def create(jwtPayload: domain.JwtPayload) = {
    ???
  }

}
