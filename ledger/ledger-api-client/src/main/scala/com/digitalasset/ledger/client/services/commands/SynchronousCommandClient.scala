// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands

import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service._
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

class SynchronousCommandClient(commandService: CommandService) {

  def submitAndWait(submitAndWaitRequest: SubmitAndWaitRequest): Future[Empty] = {
    commandService.submitAndWait(submitAndWaitRequest)
  }
}
