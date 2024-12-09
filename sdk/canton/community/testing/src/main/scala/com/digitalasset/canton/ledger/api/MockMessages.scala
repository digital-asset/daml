// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.google.protobuf.timestamp.Timestamp

object MockMessages {

  val workflowId = "workflowId"
  val applicationId = "applicationId"
  val commandId = "commandId"
  val party = "party"
  val party2 = "party2"
  val ledgerEffectiveTime: Timestamp = Timestamp(0L, 0)

  val commands: Commands =
    Commands(workflowId, applicationId, commandId, Nil, actAs = Seq(party))

  val submitRequest: SubmitRequest = SubmitRequest(Some(commands))

  val submitAndWaitRequest: SubmitAndWaitRequest = SubmitAndWaitRequest(Some(commands))

}
