// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
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

  private val transactionFormat = TransactionFormat(
    eventFormat = Some(
      EventFormat(
        filtersByParty = Map(party -> Filters()),
        verbose = true,
      )
    ),
    transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
  )

  val submitAndWaitRequest: SubmitAndWaitRequest = SubmitAndWaitRequest(Some(commands))
  val submitAndWaitForTransactionRequest: SubmitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(Some(commands), Some(transactionFormat))

}
