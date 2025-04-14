// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.command_service.{
  SubmitAndWaitForReassignmentRequest,
  SubmitAndWaitForTransactionRequest,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.command_submission_service.SubmitRequest
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v2.reassignment_commands.ReassignmentCommands
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, Filters, TransactionFormat}
import com.google.protobuf.timestamp.Timestamp

object MockMessages {

  val workflowId = "workflowId"
  val userId = "userId"
  val commandId = "commandId"
  val party = "party"
  val party2 = "party2"
  val ledgerEffectiveTime: Timestamp = Timestamp(0L, 0)

  val commands: Commands =
    Commands(
      workflowId = workflowId,
      userId = userId,
      commandId = commandId,
      commands = Nil,
      deduplicationPeriod = DeduplicationPeriod.Empty,
      minLedgerTimeAbs = None,
      minLedgerTimeRel = None,
      actAs = Seq(party),
      readAs = Nil,
      submissionId = "",
      disclosedContracts = Nil,
      synchronizerId = "",
      packageIdSelectionPreference = Nil,
      prefetchContractKeys = Nil,
    )

  val reassignmentCommands: ReassignmentCommands =
    ReassignmentCommands(
      workflowId = workflowId,
      userId = userId,
      submitter = party,
      commandId = commandId,
      submissionId = "",
      commands = Nil,
    )

  val submitRequest: SubmitRequest = SubmitRequest(Some(commands))

  private val eventFormat =
    EventFormat(
      filtersByParty = Map(party -> Filters(cumulative = Nil)),
      filtersForAnyParty = None,
      verbose = true,
    )

  private val transactionFormat = TransactionFormat(
    eventFormat = Some(eventFormat),
    transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
  )

  val submitAndWaitRequest: SubmitAndWaitRequest = SubmitAndWaitRequest(Some(commands))
  val submitAndWaitForTransactionRequest: SubmitAndWaitForTransactionRequest =
    SubmitAndWaitForTransactionRequest(Some(commands), Some(transactionFormat))
  val submitAndWaitForReassignmentRequest: SubmitAndWaitForReassignmentRequest =
    SubmitAndWaitForReassignmentRequest(Some(reassignmentCommands), Some(eventFormat))

}
