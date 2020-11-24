// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.daml.lf.data.Ref.LedgerString

package object ledger {

  /** Identifiers used to correlate submission with results. */
  type CommandId = LedgerString
  val CommandId: LedgerString.type = LedgerString
  val CommandIdKey: String = "daml.command_id"

  /** Identifiers used for correlating submission with a workflow.  */
  type WorkflowId = LedgerString
  val WorkflowId: LedgerString.type = LedgerString
  val WorkflowIdKey: String = "daml.workflow_id"

  /** Identifiers for submitting client applications. */
  type ApplicationId = LedgerString
  val ApplicationId: LedgerString.type = LedgerString

  type EventId = lf.ledger.EventId
  val EventId: lf.ledger.EventId.type = lf.ledger.EventId

  val TransactionId: LedgerString.type = LedgerString
  type TransactionId = LedgerString
  val TransactionIdKey: String = "daml.transaction_id"

  val OffsetKey: String = "daml.offset"
  val OffsetRangeFromKey: String = "daml.offset_from"
  val OffsetRangeToKey: String = "daml.offset_to"
}
