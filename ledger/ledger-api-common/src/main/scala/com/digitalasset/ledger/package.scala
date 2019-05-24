// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import com.digitalasset.daml.lf.data.Ref.LedgerString

package object ledger {

  /** Identifiers used to correlate submission with results. */
  val CommandId: LedgerString.type = LedgerString
  type CommandId = CommandId.T

  /** Identifiers used for correlating submission with a workflow.  */
  val WorkflowId: LedgerString.type = LedgerString
  type WorkflowId = WorkflowId.T

  /** Identifiers for submitting client applications. */
  val ApplicationId: LedgerString.type = LedgerString
  type ApplicationId = ApplicationId.T

  val EventId: LedgerString.type = LedgerString
  type EventId = EventId.T

}
