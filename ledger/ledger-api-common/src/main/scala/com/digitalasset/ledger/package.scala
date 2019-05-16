// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset

import com.digitalasset.daml.lf.data.Ref.LedgerString

package object ledger {

  /** Identifiers used to correlate submission with results. */
  type CommandId = LedgerString

  /** Identifiers used for correlating submission with a workflow.  */
  type WorkflowId = LedgerString

  /** Identifiers for submitting client applications. */
  type ApplicationId = LedgerString

  type EventId = LedgerString

}
