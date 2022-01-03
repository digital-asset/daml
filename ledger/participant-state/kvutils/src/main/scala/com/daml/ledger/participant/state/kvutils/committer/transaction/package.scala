// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

package object transaction {
  private[transaction] type RawContractId = String

  private[transaction] type Step = CommitStep[DamlTransactionEntrySummary]
}
