// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import com.daml.ledger.participant.state.v1.AbsoluteContractInst
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.platform.store.entries.LedgerEntry

/**
  * Every time the ledger persists a transactions, the active contract set (ACS) is updated.
  * Updating the ACS requires knowledge of blinding info, which is not included in LedgerEntry.Transaction.
  * The SqlLedger persistence queue Transaction elements are therefore enriched with blinding info.
  */
sealed abstract class PersistenceEntry extends Product with Serializable {
  def entry: LedgerEntry
}

object PersistenceEntry {
  final case class Rejection(entry: LedgerEntry.Rejection) extends PersistenceEntry

  final case class Transaction(
      entry: LedgerEntry.Transaction,
      globalDivulgence: Relation[AbsoluteContractId, Party],
      divulgedContracts: List[(AbsoluteContractId, AbsoluteContractInst)]
  ) extends PersistenceEntry

  final case class Checkpoint(entry: LedgerEntry.Checkpoint) extends PersistenceEntry
}
