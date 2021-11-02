// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.db.migration.postgres

// Copied here to make it safe against future refactoring
// in production code
/** Type aliases used throughout the package
  */
package object v25_backfill_participant_events {

  import com.daml.lf.value.{Value => lfval}
  private[migration] type ContractId = lfval.ContractId

  import com.daml.lf.{transaction => lftx}
  private[migration] type NodeId = lftx.NodeId
  private[migration] type Transaction = lftx.VersionedTransaction
  private[migration] type Create = lftx.Node.NodeCreate
  private[migration] type Exercise = lftx.Node.NodeExercises

  import com.daml.lf.{data => lfdata}
  private[migration] type Party = lfdata.Ref.Party
  private[migration] type Identifier = lfdata.Ref.Identifier
  private[migration] type LedgerString = lfdata.Ref.LedgerString
  private[migration] type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  private[migration] type DisclosureRelation = WitnessRelation[NodeId]
  private[migration] val Relation = lfdata.Relation.Relation
}
