// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres

// Copied here to make it safe against future refactoring
// in production code
/**
  * Type aliases used throughout the package
  */
package object v29_fix_participant_events {

  import com.daml.lf.value.{Value => lfval}
  type ContractId = lfval.AbsoluteContractId

  import com.daml.lf.{transaction => lftx}
  type NodeId = lftx.Transaction.NodeId
  type Transaction = lftx.GenTransaction.WithTxValue[NodeId, ContractId]
  type Create = lftx.Node.NodeCreate.WithTxValue[ContractId]
  type Exercise = lftx.Node.NodeExercises.WithTxValue[NodeId, ContractId]

  import com.daml.lf.{data => lfdata}
  type Party = lfdata.Ref.Party
  type Identifier = lfdata.Ref.Identifier
  type LedgerString = lfdata.Ref.LedgerString
  type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  type DisclosureRelation = WitnessRelation[NodeId]
  val Relation = lfdata.Relation.Relation
}
