// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres

// Copied here (with some modifications) to make it
// safe against future refactoring in production code
/**
  * Type aliases used throughout the package
  */
package object v27_backfill_participant_contracts {

  import com.daml.lf.value.{Value => lfval}
  private[postgres] type ContractId = lfval.AbsoluteContractId
  private[postgres] val ContractId = lfval.AbsoluteContractId
  private[postgres] type Value = lfval.VersionedValue[ContractId]
  private[postgres] type Contract = lfval.ContractInst[Value]
  private[postgres] val Contract = lfval.ContractInst

  import com.daml.lf.{transaction => lftx}
  private[postgres] type NodeId = lftx.Transaction.NodeId
  private[postgres] type Transaction = lftx.GenTransaction.WithTxValue[NodeId, ContractId]
  private[postgres] type Node = lftx.Node.GenNode.WithTxValue[NodeId, ContractId]
  private[postgres] type Create = lftx.Node.NodeCreate.WithTxValue[ContractId]
  private[postgres] type Exercise = lftx.Node.NodeExercises.WithTxValue[NodeId, ContractId]
  private[postgres] type Fetch = lftx.Node.NodeFetch.WithTxValue[ContractId]
  private[postgres] type LookupByKey = lftx.Node.NodeLookupByKey.WithTxValue[ContractId]
  private[postgres] type Key = lftx.Node.GlobalKey
  private[postgres] val Key = lftx.Node.GlobalKey

  import com.daml.lf.{data => lfdata}
  private[postgres] type Party = lfdata.Ref.Party
  private[postgres] type Identifier = lfdata.Ref.Identifier
  private[postgres] val Identifier = lfdata.Ref.Identifier
  private[postgres] type LedgerString = lfdata.Ref.LedgerString
  private[postgres] val LedgerString = lfdata.Ref.LedgerString
  private[postgres] type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  private[postgres] type DisclosureRelation = WitnessRelation[NodeId]
  private[postgres] type DivulgenceRelation = WitnessRelation[ContractId]
  private[postgres] type FilterRelation = lfdata.Relation.Relation[Party, lfdata.Ref.Identifier]
  private[postgres] val Relation = lfdata.Relation.Relation
}
