// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

/**
  * Type aliases used throughout the package
  */
package object events {

  import com.digitalasset.daml.lf.value.{Value => lfval}
  private[events] type ContractId = lfval.AbsoluteContractId
  private[events] type Value = lfval.VersionedValue[ContractId]

  import com.digitalasset.daml.lf.{transaction => lftx}
  private[events] type NodeId = lftx.Transaction.NodeId
  private[events] type Transaction = lftx.GenTransaction.WithTxValue[NodeId, ContractId]
  private[events] type Node = lftx.Node.GenNode.WithTxValue[NodeId, ContractId]
  private[events] type Create = lftx.Node.NodeCreate.WithTxValue[ContractId]
  private[events] type Exercise = lftx.Node.NodeExercises.WithTxValue[NodeId, ContractId]

  import com.digitalasset.daml.lf.{data => lfdata}
  private[events] type Party = lfdata.Ref.Party
  private[events] type DisclosureRelation = lfdata.Relation.Relation[NodeId, Party]
  private[events] val DisclosureRelation = lfdata.Relation.Relation

}
