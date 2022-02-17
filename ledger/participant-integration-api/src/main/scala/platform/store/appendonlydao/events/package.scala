// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao

// TODO append-only: revisit visibility, and necessity during cleanup
/** Type aliases used throughout the package
  */
package object events {

  import com.daml.lf.value.{Value => lfval}
  type ContractId = lfval.ContractId
  val ContractId = com.daml.lf.value.Value.ContractId
  type Value = lfval.VersionedValue
  type Contract = lfval.VersionedContractInstance
  val Contract = lfval.VersionedContractInstance

  import com.daml.lf.{transaction => lftx}
  type NodeId = lftx.NodeId
  type Node = lftx.Node
  type Create = lftx.Node.Create
  type Exercise = lftx.Node.Exercise
  type Fetch = lftx.Node.Fetch
  type LookupByKey = lftx.Node.LookupByKey
  type Key = lftx.GlobalKey
  val Key = lftx.GlobalKey

  import com.daml.lf.{data => lfdata}
  type Party = lfdata.Ref.Party
  val Party = lfdata.Ref.Party
  type Identifier = lfdata.Ref.Identifier
  val Identifier = lfdata.Ref.Identifier
  type QualifiedName = lfdata.Ref.QualifiedName
  val QualifiedName = lfdata.Ref.QualifiedName
  type DottedName = lfdata.Ref.DottedName
  val DottedName = lfdata.Ref.DottedName
  type ModuleName = lfdata.Ref.ModuleName
  val ModuleName = lfdata.Ref.ModuleName
  type LedgerString = lfdata.Ref.LedgerString
  val LedgerString = lfdata.Ref.LedgerString
  type TransactionId = lfdata.Ref.LedgerString
  val TransactionId = lfdata.Ref.LedgerString
  type WorkflowId = lfdata.Ref.LedgerString
  val WorkflowId = lfdata.Ref.LedgerString
  type ChoiceName = lfdata.Ref.ChoiceName
  val ChoiceName = lfdata.Ref.ChoiceName
  type PackageId = lfdata.Ref.PackageId
  val PackageId = lfdata.Ref.PackageId
  type WitnessRelation[A] = lfdata.Relation.Relation[A, Party]
  type DisclosureRelation = WitnessRelation[NodeId]
  type DivulgenceRelation = WitnessRelation[ContractId]
  private[store] type FilterRelation =
    lfdata.Relation.Relation[Party, lfdata.Ref.Identifier]
  val Relation = lfdata.Relation.Relation

  import com.daml.lf.crypto
  type Hash = crypto.Hash
}
