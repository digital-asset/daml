// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.ledger.offset.Offset

/** Type aliases used throughout the package */
package object platform {
  import com.daml.lf.value.{Value as lfval}
  private[platform] type ContractId = lfval.ContractId
  private[platform] val ContractId = com.daml.lf.value.Value.ContractId
  private[platform] type Value = lfval.VersionedValue
  private[platform] type Contract = lfval.VersionedContractInstance
  private[platform] val Contract = lfval.VersionedContractInstance

  import com.daml.lf.{transaction as lftx}
  private[platform] type NodeId = lftx.NodeId
  private[platform] type Node = lftx.Node
  private[platform] type Create = lftx.Node.Create
  private[platform] type Exercise = lftx.Node.Exercise
  private[platform] type Key = lftx.GlobalKey
  private[platform] val Key = lftx.GlobalKey

  import com.daml.lf.{data as lfdata}
  private[platform] type Party = lfdata.Ref.Party
  private[platform] val Party = lfdata.Ref.Party
  private[platform] type Identifier = lfdata.Ref.Identifier
  private[platform] val Identifier = lfdata.Ref.Identifier
  private[platform] type QualifiedName = lfdata.Ref.QualifiedName
  private[platform] val QualifiedName = lfdata.Ref.QualifiedName
  private[platform] type DottedName = lfdata.Ref.DottedName
  private[platform] val DottedName = lfdata.Ref.DottedName
  private[platform] type ModuleName = lfdata.Ref.ModuleName
  private[platform] val ModuleName = lfdata.Ref.ModuleName
  private[platform] type LedgerString = lfdata.Ref.LedgerString
  private[platform] val LedgerString = lfdata.Ref.LedgerString
  private[platform] type TransactionId = lfdata.Ref.LedgerString
  private[platform] val TransactionId = lfdata.Ref.LedgerString
  private[platform] type WorkflowId = lfdata.Ref.LedgerString
  private[platform] val WorkflowId = lfdata.Ref.LedgerString
  private[platform] type SubmissionId = lfdata.Ref.SubmissionId
  private[platform] val SubmissionId = lfdata.Ref.SubmissionId
  private[platform] type ApplicationId = lfdata.Ref.ApplicationId
  private[platform] val ApplicationId = lfdata.Ref.ApplicationId
  private[platform] type CommandId = lfdata.Ref.CommandId
  private[platform] val CommandId = lfdata.Ref.CommandId
  private[platform] type ParticipantId = lfdata.Ref.ParticipantId
  private[platform] val ParticipantId = lfdata.Ref.ParticipantId
  private[platform] type ChoiceName = lfdata.Ref.ChoiceName
  private[platform] val ChoiceName = lfdata.Ref.ChoiceName
  private[platform] type PackageId = lfdata.Ref.PackageId
  private[platform] val PackageId = lfdata.Ref.PackageId
  private[platform] type Relation[A, B] = lfdata.Relation[A, B]
  private[platform] type UserId = lfdata.Ref.UserId
  private[platform] val UserId = lfdata.Ref.UserId

  private[platform] type FilterRelation = Relation[lfdata.Ref.Identifier, Party]

  import com.daml.lf.crypto
  private[platform] type Hash = crypto.Hash

  private[platform] type PruneBuffers = Offset => Unit
}
