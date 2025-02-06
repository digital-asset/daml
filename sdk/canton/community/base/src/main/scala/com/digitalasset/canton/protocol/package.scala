// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.sequencing.protocol.OpenEnvelope
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.*
import com.digitalasset.daml.lf.value.Value

/** Provides shorthands for general purpose types.
  * <p>
  * Most notably, it provides a facade for Daml-LF transactions and nodes.
  * By default, code should access Daml-LF transaction and nodes types through this facade.
  */
package object protocol {

  /** Shorthand for Daml-LF contract ids */
  type LfContractId = Value.ContractId
  val LfContractId: Value.ContractId.type = Value.ContractId

  type LfNodeId = NodeId
  val LfNodeId: NodeId.type = NodeId

  /** Shorthand for Daml-LF transaction wrapped in versioned transaction in turn wrapped in
    * committed or submitted transaction
    */
  type LfTransaction = Transaction
  val LfTransaction: Transaction.type = Transaction

  type LfVersionedTransaction = VersionedTransaction
  val LfVersionedTransaction: VersionedTransaction.type = VersionedTransaction

  type LfCommittedTransaction = CommittedTransaction
  val LfCommittedTransaction: CommittedTransaction.type = CommittedTransaction

  type LfSubmittedTransaction = SubmittedTransaction
  val LfSubmittedTransaction: SubmittedTransaction.type = SubmittedTransaction

  type LfLanguageVersion = LanguageVersion
  val LfLanguageVersion: LanguageVersion.type = LanguageVersion

  val LfTransactionVersion: TransactionVersion.type = TransactionVersion

  val DummyTransactionVersion: LfLanguageVersion = LanguageVersion.v2_dev

  // Ledger transaction statistics based on lf transaction nodes
  type LedgerTransactionNodeStatistics = TransactionNodeStatistics
  val LedgerTransactionNodeStatistics: TransactionNodeStatistics.type = TransactionNodeStatistics

  /** Shorthand for Daml-LF nodes.
    * Nodes include `NodeId`s of their children.
    * Children need to be looked up in the underlying transaction.
    */
  type LfNode = Node

  /** Shorthand for Daml-LF "action" nodes (all node types besides "rollback" nodes)
    */
  type LfActionNode = Node.Action

  /** Shorthand for create nodes. */
  type LfNodeCreate = Node.Create
  val LfNodeCreate: Node.Create.type = Node.Create

  /** Shorthand for fetch nodes. */
  type LfNodeFetch = Node.Fetch
  val LfNodeFetch: Node.Fetch.type = Node.Fetch

  /** Shorthand for exercise nodes.
    * Nodes include `NodeId`s of their children.
    * Children need to be looked up in the underlying transaction.
    */
  type LfNodeExercises = Node.Exercise
  val LfNodeExercises: Node.Exercise.type = Node.Exercise

  /** Shorthand for lookup by key nodes. */
  type LfNodeLookupByKey = Node.LookupByKey
  val LfNodeLookupByKey: Node.LookupByKey.type = Node.LookupByKey

  /** Shorthand for rollback nodes. */
  type LfNodeRollback = Node.Rollback
  val LfNodeRollback: Node.Rollback.type = Node.Rollback

  /** Shorthand for leaf only action nodes. */
  type LfLeafOnlyActionNode = Node.LeafOnlyAction

  /** Shorthand for contract instances. */
  type LfContractInst = Value.VersionedContractInstance
  val LfContractInst: Value.VersionedContractInstance.type = Value.VersionedContractInstance

  type LfHash = Hash
  val LfHash: Hash.type = Hash

  /** Shorthand for global contract keys (with template_id). */
  type LfGlobalKey = GlobalKey
  val LfGlobalKey: GlobalKey.type = GlobalKey

  type LfGlobalKeyWithMaintainers = GlobalKeyWithMaintainers
  val LfGlobalKeyWithMaintainers: GlobalKeyWithMaintainers.type = GlobalKeyWithMaintainers

  type LfTemplateId = Ref.TypeConName
  val LfTemplateId: Ref.TypeConName.type = Ref.TypeConName

  type LfChoiceName = Ref.ChoiceName
  val LfChoiceName: Ref.ChoiceName.type = Ref.ChoiceName

  type RequestProcessor[VT <: ViewType] =
    Phase37Processor[RequestAndRootHashMessage[OpenEnvelope[EncryptedViewMessage[VT]]]]

  def maxTransactionVersion(versions: NonEmpty[Seq[LfLanguageVersion]]): LfLanguageVersion = {
    import Ordering.Implicits.*
    versions.reduceLeft[LfLanguageVersion](_ max _)
  }

}
