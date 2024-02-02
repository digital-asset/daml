// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.functor.*
import com.digitalasset.canton.crypto.{HashOps, RandomOps}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.{LfKeyResolver, LfPartyId}

/** Encapsulates the most widely used representations of a transaction.
  */
trait ExampleTransaction {

  def cryptoOps: HashOps with RandomOps

  /** Set of parties who are informees of an action (root or not) in the transaction */
  def allInformees: Set[LfPartyId] = fullInformeeTree.allInformees

  /** The transaction with unsuffixed contract IDs and the transaction version */
  def versionedUnsuffixedTransaction: LfVersionedTransaction

  /** Map from the nodes of the transaction to their seed if they need a seed */
  def seeds: Map[LfNodeId, LfHash] = metadata.seeds

  /** Metadata for the transaction as a whole */
  def metadata: TransactionMetadata

  /** @throws IllegalArgumentException if [[versionedUnsuffixedTransaction]] is malformed
    */
  def wellFormedUnsuffixedTransaction: WellFormedTransaction[WithoutSuffixes] =
    WellFormedTransaction.normalizeAndAssert(
      versionedUnsuffixedTransaction,
      metadata,
      WithoutSuffixes,
    )

  /** The key resolver to be used for iterating over the transaction nodes */
  def keyResolver: LfKeyResolver

  /** The root views of the transaction in execution order */
  def rootViewDecompositions: Seq[TransactionViewDecomposition.NewView]

  /** The root views of the transaction in execution order */
  def rootViews: Seq[TransactionView]

  /** Associates all views (root or not) with their (direct and indirect) subviews (in execution order).
    * Recall that every view is also a subview of itself.
    */
  def viewWithSubviews: Seq[(TransactionView, Seq[TransactionView])]

  def inputContracts: Map[LfContractId, SerializableContract] =
    transactionViewTrees.flatMap(_.viewParticipantData.coreInputs).toMap.fmap(_.contract)

  def transactionTree: GenTransactionTree

  def transactionId: TransactionId = transactionTree.transactionId

  def fullInformeeTree: FullInformeeTree

  /** The sequence of reinterpreted action descriptions for all views in execution order, with their witnesses */
  def reinterpretedSubtransactions: Seq[
    (
        FullTransactionViewTree,
        (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
        Witnesses,
    )
  ]

  /** All transaction view trees, including those corresponding to non-root views, in execution order */
  def transactionViewTrees: Seq[FullTransactionViewTree] = reinterpretedSubtransactions.map(_._1)

  /** All transaction view trees, including those corresponding to non-root views, in execution order */
  def transactionViewTreesWithWitnesses: Seq[(FullTransactionViewTree, Witnesses)] =
    reinterpretedSubtransactions.map(r => r._1 -> r._3)

  /** Transaction view trees for root views, in execution order */
  def rootTransactionViewTrees: Seq[FullTransactionViewTree]

  /** The transaction with suffixed contract ids and the transaction version. */
  def versionedSuffixedTransaction: LfVersionedTransaction

  /** @throws IllegalArgumentException if [[versionedSuffixedTransaction]] is malformed
    */
  def wellFormedSuffixedTransaction: WellFormedTransaction[WithSuffixes] =
    WellFormedTransaction.normalizeAndAssert(versionedSuffixedTransaction, metadata, WithSuffixes)

  /** Yields brief description of this example, which must be suitable for naming test cases.as part of usable to identify
    *
    * Implementing classes must overwrite this method.
    */
  override def toString: String = throw new UnsupportedOperationException(
    "Please overwrite the toString method."
  )
}
