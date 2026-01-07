// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import cats.implicits.toFunctorOps
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.protocol.GenContractInstance
import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.Relation
import com.digitalasset.daml.lf.transaction.{FatContractInstance, Node, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

object InputContractPackages {

  /** Returns a mapping from all contract ids referenced in the transaction to their package ids,
    * excluding those that are created within the transaction.
    */
  def forTransaction(tx: Transaction): data.Relation[ContractId, LfPackageId] =
    tx.fold(data.Relation.empty[ContractId, LfPackageId]) {
      case (
            acc,
            (_, Node.Exercise(coid, _, templateId, _, _, _, _, _, _, _, _, _, _, _, _, _, _)),
          ) =>
        Relation.update(acc, coid, templateId.packageId)
      case (acc, (_, Node.Fetch(coid, _, templateId, _, _, _, _, _, _, _))) =>
        Relation.update(acc, coid, templateId.packageId)
      case (acc, (_, Node.LookupByKey(_, templateId, _, Some(coid), _))) =>
        Relation.update(acc, coid, templateId.packageId)
      case (acc, _) => acc
    } -- tx.localContracts.keySet

  /** Merges two maps, returning an error if their key sets differ. */
  private[events] def strictZipByKey[K, V1, V2](
      m1: Map[K, V1],
      m2: Map[K, V2],
  ): Either[Set[K], Map[K, (V1, V2)]] = {
    val keys1 = m1.keySet
    val keys2 = m2.keySet
    Either.cond(
      keys1 == keys2,
      keys1.view.map(k => k -> (m1(k), m2(k))).toMap,
      (keys1 union keys2) -- (keys1 intersect keys2),
    )
  }

  /** Returns a mapping from all contract ids referenced in the transaction to their (contract
    * instance, package id), excluding those that are created within the transaction. Fails if the
    * set of contract ids in the transaction and in the provided contracts differ.
    */
  def forTransactionWithContracts(
      tx: Transaction,
      contracts: Map[ContractId, GenContractInstance],
  ): Either[Set[ContractId], Map[ContractId, (FatContractInstance, Set[LfPackageId])]] =
    strictZipByKey(contracts.fmap(_.inst), forTransaction(tx))

}
