// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.value.{Value => Val}

class Normalization {

  /** This class provides methods to normalize a transaction and embedded values.
    *
    * Informal spec: normalization is the result of serialization and deserialization.
    *
    * Here we take care of the following:
    * - type information is dropped from Variant and Record values
    * - field names are dropped from Records
    * - values are normalized recursively
    * - all values embedded in transaction nodes are normalized
    * - version-specific normalization is applied to the 'byKey' fields of 'Node.Fetch' and 'Node.Exercises'
    *
    * We do not normalize the node-ids in the transaction here, but rather assume that
    * aspect of normalization has already been performed (by the engine, or by
    * deserialization).
    *
    * Eventually we would like that all aspects of normalization are achieved directly by
    * the transaction which is constructed by the engine. When this is done, we will no
    * longer need this separate normalization pass.
    */

  private type KWM = Node.KeyWithMaintainers
  private type VTX = VersionedTransaction

  def normalizeTx(vtx: VTX): VTX = {
    vtx match {
      case VersionedTransaction(_, nodes, _) =>
        VersionedTransaction(
          vtx.version,
          nodes.map { case (k, v) =>
            (k, normNode(v))
          },
          vtx.roots,
        )
    }
  }

  private def normNode(
      node: Node
  ): Node = {
    import scala.Ordering.Implicits.infixOrderingOps
    node match {

      case old: Node.Create =>
        old
          .copy(arg = normValue(old.version)(old.arg))
          .copy(key = old.key.map(normKWM(old.version)))

      case old: Node.Fetch =>
        (if (old.version >= TransactionVersion.minByKey) {
           old
         } else {
           old.copy(byKey = false)
         })
          .copy(
            key = old.key.map(normKWM(old.version))
          )

      case old: Node.Exercise =>
        (if (old.version >= TransactionVersion.minByKey) {
           old
         } else {
           old.copy(byKey = false)
         })
          .copy(
            chosenValue = normValue(old.version)(old.chosenValue),
            exerciseResult = old.exerciseResult.map(normValue(old.version)),
            key = old.key.map(normKWM(old.version)),
          )

      case old: Node.LookupByKey =>
        old.copy(
          key = normKWM(old.version)(old.key)
        )

      case old: Node.Rollback => old

    }
  }

  private def normValue(version: TransactionVersion)(x: Val): Val = {
    Util.assertNormalizeValue(x, version)
  }

  private def normKWM(version: TransactionVersion)(x: KWM): KWM = {
    x match {
      case Node.KeyWithMaintainers(key, maintainers) =>
        Node.KeyWithMaintainers(normValue(version)(key), maintainers)
    }
  }

}

object Normalization {
  def normalizeTx(tx: VersionedTransaction): VersionedTransaction = {
    new Normalization().normalizeTx(tx)
  }
}
