// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.value.{Value => V}

import com.daml.lf.transaction.Node.{
  KeyWithMaintainers,
  GenNode,
  NodeCreate,
  NodeFetch,
  NodeLookupByKey,
  NodeExercises,
  NodeRollback,
}

class Normalization[Nid, Cid] {

  /** This class provides methods to normalize a transaction and embedded values.
    *
    * Informal spec: normalization is the result of serialization and deserialization.
    *
    * Here we take care of the following:
    * - type information is dropped from Variant and Record values
    * - field names are dropped from Records
    * - values are normalized recursively
    * - all values embedded in transaction nodes are normalized
    * - version-specific normalization is applied to the 'byKey' fields of 'NodeFetch' and 'NodeExercises'
    *
    * We do not normalize the node-ids in the transaction here, but rather assume that
    * aspect of normalization has already been performed (by the engine, or by
    * deserialization).
    *
    * Eventually we would like that all aspects of normalization are achieved directly by
    * the transaction which is constructed by the engine. When this is done, we will no
    * longer need this separate normalization pass.
    */

  private type Val = V[Cid]
  private type KWM = KeyWithMaintainers[Val]
  private type Node = GenNode[Nid, Cid]
  private type VTX = VersionedTransaction[Nid, Cid]

  def normalizeTx(vtx: VTX): VTX = {
    vtx match {
      case VersionedTransaction(_, nodes, roots) =>
        // TODO: Normalized version calc should be shared with code in asVersionedTransaction
        val version = roots.iterator.foldLeft(TransactionVersion.minVersion) { (acc, nodeId) =>
          import scala.Ordering.Implicits.infixOrderingOps
          nodes(nodeId).optVersion match {
            case Some(version) => acc max version
            case None => acc max TransactionVersion.minExceptions
          }
        }
        VersionedTransaction(
          version,
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

      case old: NodeCreate[_] =>
        old
          .copy(arg = normValue(old.arg))
          .copy(key = old.key.map(normKWM))

      case old: NodeFetch[_] =>
        (if (old.version >= TransactionVersion.minByKey) {
           old
         } else {
           old.copy(byKey = false)
         })
          .copy(
            key = old.key.map(normKWM)
          )

      case old: NodeExercises[_, _] =>
        (if (old.version >= TransactionVersion.minByKey) {
           old
         } else {
           old.copy(byKey = false)
         })
          .copy(
            chosenValue = normValue(old.chosenValue),
            exerciseResult = old.exerciseResult.map(normValue),
            key = old.key.map(normKWM),
          )

      case old: NodeLookupByKey[_] =>
        old.copy(
          key = normKWM(old.key)
        )

      case old: NodeRollback[_] => old

    }
  }

  // It's ok to code this in stack unaware fashion, as values  have a depth limit of 100
  private def normValue(x: Val): Val = {
    x match {

      // recursive cases (with normalization)
      case V.ValueRecord(_, fields) =>
        V.ValueRecord(
          tycon = None, //norm
          fields = fields.map { case (_, v) =>
            (
              None, //norm
              normValue(v),
            )
          },
        )

      case V.ValueVariant(_, variant, v) =>
        V.ValueVariant(
          None, //norm
          variant,
          normValue(v),
        )

      // other recursive cases
      case V.ValueList(list) => V.ValueList(list.map(normValue))
      case V.ValueOptional(opt) => V.ValueOptional(opt.map(normValue))
      case V.ValueGenMap(entries) =>
        V.ValueGenMap(entries.map { case (k, v) => (normValue(k), normValue(v)) })
      case V.ValueTextMap(x) => V.ValueTextMap(x.mapValue(normValue))

      // non-recursive cases
      case V.ValueContractId(_) => x
      case _: V.ValueCidlessLeaf => x
    }
  }

  private def normKWM(x: KWM): KWM = {
    x match {
      case KeyWithMaintainers(key, maintainers) =>
        KeyWithMaintainers(normValue(key), maintainers)
    }
  }

}
