// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.transaction.Transaction.ChildrenRecursion

/** Container for transaction statistics.
  *
  * @param creates number of creates nodes,
  * @param consumingExerciseByCids number of consuming exercises by contract ID nodes,
  * @param nonconsumingExerciseByCids number of non-consuming Exercises by contract ID nodes,
  * @param consumingExerciseByKeys number of consuming exercise by contract key nodes,
  * @param nonconsumingExerciseByKeys number of non-consuming exercise by key nodes,
  * @param fetchByCids number of fetch nodes,
  * @param fetchByKeys number of fetch by key  nodes,
  * @param lookupByKeys number of lookup by key  nodes,
  * @param rollbacks number of rollback nodes.
  */
final case class TransactionNodeStatistic(
    creates: Int,
    consumingExerciseByCids: Int,
    nonconsumingExerciseByCids: Int,
    consumingExerciseByKeys: Int,
    nonconsumingExerciseByKeys: Int,
    fetchByCids: Int,
    fetchByKeys: Int,
    lookupByKeys: Int,
    rollbacks: Int,
) {

  def +(that: TransactionNodeStatistic) =
    TransactionNodeStatistic(
      creates = this.creates + that.creates,
      consumingExerciseByCids = this.consumingExerciseByCids + that.consumingExerciseByCids,
      nonconsumingExerciseByCids =
        this.nonconsumingExerciseByCids + that.nonconsumingExerciseByCids,
      consumingExerciseByKeys = this.consumingExerciseByKeys + that.consumingExerciseByKeys,
      nonconsumingExerciseByKeys =
        this.nonconsumingExerciseByKeys + that.nonconsumingExerciseByKeys,
      fetcheByCids = this.fetcheByCids + that.fetcheByCids,
      fetchByKeys = this.fetchByKeys + that.fetchByKeys,
      lookupByKeys = this.lookupByKeys + that.lookupByKeys,
      rollbacks = this.rollbacks + that.rollbacks,
    )

  def exercisesByCid: Int = consumingExerciseByCids + nonconsumingExerciseByCids
  def exercisesByKey: Int = consumingExerciseByKeys + nonconsumingExerciseByKeys
  def exercises: Int = exercisesByCid + exercisesByKey
  def fetches: Int = fetcheByCids + fetchByKeys
  def actionNodes: Int = creates + exercises + fetches + lookupByKeys
  def nodes: Int = actionNodes + rollbacks
}

object TransactionNodeStatistic {

  val Empty = TransactionNodeStatistic(0, 0, 0, 0, 0, 0, 0, 0, 0)

  private[this] val numberOfFields = Empty.productArity

  private[this] val Seq(
    createsIdx,
    consumingExerciseByCidsIdx,
    nonconsumingExerciseCidsIdx,
    consumingExercisesByKeysIdx,
    nonconsumingExerciseByKeysIdx,
    fetchesIdx,
    fetchByKeysIdx,
    lookupByKeysIdx,
    rollbacksIdx,
  ) =
    (0 until numberOfFields)

  private[this] def emptyFields = Array.fill(numberOfFields)(0)

  private[this] def build(stats: Array[Int]) =
    TransactionNodeStatistic(
      creates = stats(createsIdx),
      consumingExerciseByCids = stats(consumingExerciseByCidsIdx),
      nonconsumingExerciseByCids = stats(nonconsumingExerciseCidsIdx),
      consumingExerciseByKeys = stats(consumingExercisesByKeysIdx),
      nonconsumingExerciseByKeys = stats(nonconsumingExerciseByKeysIdx),
      fetcheByCids = stats(fetchesIdx),
      fetchByKeys = stats(fetchByKeysIdx),
      lookupByKeys = stats(lookupByKeysIdx),
      rollbacks = stats(rollbacksIdx),
    )

  def stats(tx: VersionedTransaction): (TransactionNodeStatistic, TransactionNodeStatistic) =
    stats(tx.transaction)

  /** This function produces statistic about the committed nodes (those nodes
    *  that do not appear under a rollback node) on the one hand and
    *  rollbacked nodes (those nodes that do appear under a rollback node) on
    *  the other hand.
    */
  def stats(tx: Transaction): (TransactionNodeStatistic, TransactionNodeStatistic) = {
    val committed = emptyFields
    val rollbacked = emptyFields
    var rollbackDepth = 0

    def incr(fieldIdx: Int) =
      if (rollbackDepth > 0) rollbacked(fieldIdx) += 1 else committed(fieldIdx) += 1

    tx.foreachInExecutionOrder(
      exerciseBegin = { (_, exe) =>
        val idx =
          if (exe.consuming)
            if (exe.byKey)
              consumingExercisesByKeysIdx
            else
              consumingExerciseByCidsIdx
          else if (exe.byKey)
            nonconsumingExerciseByKeysIdx
          else
            nonconsumingExerciseCidsIdx
        incr(idx)
        ChildrenRecursion.DoRecurse
      },
      rollbackBegin = { (_, _) =>
        incr(rollbacksIdx)
        rollbackDepth += 1
        ChildrenRecursion.DoRecurse
      },
      leaf = { (_, node) =>
        val idx = node match {
          case _: Node.Create =>
            createsIdx
          case fetch: Node.Fetch =>
            if (fetch.byKey)
              fetchByKeysIdx
            else
              fetchesIdx
          case _: Node.LookupByKey =>
            lookupByKeysIdx
        }
        incr(idx)
      },
      exerciseEnd = (_, _) => (),
      rollbackEnd = (_, _) => rollbackDepth -= 1,
    )

    (build(committed), build(rollbacked))
  }

}
