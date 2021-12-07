// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.transaction.Transaction.ChildrenRecursion

/** Container for transaction statistics.
  *
  * @param creates number of creates nodes,
  * @param consumingExercisesByCid number of consuming exercises by contract ID nodes,
  * @param nonconsumingExercisesByCid number of non-consuming Exercises by contract ID nodes,
  * @param consumingExercisesByKey number of consuming exercise by contract key nodes,
  * @param nonconsumingExercisesByKey number of non-consuming exercise by key nodes,
  * @param fetchesByCid number of fetch by contract ID nodes,
  * @param fetchesByKey number of fetch by key nodes,
  * @param lookupsByKey number of lookup by key nodes,
  * @param rollbacks number of rollback nodes.
  */
final case class TransactionNodeStatistics(
    creates: Int,
    consumingExercisesByCid: Int,
    nonconsumingExercisesByCid: Int,
    consumingExercisesByKey: Int,
    nonconsumingExercisesByKey: Int,
    fetchesByCid: Int,
    fetchesByKey: Int,
    lookupsByKey: Int,
    rollbacks: Int,
) {

  def +(that: TransactionNodeStatistics) =
    TransactionNodeStatistics(
      creates = this.creates + that.creates,
      consumingExercisesByCid = this.consumingExercisesByCid + that.consumingExercisesByCid,
      nonconsumingExercisesByCid =
        this.nonconsumingExercisesByCid + that.nonconsumingExercisesByCid,
      consumingExercisesByKey = this.consumingExercisesByKey + that.consumingExercisesByKey,
      nonconsumingExercisesByKey =
        this.nonconsumingExercisesByKey + that.nonconsumingExercisesByKey,
      fetchesByCid = this.fetchesByCid + that.fetchesByCid,
      fetchesByKey = this.fetchesByKey + that.fetchesByKey,
      lookupsByKey = this.lookupsByKey + that.lookupsByKey,
      rollbacks = this.rollbacks + that.rollbacks,
    )

  def exercisesByCid: Int = consumingExercisesByCid + nonconsumingExercisesByCid
  def exercisesByKey: Int = consumingExercisesByKey + nonconsumingExercisesByKey
  def exercises: Int = exercisesByCid + exercisesByKey
  def consumingExercises: Int = consumingExercisesByCid + consumingExercisesByKey
  def nonconsumingExercises: Int = nonconsumingExercisesByCid + nonconsumingExercisesByKey
  def fetches: Int = fetchesByCid + fetchesByKey
  def byKeys: Int = exercisesByKey + fetchesByKey + lookupsByKey
  def actions: Int = creates + exercises + fetches + lookupsByKey
  def nodes: Int = actions + rollbacks
}

object TransactionNodeStatistics {

  val Empty = TransactionNodeStatistics(0, 0, 0, 0, 0, 0, 0, 0, 0)

  private[this] val numberOfFields = Empty.productArity

  private[this] val Seq(
    createsIdx,
    consumingExercisesByCidIdx,
    nonconsumingExerciseCidsIdx,
    consumingExercisesByKeyIdx,
    nonconsumingExercisesByKeyIdx,
    fetchesIdx,
    fetchesByKeyIdx,
    lookupsByKeyIdx,
    rollbacksIdx,
  ) =
    (0 until numberOfFields)

  private[this] def emptyFields = Array.fill(numberOfFields)(0)

  private[this] def build(stats: Array[Int]) =
    TransactionNodeStatistics(
      creates = stats(createsIdx),
      consumingExercisesByCid = stats(consumingExercisesByCidIdx),
      nonconsumingExercisesByCid = stats(nonconsumingExerciseCidsIdx),
      consumingExercisesByKey = stats(consumingExercisesByKeyIdx),
      nonconsumingExercisesByKey = stats(nonconsumingExercisesByKeyIdx),
      fetchesByCid = stats(fetchesIdx),
      fetchesByKey = stats(fetchesByKeyIdx),
      lookupsByKey = stats(lookupsByKeyIdx),
      rollbacks = stats(rollbacksIdx),
    )

  def stats(tx: VersionedTransaction): (TransactionNodeStatistics, TransactionNodeStatistics) =
    stats(tx.transaction)

  /** This function produces statistic about the committed nodes (those nodes
    *  that do not appear under a rollback node) on the one hand and
    *  rollbacked nodes (those nodes that do appear under a rollback node) on
    *  the other hand.
    */
  def stats(tx: Transaction): (TransactionNodeStatistics, TransactionNodeStatistics) = {
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
              consumingExercisesByKeyIdx
            else
              consumingExercisesByCidIdx
          else if (exe.byKey)
            nonconsumingExercisesByKeyIdx
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
              fetchesByKeyIdx
            else
              fetchesIdx
          case _: Node.LookupByKey =>
            lookupsByKeyIdx
        }
        incr(idx)
      },
      exerciseEnd = (_, _) => (),
      rollbackEnd = (_, _) => rollbackDepth -= 1,
    )

    (build(committed), build(rollbacked))
  }

}
