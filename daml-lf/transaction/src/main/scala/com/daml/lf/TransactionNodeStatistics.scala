// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref.PackageId

import scala.annotation.nowarn

object TransactionNodeStatistics {

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
    */
  final case class Actions(
      creates: Int,
      consumingExercisesByCid: Int,
      nonconsumingExercisesByCid: Int,
      consumingExercisesByKey: Int,
      nonconsumingExercisesByKey: Int,
      fetchesByCid: Int,
      fetchesByKey: Int,
      lookupsByKey: Int,
  ) {

    def +(that: Actions) =
      Actions(
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
      )

    def exercisesByCid: Int = consumingExercisesByCid + nonconsumingExercisesByCid
    def exercisesByKey: Int = consumingExercisesByKey + nonconsumingExercisesByKey
    def exercises: Int = exercisesByCid + exercisesByKey
    def consumingExercises: Int = consumingExercisesByCid + consumingExercisesByKey
    def nonconsumingExercises: Int = nonconsumingExercisesByCid + nonconsumingExercisesByKey
    def fetches: Int = fetchesByCid + fetchesByKey
    def byKeys: Int = exercisesByKey + fetchesByKey + lookupsByKey
    def actions: Int = creates + exercises + fetches + lookupsByKey
  }

  val EmptyActions: Actions = Actions(0, 0, 0, 0, 0, 0, 0, 0)

  val Empty: TransactionNodeStatistics = TransactionNodeStatistics(EmptyActions, EmptyActions)

  private[this] val numberOfFields = EmptyActions.productArity

  private[this] val Seq(
    createsIdx,
    consumingExercisesByCidIdx,
    nonconsumingExerciseCidsIdx,
    consumingExercisesByKeyIdx,
    nonconsumingExercisesByKeyIdx,
    fetchesIdx,
    fetchesByKeyIdx,
    lookupsByKeyIdx,
  ) =
    (0 until numberOfFields): @nowarn("msg=match may not be exhaustive")

  private[this] def emptyFields = Array.fill(numberOfFields)(0)

  private[this] def build(stats: Array[Int]) =
    Actions(
      creates = stats(createsIdx),
      consumingExercisesByCid = stats(consumingExercisesByCidIdx),
      nonconsumingExercisesByCid = stats(nonconsumingExerciseCidsIdx),
      consumingExercisesByKey = stats(consumingExercisesByKeyIdx),
      nonconsumingExercisesByKey = stats(nonconsumingExercisesByKeyIdx),
      fetchesByCid = stats(fetchesIdx),
      fetchesByKey = stats(fetchesByKeyIdx),
      lookupsByKey = stats(lookupsByKeyIdx),
    )

  /** This function produces statistics about the committed nodes (those nodes
    *  that do not appear under a rollback node) on the one hand and
    *  rolled back nodes (those nodes that do appear under a rollback node) on
    *  the other hand within a given transaction `tx`.
    */
  def apply(
      tx: VersionedTransaction,
      excludedPackages: Set[PackageId] = Set.empty,
  ): TransactionNodeStatistics =
    apply(tx.transaction, excludedPackages)

  /** Calculate the node statistics unless all actions in the transaction use infrastructure packages in
    * which case return Empty.
    */
  def apply(
      tx: Transaction,
      excludedPackages: Set[PackageId],
  ): TransactionNodeStatistics = {
    val excluded = tx.nodes.values
      .collect({ case a: Node.Action => a })
      .forall(_.packageIds.forall(excludedPackages.contains))
    if (!excluded) {
      build(tx)
    } else {
      TransactionNodeStatistics.Empty
    }
  }

  private def build(tx: Transaction): TransactionNodeStatistics = {
    val committed = emptyFields
    val rolledBack = emptyFields
    var rollbackDepth = 0

    def incr(fieldIdx: Int) =
      if (rollbackDepth > 0) rolledBack(fieldIdx) += 1 else committed(fieldIdx) += 1

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
        Transaction.ChildrenRecursion.DoRecurse
      },
      rollbackBegin = { (_, _) =>
        rollbackDepth += 1
        Transaction.ChildrenRecursion.DoRecurse
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

    TransactionNodeStatistics(build(committed), build(rolledBack))
  }

}

final case class TransactionNodeStatistics(
    committed: TransactionNodeStatistics.Actions,
    rolledBack: TransactionNodeStatistics.Actions,
) {
  def +(that: TransactionNodeStatistics) = {
    TransactionNodeStatistics(this.committed + that.committed, this.rolledBack + that.rolledBack)
  }
}
