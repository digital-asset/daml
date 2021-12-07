// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TransactionStatisticsSpec
    extends AnyWordSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  private[this] def create(b: TxBuilder, withKey: Boolean = false) = {
    val parties = Set(b.newParty)
    b.create(
      id = b.newCid,
      templateId = b.newIdenfier,
      argument = Value.ValueUnit,
      signatories = parties,
      observers = Set.empty,
      key = if (withKey) Some(Value.ValueUnit) else None,
      maintainers = if (withKey) parties else Set.empty,
      byInterface = None,
    )
  }

  private[this] def exe(consuming: Boolean, byKey: Boolean)(b: TxBuilder) = {
    val c = create(b, byKey)
    b.exercise(
      contract = c,
      choice = b.newChoiceName,
      consuming = consuming,
      actingParties = c.signatories,
      argument = Value.ValueUnit,
      result = Some(Value.ValueUnit),
      choiceObservers = Set.empty,
      byKey = byKey,
      byInterface = None,
    )
  }

  private[this] def fetch(byKey: Boolean)(b: TxBuilder) =
    b.fetch(create(b, byKey), byKey, None)

  private[this] def lookup(b: TxBuilder) =
    b.lookupByKey(create(b, withKey = true), true)

  private[this] def rollback(b: TxBuilder) =
    b.rollback()

  private[this] def addAllNodes(b: TxBuilder, rbId: NodeId) = {
    b.add(create(b), rbId)
    b.add(exe(consuming = false, byKey = false)(b), rbId)
    b.add(exe(consuming = true, byKey = false)(b), rbId)
    b.add(exe(consuming = false, byKey = true)(b), rbId)
    b.add(exe(consuming = true, byKey = true)(b), rbId)
    b.add(fetch(byKey = false)(b), rbId)
    b.add(fetch(byKey = true)(b), rbId)
    b.add(lookup(b), rbId)
    b.add(rollback(b), rbId)
    ()
  }

  private[this] def repeat(n: Int, f: TxBuilder => Unit)(b: TxBuilder) =
    (0 until n).foreach(_ => f(b))

  "TransactionStats.stats" should {

    val n = 3

    type Getter = TransactionNodeStatistics => Int
    val testCases = Table[TxBuilder => Node, Getter](
      "makeNode" -> "getter",
      (create(_), _.creates),
      (exe(consuming = true, byKey = false), _.consumingExercisesByCid),
      (exe(consuming = false, byKey = false), _.nonconsumingExercisesByCid),
      (exe(consuming = true, byKey = true), _.consumingExercisesByKey),
      (exe(consuming = false, byKey = true), _.nonconsumingExercisesByKey),
      (fetch(byKey = false), _.fetchesByCid),
      (fetch(byKey = true), _.fetchesByKey),
      (lookup, _.lookupsByKey),
      (rollback, _.rollbacks),
    )

    "count each type of committed nodes properly" in {
      forEvery(testCases) { (makeNode, getter) =>
        val builder = TxBuilder()

        for (i <- 1 to n) {
          builder.add(makeNode(builder))
          inside(TransactionNodeStatistics.stats(builder.build())) { case (committed, rollbacked) =>
            getter(committed) shouldBe i
            committed.nodes shouldBe i
            rollbacked shouldBe TransactionNodeStatistics.Empty
          }
        }
      }
    }

    "count each type of rollbacked nodes properly" in {
      forEvery(testCases) { case (makeNode, getter) =>
        val builder = TxBuilder()
        val rollbackId = builder.add(builder.rollback())

        for (i <- 1 to n) {
          builder.add(makeNode(builder), rollbackId)
          inside(TransactionNodeStatistics.stats(builder.build())) { case (committed, rollbacked) =>
            committed shouldBe TransactionNodeStatistics(0, 0, 0, 0, 0, 0, 0, 0, rollbacks = 1)
            getter(rollbacked) shouldBe i
            rollbacked.nodes shouldBe i
          }
        }
      }
    }

    "count all committed nodes properly" in {
      val b = TxBuilder()
      var exeId = b.add(exe(false, false)(b)) // one nonconsumming exercises

      for (i <- 1 to n) {
        addAllNodes(b, exeId) // one additional nodes of each types
        inside(TransactionNodeStatistics.stats(b.build())) { case (committed, rollbacked) =>
          // There are twice more nonconsumming exercises by cid are double because
          // we use a extra on to nest the node of the next loop
          committed shouldBe TransactionNodeStatistics(i, i, 2 * i, i, i, i, i, i, i)
          rollbacked shouldBe TransactionNodeStatistics.Empty
        }
        exeId = b.add(exe(false, false)(b), exeId) // one nonconsumming exercises
      }
    }

    "count all rollbacked nodes properly" in {
      val b = TxBuilder()
      var rbId = b.add(rollback(b)) // a "committed" rollback node

      for (i <- 1 to n) {
        addAllNodes(b, rbId) // one additional "rollbacked" nodes of each type
        rbId = b.add(rollback(b), rbId) // one additional "rollbacked" rollback node
        inside(TransactionNodeStatistics.stats(b.build())) { case (committed, rollbacked) =>
          committed shouldBe TransactionNodeStatistics(0, 0, 0, 0, 0, 0, 0, 0, 1)
          // There are twice more rollback nodes, since we use an extra one to
          // nest the node of the loop
          rollbacked shouldBe TransactionNodeStatistics(i, i, i, i, i, i, i, i, 2 * i)
        }
      }
    }
  }

}
