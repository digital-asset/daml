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

class TransactionStatsSpec
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

    type Getter = TransactionNodeStatistic => Int
    val testCases = Table[TxBuilder => Node, Getter](
      "makeNode" -> "getter",
      (create(_), _.creates),
      (exe(consuming = true, byKey = false), _.consumingExerciseByCids),
      (exe(consuming = false, byKey = false), _.nonconsumingExerciseByCids),
      (exe(consuming = true, byKey = true), _.consumingExerciseByKeys),
      (exe(consuming = false, byKey = true), _.nonconsumingExerciseByKeys),
      (fetch(byKey = false), _.fetcheByCids),
      (fetch(byKey = true), _.fetchByKeys),
      (lookup, _.lookupByKeys),
      (rollback, _.rollbacks),
    )

    "count each type of committed nodes properly" in {
      forEvery(testCases) { (makeNode, getter) =>
        val builder = TxBuilder()

        for (i <- 1 to n) {
          builder.add(makeNode(builder))
          inside(TransactionNodeStatistic.stats(builder.build())) { case (committed, rollbacked) =>
            getter(committed) shouldBe i
            committed.nodes shouldBe i
            rollbacked shouldBe TransactionNodeStatistic.Empty
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
          inside(TransactionNodeStatistic.stats(builder.build())) { case (committed, rollbacked) =>
            committed shouldBe TransactionNodeStatistic(0, 0, 0, 0, 0, 0, 0, 0, rollbacks = 1)
            getter(rollbacked) shouldBe i
            rollbacked.nodes shouldBe i
          }
        }
      }
    }

    "count all committed nodes properly" in {
      val b = TxBuilder()
      var exeId = b.add(exe(false, false)(b))

      for (i <- 1 to n) {
        addAllNodes(b, exeId)
        inside(TransactionNodeStatistic.stats(b.build())) { case (committed, rollbacked) =>
          committed shouldBe TransactionNodeStatistic(i, i, 2 * i, i, i, i, i, i, i)
          rollbacked shouldBe TransactionNodeStatistic.Empty
        }
        exeId = b.add(exe(false, false)(b), exeId)
      }
    }

    "count all rollbacked nodes properly" in {
      val b = TxBuilder()
      var rbId = b.add(rollback(b))

      for (i <- 1 to n) {
        addAllNodes(b, rbId)
        val tx = b.build()
        inside(TransactionNodeStatistic.stats(tx)) { case (committed, rollbacked) =>
          committed shouldBe TransactionNodeStatistic(0, 0, 0, 0, 0, 0, 0, 0, 1)
          rollbacked shouldBe TransactionNodeStatistic(i, i, i, i, i, i, i, i, 2 * i - 1)
        }
        rbId = b.add(rollback(b), rbId)
      }
    }
  }

}
