// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey
import com.daml.lf.transaction.test.TestNodeBuilder.CreateKey.NoKey
import com.daml.lf.transaction.TransactionNodeStatistics.Actions
import com.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestIdFactory, TestNodeBuilder}
import com.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class TransactionNodeStatisticsSpec
    extends AnyWordSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks
    with TestIdFactory {

  import TransactionNodeStatisticsSpec.TxBuilder

  "TransactionNodeStatistics.Actions#+" should {

    "add" in {
      val s1 = Actions(1, 1, 1, 1, 1, 1, 1, 1)
      val s2 = Actions(2, 3, 5, 7, 11, 13, 17, 19)
      val s1s2 = Actions(3, 4, 6, 8, 12, 14, 18, 20)
      val s2s2 = Actions(4, 6, 10, 14, 22, 26, 34, 38)

      s2 + TransactionNodeStatistics.EmptyActions shouldBe s2
      TransactionNodeStatistics.EmptyActions + s2 shouldBe s2
      s1 + s2 shouldBe s1s2
      s2 + s1 shouldBe s1s2
      s2 + s2 shouldBe s2s2
    }
  }

  "TransactionNodeStatistics#+" should {

    "add" in {
      val d1c = Actions(1, 1, 1, 1, 1, 1, 1, 1)
      val d1r = Actions(2, 3, 5, 7, 11, 13, 17, 19)
      val d2c = Actions(3, 5, 7, 11, 13, 17, 19, 23)
      val d2r = Actions(5, 7, 11, 13, 17, 19, 23, 29)

      val s1 = TransactionNodeStatistics(d1c, d1r)
      val s2 = TransactionNodeStatistics(d2c, d2r)
      val expected = TransactionNodeStatistics(d1c + d2c, d1r + d2r)
      s1 + s2 shouldBe expected
    }
  }

  "TransactionNodeStatistics" should {

    def create(b: TxBuilder, withKey: Boolean = false) = {
      val parties = Set(b.newParty)
      b.create(
        id = b.newCid,
        templateId = b.newIdentifier,
        argument = Value.ValueUnit,
        signatories = parties,
        observers = Set.empty,
        key = if (withKey) CreateKey.SignatoryMaintainerKey(Value.ValueUnit) else NoKey,
      )
    }

    def exe(consuming: Boolean, byKey: Boolean)(b: TxBuilder) = {
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
      )
    }

    def fetch(byKey: Boolean)(b: TxBuilder) =
      b.fetch(create(b, byKey), byKey)

    def lookup(b: TxBuilder) =
      b.lookupByKey(create(b, withKey = true), true)

    def rollback(b: TxBuilder) =
      b.rollback()

    def addAllNodes(b: TxBuilder, rbId: NodeId) = {
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

    val testIterations = 3

    type Getter = Actions => Int

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
    )

    "count each type of committed nodes properly" in {
      forEvery(testCases) { (makeNode, getter) =>
        val builder = new TxBuilder()
        for (i <- 1 to testIterations) {
          builder.add(makeNode(builder))
          inside(TransactionNodeStatistics(builder.build())) {
            case TransactionNodeStatistics(committed, rolledBack) =>
              getter(committed) shouldBe i
              committed.actions shouldBe i
              rolledBack shouldBe TransactionNodeStatistics.EmptyActions
          }
        }
      }
    }

    "count each type of rolled back nodes properly" in {
      forEvery(testCases) { case (makeNode, getter) =>
        val builder = new TxBuilder()
        val rollbackId = builder.add(builder.rollback())

        for (i <- 1 to testIterations) {
          builder.add(makeNode(builder), rollbackId)
          inside(TransactionNodeStatistics.apply(builder.build())) {
            case TransactionNodeStatistics(committed, rolledBack) =>
              committed shouldBe Actions(0, 0, 0, 0, 0, 0, 0, 0)
              getter(rolledBack) shouldBe i
              rolledBack.actions shouldBe i
          }
        }
      }
    }

    "count all committed nodes properly" in {
      val b = new TxBuilder()
      var exeId = b.add(exe(consuming = false, byKey = false)(b)) // one nonconsumming exercises

      for (i <- 1 to testIterations) {
        addAllNodes(b, exeId) // one additional nodes of each types
        inside(TransactionNodeStatistics.apply(b.build())) {
          case TransactionNodeStatistics(committed, rolledBack) =>
            // There are twice more nonconsumming exercises by cid are double because
            // we use a extra one to nest the other node of the next loop
            committed shouldBe Actions(i, i, 2 * i, i, i, i, i, i)
            rolledBack shouldBe TransactionNodeStatistics.EmptyActions
        }
        exeId =
          b.add(exe(consuming = false, byKey = false)(b), exeId) // one nonconsumming exercises
      }
    }

    "count all rolled back nodes properly" in {
      val b = new TxBuilder()
      var rbId = b.add(rollback(b)) // a committed rollback node

      for (i <- 1 to testIterations) {
        rbId = b.add(rollback(b), rbId) // one additional rolled Back rollback node
        addAllNodes(b, rbId) // one additional rolled Back nodes of each type
        inside(TransactionNodeStatistics.apply(b.build())) {
          case TransactionNodeStatistics(committed, rolledBack) =>
            committed shouldBe Actions(0, 0, 0, 0, 0, 0, 0, 0)
            // There are twice more rollback nodes, since we use an extra one to
            // nest the other nodes in each loop
            rolledBack shouldBe Actions(i, i, i, i, i, i, i, i)
        }
      }
    }

    "exclude infrastructure transactions" in {
      forEvery(testCases) { (makeNode, _) =>
        val builder = new TxBuilder()
        val node = makeNode(builder)
        val excludedPackageIds = Set(node).collect({ case a: Node.Action => a.packageIds }).flatten
        builder.add(node)
        TransactionNodeStatistics(
          builder.build(),
          excludedPackageIds,
        ) shouldBe TransactionNodeStatistics.Empty
      }
    }

    "only exclude transaction if all packages are infrastructure" in {
      forEvery(testCases) { (makeNode, _) =>
        val builder = new TxBuilder()
        val nonExcludedNode = makeNode(builder)
        val excludedNode = makeNode(builder)
        val excludedPackageIds =
          Set(excludedNode).collect({ case a: Node.Action => a.packageIds }).flatten
        builder.add(nonExcludedNode)
        builder.add(excludedNode)
        TransactionNodeStatistics(
          builder.build(),
          excludedPackageIds,
        ) should not be (TransactionNodeStatistics.Empty)
      }
    }
  }

}

object TransactionNodeStatisticsSpec {
  class TxBuilder extends NodeIdTransactionBuilder with TestNodeBuilder
}
