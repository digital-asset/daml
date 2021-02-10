// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.language.{Util => AstUtil}
import com.daml.lf.speedy.PartialTransaction._
import com.daml.lf.transaction.{Node, TransactionVersion}
import com.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartialTransactionSpec extends AnyWordSpec with Matchers {

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("PartialTransactionSpec")
  private[this] val templateId = data.Ref.Identifier.assertFromString("pkg:Mod:Template")
  private[this] val choiceId = data.Ref.Name.assertFromString("choice")
  private[this] val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("My contract"))

  private[this] val initialState = PartialTransaction.initial(
    _ => TransactionVersion.maxVersion,
    data.Time.Timestamp.Epoch,
    InitialSeeding.TransactionSeed(transactionSeed),
  )

  private[this] def contractIdsInOrder(ptx: PartialTransaction): Seq[Value.ContractId] = {
    ptx.finish match {
      case CompleteTransaction(tx) =>
        tx.fold(Vector.empty[Value.ContractId]) {
          case (acc, (_, create: Node.NodeCreate[Value.ContractId])) => acc :+ create.coid
          case (acc, _) => acc
        }
      case IncompleteTransaction(_) =>
        sys.error("unexpected error")
    }
  }

  private[this] implicit class PartialTransactionExtra(val ptx: PartialTransaction) {
    def create: PartialTransaction =
      ptx.insertCreate(
        None,
        templateId,
        Value.ValueUnit,
        "agreement",
        None,
        Set.empty,
        Set.empty,
        None,
      ) match {
        case Right((_, newPtx)) => newPtx
        case Left(_) => sys.error("unexpected error")
      }

    def beginExercise: PartialTransaction =
      ptx.beginExercises(
        auth = None,
        targetId = cid,
        templateId = templateId,
        choiceId = choiceId,
        optLocation = None,
        consuming = false,
        actingParties = Set.empty,
        signatories = Set.empty,
        stakeholders = Set.empty,
        choiceObservers = Set.empty,
        mbKey = None,
        byKey = false,
        chosenValue = Value.ValueUnit,
      ) match {
        case Right(value) => value
        case Left(_) =>
          sys.error("unexpected failing beginExercises")
      }
  }

  private[this] val outputCids =
    contractIdsInOrder(
      initialState //
        .create // create cid_0
        .beginExercise //
        .create // create cid_1_0
        .create // create cid_1_2
        .create // create cid_1_3
        .endExercises(Value.ValueUnit)
        .create // create cid_2
    )

  val Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2) = outputCids

  "try context" should {
    "be without effect when closed without exception" in {
      def run1 = contractIdsInOrder(
        initialState //
          .create //
          .beginExercise //
          .create //
          .beginTry //
          .create //
          .endTry //
          .create //
          .endExercises(Value.ValueUnit) //
          .create //
      )

      def run2 = contractIdsInOrder(
        initialState //
          .create //
          .beginTry //
          .beginExercise //
          .create //
          .create //
          .create //
          .endExercises(Value.ValueUnit) //
          .endTry //
          .create //
      )

      run1 shouldBe outputCids
      run2 shouldBe outputCids

    }

    "rollback the current transaction without resetting seed counter for contract IDs" in {
      def run1 = contractIdsInOrder(
        initialState //
          .create //
          .beginTry //
          .beginExercise //
          .create //
          .create //
          .create //
          .endExercises(Value.ValueUnit) //
          .catchAndRollback(AstUtil.TUnit, Value.ValueUnit) //
          .create //
      )

      def run2 = contractIdsInOrder(
        initialState //
          .create //
          .beginTry //
          .beginExercise //
          .create //
          .create //
          .create //
          .abortExercises //
          .catchAndRollback(AstUtil.TUnit, Value.ValueUnit) //
          .create //
      )

      def run3 = contractIdsInOrder(
        initialState //
          .create //
          .beginExercise //
          .create //
          .beginTry //
          .create //
          .catchAndRollback(AstUtil.TUnit, Value.ValueUnit) //
          .create //
          .abortExercises //
          .create //
      )

      run1 shouldBe Seq(cid_0, cid_2)
      run2 shouldBe Seq(cid_0, cid_2)
      run3 shouldBe Seq(cid_0, cid_1_0, cid_1_2, cid_2)
    }
  }

}
