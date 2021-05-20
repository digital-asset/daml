// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.ledger.Authorize
import com.daml.lf.speedy.PartialTransaction._
import com.daml.lf.speedy.SValue._
import com.daml.lf.transaction.{ContractKeyUniquenessMode, Node, TransactionVersion}
import com.daml.lf.value.Value
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartialTransactionSpec extends AnyWordSpec with Matchers with Inside {

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("PartialTransactionSpec")
  private[this] val templateId = data.Ref.Identifier.assertFromString("pkg:Mod:Template")
  private[this] val choiceId = data.Ref.Name.assertFromString("choice")
  private[this] val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("My contract"))
  private[this] val party = data.Ref.Party.assertFromString("Alice")

  private[this] val initialState = PartialTransaction.initial(
    _ => TransactionVersion.maxVersion,
    ContractKeyUniquenessMode.On,
    data.Time.Timestamp.Epoch,
    InitialSeeding.TransactionSeed(transactionSeed),
  )

  private[this] def contractIdsInOrder(ptx: PartialTransaction): Seq[Value.ContractId] = {
    inside(ptx.finish) { case CompleteTransaction(tx) =>
      tx.fold(Vector.empty[Value.ContractId]) {
        case (acc, (_, create: Node.NodeCreate[Value.ContractId])) => acc :+ create.coid
        case (acc, _) => acc
      }
    }
  }

  private[this] implicit class PartialTransactionExtra(val ptx: PartialTransaction) {

    def insertCreate_ : PartialTransaction =
      ptx.insertCreate(
        Authorize(Set(party)),
        templateId,
        Value.ValueUnit,
        "agreement",
        None,
        Set(party),
        Set.empty,
        None,
      ) match {
        case Right((_, newPtx)) => newPtx
        case Left(_) => sys.error("unexpected error")
      }

    def beginExercises_ : PartialTransaction =
      ptx.beginExercises(
        Authorize(Set(party)),
        targetId = cid,
        templateId = templateId,
        choiceId = choiceId,
        optLocation = None,
        consuming = false,
        actingParties = Set(party),
        signatories = Set(party),
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

    def endExercises_ : PartialTransaction =
      ptx.endExercises(Value.ValueUnit)

    private val dummyException = SBuiltinException(ArithmeticError, "Dummy", ImmArray.empty)

    def rollbackTry_ : PartialTransaction =
      ptx.rollbackTry(dummyException)
  }

  private[this] val outputCids =
    contractIdsInOrder(
      initialState //
        .insertCreate_ // create the contract cid_0
        .beginExercises_ // open an exercise context
        .insertCreate_ // create the contract cid_1_0
        .insertCreate_ // create the contract cid_1_2
        .insertCreate_ // create the contract cid_1_3
        .endExercises_ // close the exercise context normally
        .insertCreate_ // create the contract cid_2
    )

  val Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2) = outputCids

  "try context" should {
    "be without effect when closed without exception" in {
      def run1 = contractIdsInOrder(
        initialState //
          .insertCreate_ // create the contract cid_0
          .beginExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_ // create the contract cid_1_1
          .endTry // close the try context
          .insertCreate_ // create the contract cid_1_2
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      def run2 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialState //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a try context
          .beginExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_2
          .insertCreate_ // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .endTry // close the try context
          .insertCreate_ // create the contract cid_2
      )

      run1 shouldBe outputCids
      run2 shouldBe outputCids

    }

    "rollback the current transaction without resetting seed counter for contract IDs" in {
      def run1 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialState //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a first try context
          .beginExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_1
          .insertCreate_ // create the contract cid_1_2
          // an exception is thrown
          .abortExercises // close abruptly the exercise due to an uncaught exception
          .rollbackTry_ // the try context handles the exception
          .insertCreate_ // create the contract cid_2
      )

      def run2 = contractIdsInOrder(
        initialState //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a first try context
          .beginExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_1
          .beginTry // open a second try context
          .insertCreate_ // create the contract cid_1_2
          // an exception is thrown
          .abortTry // the second try context does not handle the exception
          .abortExercises // close abruptly the exercise due to an uncaught exception
          .rollbackTry_ // the first try context does handle the exception
          .insertCreate_ // create the contract cid_2
      )

      def run3 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialState //
          .insertCreate_ // create the contract cid_0
          .beginExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_ // create the contract cid_1_2
          .rollbackTry_ // the try  context does handle the exception
          .insertCreate_ // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      run1 shouldBe Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2)
      run2 shouldBe Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2)
      run3 shouldBe Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2)
    }
  }

}
