// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data._
import com.daml.lf.ledger.Authorize
import com.daml.lf.speedy.PartialTransaction._
import com.daml.lf.speedy.SValue.{SValue => _, _}
import com.daml.lf.transaction.{ContractKeyUniquenessMode, Node, TransactionVersion}
import com.daml.lf.value.Value
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartialTransactionSpec extends AnyWordSpec with Matchers with Inside {

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("PartialTransactionSpec")

  private val templates: Map[TransactionVersion, Ref.Identifier] =
    TransactionVersion.All.view
      .map(v => v -> Ref.Identifier.assertFromString(s"pkg${v.protoValue}:Mod:Template"))
      .toMap
  private[this] val templateId = templates(TransactionVersion.StableVersions.max)
  private[this] val choiceId = Ref.Name.assertFromString("choice")
  private[this] val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("My contract"))
  private[this] val party = Ref.Party.assertFromString("Alice")
  private[this] val committers: Set[Ref.Party] = Set.empty

  private[this] val pkg2TxVersion = templates.map { case (version, id) => id.packageId -> version }

  private[this] val initialState = PartialTransaction.initial(
    pkg2TxVersion,
    ContractKeyUniquenessMode.On,
    Time.Timestamp.Epoch,
    InitialSeeding.TransactionSeed(transactionSeed),
    committers,
  )

  private[this] def contractIdsInOrder(ptx: PartialTransaction): Seq[Value.ContractId] = {
    inside(ptx.finish) { case CompleteTransaction(tx, _, _) =>
      tx.fold(Vector.empty[Value.ContractId]) {
        case (acc, (_, create: Node.Create)) => acc :+ create.coid
        case (acc, _) => acc
      }
    }
  }

  private[this] implicit class PartialTransactionExtra(val ptx: PartialTransaction) {

    def insertCreate_(templateId: Ref.Identifier = templateId): PartialTransaction =
      ptx
        .insertCreate(
          Authorize(Set(party)),
          templateId,
          Value.ValueUnit,
          "agreement",
          None,
          Set(party),
          Set.empty,
          None,
          None,
        )
        ._2

    def beginExercises_(templateId: Ref.Identifier = templateId): PartialTransaction =
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
        byInterface = None,
        chosenValue = Value.ValueUnit,
      )

    def endExercises_ : PartialTransaction =
      ptx.endExercises(SValue.SUnit)

    private val dummyException = SArithmeticError("Dummy", ImmArray.Empty)

    def rollbackTry_ : PartialTransaction =
      ptx.rollbackTry(dummyException)
  }

  private[this] val outputCids =
    contractIdsInOrder(
      initialState //
        .insertCreate_() // create the contract cid_0
        .beginExercises_() // open an exercise context
        .insertCreate_() // create the contract cid_1_0
        .insertCreate_() // create the contract cid_1_2
        .insertCreate_() // create the contract cid_1_3
        .endExercises_ // close the exercise context normally
        .insertCreate_() // create the contract cid_2
    )

  val Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2) = outputCids

  "try context" should {
    "be without effect when closed without exception" in {
      def run1 = contractIdsInOrder(
        initialState //
          .insertCreate_() // create the contract cid_0
          .beginExercises_() // open an exercise context
          .insertCreate_() // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_() // create the contract cid_1_1
          .endTry // close the try context
          .insertCreate_() // create the contract cid_1_2
          .endExercises_ // close the exercise context normally
          .insertCreate_() // create the contract cid_2
      )

      def run2 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialState //
          .insertCreate_() // create the contract cid_0
          .beginTry // open a try context
          .beginExercises_() // open an exercise context
          .insertCreate_() // create the contract cid_1_0
          .insertCreate_() // create the contract cid_1_2
          .insertCreate_() // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .endTry // close the try context
          .insertCreate_() // create the contract cid_2
      )

      run1 shouldBe outputCids
      run2 shouldBe outputCids

    }

    "rollback the current transaction without resetting seed counter for contract IDs" in {
      def run1 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialState //
          .insertCreate_() // create the contract cid_0
          .beginTry // open a first try context
          .beginExercises_() // open an exercise context
          .insertCreate_() // create the contract cid_1_0
          .insertCreate_() // create the contract cid_1_1
          .insertCreate_() // create the contract cid_1_2
          // an exception is thrown
          .abortExercises // close abruptly the exercise due to an uncaught exception
          .rollbackTry_ // the try context handles the exception
          .insertCreate_() // create the contract cid_2
      )

      def run2 = contractIdsInOrder(
        initialState //
          .insertCreate_() // create the contract cid_0
          .beginTry // open a first try context
          .beginExercises_() // open an exercise context
          .insertCreate_() // create the contract cid_1_0
          .insertCreate_() // create the contract cid_1_1
          .beginTry // open a second try context
          .insertCreate_() // create the contract cid_1_2
          // an exception is thrown
          .abortTry // the second try context does not handle the exception
          .abortExercises // close abruptly the exercise due to an uncaught exception
          .rollbackTry_ // the first try context does handle the exception
          .insertCreate_() // create the contract cid_2
      )

      def run3 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialState //
          .insertCreate_() // create the contract cid_0
          .beginExercises_() // open an exercise context
          .insertCreate_() // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_() // create the contract cid_1_2
          .rollbackTry_ // the try  context does handle the exception
          .insertCreate_() // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .insertCreate_() // create the contract cid_2
      )

      run1 shouldBe Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2)
      run2 shouldBe Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2)
      run3 shouldBe Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2)
    }
  }

  "infers version as" should {

    "be the max of the version of the children nodes" in {

      import TransactionVersion._

      def versionsInOrder(ptx: PartialTransaction) =
        inside(ptx.finish) { case CompleteTransaction(tx, _, _) =>
          tx.version -> tx.fold(Vector.empty[Option[TransactionVersion]])(_ :+ _._2.optVersion)
        }

      versionsInOrder(
        initialState
          .insertCreate_(templates(V14))
      ) shouldBe (V14, Seq(Some(V14)))

      versionsInOrder(
        initialState
          .insertCreate_(templates(V12))
          .insertCreate_(templates(V13))
      ) shouldBe (V13, Seq(Some(V12), Some(V13)))

      versionsInOrder(
        initialState
          .beginExercises_(templates(V14))
          .insertCreate_(templates(V12))
          .insertCreate_(templates(V13))
          .endExercises_
      ) shouldBe (V14, Seq(Some(V14), Some(V12), Some(V13)))

      versionsInOrder(
        initialState
          .beginExercises_(templates(V11))
          .insertCreate_(templates(V12))
          .insertCreate_(templates(V13))
          .endExercises_
      ) shouldBe (V13, Seq(Some(V13), Some(V12), Some(V13)))

      versionsInOrder(
        initialState.beginTry
          .insertCreate_(templates(VDev))
          .insertCreate_(templates(V14))
          .endTry
      ) shouldBe (VDev, Seq(Some(VDev), Some(V14)))

      versionsInOrder(
        initialState.beginTry
          .insertCreate_(templates(V14))
          .insertCreate_(templates(VDev))
          .rollbackTry_
      ) shouldBe (VDev, Seq(None, Some(V14), Some(VDev)))

    }
  }

}
