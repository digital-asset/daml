// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.speedy.Speedy.ContractInfo
import com.digitalasset.daml.lf.transaction.{ContractKeyUniquenessMode, Node, TransactionVersion}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PartialTransactionSpec extends AnyWordSpec with Matchers with Inside {

  private[this] val txVersion = TransactionVersion.maxVersion
  private[this] val contractIdVersion = ContractIdVersion.V2

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("PartialTransactionSpec")
  private[this] val pkgName = data.Ref.PackageName.assertFromString("-package-name-")
  private[this] val templateId = data.Ref.Identifier.assertFromString("pkg:Mod:Template")
  private[this] val choiceId = data.Ref.Name.assertFromString("choice")
  private[this] val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("My contract"))
  private[this] val party = data.Ref.Party.assertFromString("Alice")
  private[this] val committers: Set[data.Ref.Party] = Set(party)

  private[this] val initialState = PartialTransaction.initial(
    ContractKeyUniquenessMode.Strict,
    InitialSeeding.TransactionSeed(transactionSeed),
    committers,
  )

  private[this] def contractIdsInOrder(ptx: PartialTransaction): List[Value.ContractId] = {
    ptx.finish.toOption.get._1
      .fold(List.empty[Value.ContractId]) {
        case (acc, (_, create: Node.Create)) => acc :+ create.coid
        case (acc, _) => acc
      }
      .reverse
  }

  private[this] implicit class PartialTransactionExtra(val ptx: PartialTransaction) {

    val contract = ContractInfo(
      version = txVersion,
      packageName = pkgName,
      templateId = templateId,
      value = SValue.SRecord(templateId, ImmArray(), ArrayList()),
      signatories = Set(party),
      observers = Set.empty,
      keyOpt = None,
    )

    def insertCreate_ : PartialTransaction =
      ptx
        .insertCreate(
          preparationTime = data.Time.Timestamp.Epoch,
          contract = contract,
          optLocation = None,
          contractIdVersion = contractIdVersion,
        )
        .toOption
        .get
        ._2

    def beginExercises_ : PartialTransaction =
      ptx
        .beginExercises(
          packageName = pkgName,
          templateId = contract.templateId,
          targetId = cid,
          contract = contract,
          interfaceId = None,
          choiceId = choiceId,
          optLocation = None,
          consuming = false,
          actingParties = Set(party),
          choiceObservers = Set.empty,
          choiceAuthorizers = None,
          byKey = false,
          chosenValue = Value.ValueUnit,
          version = txVersion,
        )
        .toOption
        .get

    def endExercises_ : PartialTransaction =
      ptx.endExercises(_ => Value.ValueNone)

    def rollbackTry_ : PartialTransaction =
      ptx.rollbackTry()
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
          .rollbackTry_ // the second try context does not handle the exception
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
