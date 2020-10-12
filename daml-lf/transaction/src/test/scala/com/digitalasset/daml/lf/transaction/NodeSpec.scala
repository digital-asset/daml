// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.data.Ref.Location
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{Value, ValueVersion}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import scalaz.Equal

class NodeSpec extends WordSpec with Matchers with TableDrivenPropertyChecks {

  type Val = VersionedValue[ContractId]

  val v6 = ValueVersion("6")
  val v7 = ValueVersion("7")

  val txBuilder = TransactionBuilder()

  val someContractId = txBuilder.newCid
  val anotherContractId = txBuilder.newCid
  assert(!implicitly[Equal[ContractId]].equal(someContractId, anotherContractId))

  val alice = Ref.Party.assertFromString("Alice")
  val bob = Ref.Party.assertFromString("Bob")
  val oscar = Ref.Party.assertFromString("Oscar")
  val eve = Ref.Party.assertFromString("Eve")

  val pkgId = Ref.PackageId.assertFromString("-pkgId-")
  val eitherTyCon = Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString("Mod:Either"))
  val leftName = Ref.Name.assertFromString("Left")
  val rightName = Ref.Name.assertFromString("Right")

  def left(v: Value[ContractId]) =
    Value.ValueVariant(Some(eitherTyCon), leftName, v)

  def right(v: Value[ContractId]) =
    Value.ValueVariant(Some(eitherTyCon), rightName, v)

  val TemplateTyCon = Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString("Mod:Template"))
  val AnotherTemplateTyCon =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString("Mod:AnotherTemplate"))

  val someFieldName = Ref.Name.assertFromString("field")
  val anotherFieldName = Ref.Name.assertFromString("anotherField")
  def templatePayload(v1: Value[ContractId]) =
    Value.ValueRecord(Some(TemplateTyCon), ImmArray(Some(someFieldName) -> v1))
  def contract(payload: VersionedValue[ContractId]) =
    Value.ContractInst(TemplateTyCon, payload, agreementText = "agreement")

  val KeyTyCon = Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString("Mod:TemplateKey"))
  val someKey = VersionedValue(
    v6,
    Value.ValueRecord(
      Some(KeyTyCon),
      ImmArray(
        Some(Ref.Name.assertFromString("party1")) -> Value.ValueParty(alice),
        Some(Ref.Name.assertFromString("party2")) -> Value.ValueParty(bob),
      )
    )
  )
  val anotherKey = VersionedValue(
    v6,
    Value.ValueRecord(
      Some(KeyTyCon),
      ImmArray(
        Some(Ref.Name.assertFromString("party1")) -> Value.ValueParty(alice),
        Some(Ref.Name.assertFromString("party2")) -> Value.ValueParty(alice),
      )
    )
  )

  val someChoiceName = Ref.Name.assertFromString("aChoiceName")
  val anotherChoiceName = Ref.Name.assertFromString("anotherChoiceName")

  val someLocation =
    Location(pkgId, Ref.ModuleName.assertFromString("Mode"), "someDef", (0, 0), (2, 10))
  val anotherLocation =
    Location(pkgId, Ref.ModuleName.assertFromString("Mode"), "someDef", (3, 0), (5, 10))

  val someValue = VersionedValue(v6, left(Value.ValueUnit))
  val anotherValue = VersionedValue(v6, right(Value.ValueTrue))

  "Node.isReplayedBy" should {

    val someKeyWithM = KeyWithMaintainers(someKey, Set(oscar))

    val create = NodeCreate[ContractId, Val](
      coid = someContractId,
      coinst = contract(someValue),
      optLocation = Some(someLocation),
      signatories = Set(alice),
      stakeholders = Set(bob),
      key = Some(someKeyWithM),
    )

    val fetch = NodeFetch[ContractId, Val](
      coid = someContractId,
      templateId = TemplateTyCon,
      optLocation = Some(someLocation),
      actingParties = Some(Set(alice)),
      signatories = Set(alice),
      stakeholders = Set(bob),
      key = Some(someKeyWithM),
    )

    val exercise = NodeExercises[Nothing, ContractId, Val](
      targetCoid = someContractId,
      templateId = TemplateTyCon,
      choiceId = someChoiceName,
      optLocation = Some(someLocation),
      consuming = true,
      actingParties = Set(alice),
      chosenValue = someValue,
      stakeholders = Set(bob),
      signatories = Set(eve),
      controllersDifferFromActors = false,
      children = ImmArray.empty,
      exerciseResult = Some(someValue),
      key = Some(someKeyWithM),
    )

    val lookup = NodeLookupByKey[ContractId, Val](
      templateId = TemplateTyCon,
      optLocation = Some(someLocation),
      key = someKeyWithM,
      result = Some(someContractId)
    )

    val aliceAndBob = Set(alice, bob)

    "reject invalid replay of a create node" in {

      val rightTestCases =
        Table(
          "create node",
          fetch,
          exercise,
          lookup,
          create.copy(coid = anotherContractId),
          create.copy(coinst = create.coinst.copy(template = eitherTyCon)),
          create.copy(coinst = create.coinst.copy(arg = create.coinst.arg.copy(version = v7))),
          create.copy(coinst = create.coinst.copy(arg = anotherValue)),
          create.copy(coinst = create.coinst.copy(agreementText = "disagremenent")),
          create.copy(signatories = aliceAndBob),
          create.copy(stakeholders = aliceAndBob),
          create.copy(key = None),
          create.copy(key = create.key.map(_.copy(key = anotherKey))),
          create.copy(key = create.key.map(k => k.copy(key = k.key.copy(version = v7)))),
          create.copy(key = create.key.map(_.copy(maintainers = aliceAndBob)))
        )

      val leftTestCases =
        Table(
          "recorded node",
          exercise.copy(key = None)
        )

      forEvery(rightTestCases)(Node.isReplayedBy(create, _) shouldBe false)
      forEvery(leftTestCases)(Node.isReplayedBy(_, create) shouldBe false)

    }

    "accept valid replay of a create node" in {

      val leftTestCases =
        Table(
          "replayed node",
          create,
          create.copy(optLocation = None),
          create.copy(optLocation = Some(anotherLocation))
        )

      val rightTestCases =
        Table(
          "recorded node",
          create.copy(optLocation = None),
        )

      forEvery(leftTestCases)(Node.isReplayedBy(create, _) shouldBe true)
      forEvery(rightTestCases)(Node.isReplayedBy(_, create) shouldBe true)

    }

    "reject an invalid replay of a fetch node" in {

      val rightTestCases =
        Table(
          "replayed node",
          create,
          exercise,
          lookup,
          fetch.copy(coid = anotherContractId),
          fetch.copy(templateId = AnotherTemplateTyCon),
          fetch.copy(actingParties = None),
          fetch.copy(actingParties = Some(aliceAndBob)),
          fetch.copy(signatories = aliceAndBob),
          fetch.copy(stakeholders = aliceAndBob),
          fetch.copy(key = None),
          fetch.copy(key = fetch.key.map(_.copy(key = anotherKey))),
          fetch.copy(key = fetch.key.map(k => k.copy(key = k.key.copy(version = v7)))),
          fetch.copy(key = fetch.key.map(_.copy(maintainers = aliceAndBob)))
        )

      forEvery(rightTestCases)(Node.isReplayedBy(fetch, _) shouldBe false)

    }

    "accept valid replay of a fetch node" in {

      val rightTestCases =
        Table(
          "replayed node",
          fetch,
          fetch.copy(optLocation = None),
          fetch.copy(optLocation = Some(anotherLocation)),
        )

      val leftTestCases =
        Table(
          "recorded node",
          fetch.copy(optLocation = None),
          fetch.copy(actingParties = None),
          fetch.copy(key = None)
        )

      forEvery(rightTestCases)(Node.isReplayedBy(fetch, _) shouldBe true)
      forEvery(leftTestCases)(Node.isReplayedBy(_, fetch) shouldBe true)

    }

    "reject an invalid replay of a exercise node" in {

      val rightTestCases =
        Table(
          "replayed node",
          create,
          fetch,
          lookup,
          exercise.copy(targetCoid = anotherContractId),
          exercise.copy(templateId = AnotherTemplateTyCon),
          exercise.copy(choiceId = anotherChoiceName),
          exercise.copy(consuming = !exercise.consuming),
          exercise.copy(actingParties = aliceAndBob),
          exercise.copy(chosenValue = anotherValue),
          exercise.copy(chosenValue = exercise.chosenValue.copy(version = v7)),
          exercise.copy(stakeholders = aliceAndBob),
          exercise.copy(controllersDifferFromActors = !exercise.controllersDifferFromActors),
          exercise.copy(exerciseResult = None),
          exercise.copy(exerciseResult = exercise.exerciseResult.map(_.copy(version = v7))),
          exercise.copy(exerciseResult = Some(anotherValue)),
          exercise.copy(key = None),
          exercise.copy(key = exercise.key.map(_.copy(key = anotherKey))),
          exercise.copy(key = exercise.key.map(k => k.copy(key = k.key.copy(version = v7)))),
          exercise.copy(key = exercise.key.map(_.copy(maintainers = aliceAndBob)))
        )

      forEvery(rightTestCases)(Node.isReplayedBy(exercise, _) shouldBe false)
    }

    "accept valid replay of a exercise node" in {

      val rightTestCases =
        Table(
          "replayed node",
          exercise,
          exercise.copy(optLocation = None),
          exercise.copy(optLocation = Some(anotherLocation)),
        )

      val leftTestCases =
        Table(
          "recorded node",
          exercise.copy(optLocation = None),
          exercise.copy(exerciseResult = None),
          exercise.copy(key = None)
        )

      forEvery(rightTestCases)(Node.isReplayedBy(exercise, _) shouldBe true)
      forEvery(leftTestCases)(Node.isReplayedBy(_, exercise) shouldBe true)
    }

    "reject an invalid replay of a lookup node" in {

      val rightTestCases =
        Table(
          "replayed node",
          create,
          fetch,
          exercise,
          lookup.copy(templateId = AnotherTemplateTyCon),
          lookup.copy(key = lookup.key.copy(key = anotherKey)),
          lookup.copy(key = lookup.key.copy(key = lookup.key.key.copy(version = v7))),
          lookup.copy(key = lookup.key.copy(maintainers = aliceAndBob)),
          lookup.copy(result = Some(anotherContractId)),
          lookup.copy(result = None),
        )

      forEvery(rightTestCases)(Node.isReplayedBy(lookup, _) shouldBe true)
    }

    "accept valid replay of a lookup node" in {

      val rightTestCases =
        Table(
          "replayed node",
          lookup,
          lookup.copy(optLocation = None),
          lookup.copy(optLocation = Some(anotherLocation)),
          lookup.copy(result = None),
        )

      val leftTestCases =
        Table(
          "recorded node",
          lookup.copy(optLocation = None),
        )

      forEvery(rightTestCases)(Node.isReplayedBy(lookup, _) shouldBe true)
      forEvery(leftTestCases)(Node.isReplayedBy(_, lookup) shouldBe true)

    }

  }

}
