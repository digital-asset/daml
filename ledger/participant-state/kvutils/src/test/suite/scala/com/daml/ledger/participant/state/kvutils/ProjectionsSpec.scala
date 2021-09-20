// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.lf.data.Ref._
import com.daml.lf.data.{BackStack, ImmArray}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.Transaction.Transaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{Node, TransactionVersion}
import com.daml.lf.value.Value.{ContractId, ValueText}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProjectionsSpec extends AnyWordSpec with Matchers {

  def makeCreateNode(cid: ContractId, signatories: Set[Party], stakeholders: Set[Party]) =
    Node.NodeCreate(
      coid = cid,
      templateId = Identifier.assertFromString("some-package:Foo:Bar"),
      arg = ValueText("foo"),
      agreementText = "agreement",
      signatories = signatories,
      stakeholders = stakeholders,
      key = None,
      version = TransactionVersion.minVersion,
    )

  def makeExeNode(
      target: ContractId,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
  ) =
    Node.NodeExercises(
      targetCoid = target,
      templateId = Identifier(
        PackageId.assertFromString("some-package"),
        QualifiedName.assertFromString("Foo:Bar"),
      ),
      choiceId = Name.assertFromString("someChoice"),
      consuming = true,
      actingParties = actingParties,
      chosenValue = ValueText("foo"),
      stakeholders = stakeholders,
      signatories = signatories,
      choiceObservers = Set.empty,
      children = ImmArray.Empty,
      exerciseResult = None,
      key = None,
      byKey = false,
      version = TransactionVersion.minVersion,
    )

  def project(tx: Transaction) = {
    val bi = Blinding.blind(tx)
    Projections.computePerPartyProjectionRoots(tx, bi)
  }

  "computePerPartyProjectionRoots" should {

    "yield no roots with empty transaction" in {
      project(TransactionBuilder.Empty) shouldBe List.empty
    }

    "yield two projection roots for single root transaction with two parties" in {
      val builder = TransactionBuilder()
      val nid = builder.add(
        makeCreateNode(
          builder.newCid,
          Set(Party.assertFromString("Alice")),
          Set(Party.assertFromString("Alice"), Party.assertFromString("Bob")),
        )
      )
      val tx = builder.build()

      project(tx) shouldBe List(
        ProjectionRoots(Party.assertFromString("Alice"), BackStack(nid)),
        ProjectionRoots(Party.assertFromString("Bob"), BackStack(nid)),
      )
    }

    "yield proper projection roots in complex transaction" in {
      val builder = TransactionBuilder()

      // Alice creates an "offer contract" to Bob as part of her workflow.
      // Alice sees both the exercise and the create, and Bob only
      // sees the offer.
      val create = makeCreateNode(
        builder.newCid,
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Bob")),
      )
      val exe = makeExeNode(
        builder.newCid,
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice")),
      )
      val bobCreate = makeCreateNode(
        builder.newCid,
        Set(Party.assertFromString("Bob")),
        Set(Party.assertFromString("Bob")),
      )

      val charlieCreate = makeCreateNode(
        builder.newCid,
        Set(Party.assertFromString("Charlie")),
        Set(Party.assertFromString("Charlie")),
      )

      val nid1 = builder.add(exe)
      val nid2 = builder.add(create, nid1)
      val nid3 = builder.add(bobCreate)
      val nid4 = builder.add(charlieCreate)

      val tx = builder.build()

      project(tx) shouldBe List(
        // Alice should see the exercise as the root.
        ProjectionRoots(Party.assertFromString("Alice"), BackStack(nid1)),
        // Bob only sees the create that followed the exercise, and his own create.
        ProjectionRoots(Party.assertFromString("Bob"), BackStack(nid2, nid3)),
        // Charlie sees just his create.
        ProjectionRoots(Party.assertFromString("Charlie"), BackStack(nid4)),
      )

    }

  }
}
