// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{BackStack, ImmArray}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.transaction.Transaction.Transaction
import com.digitalasset.daml.lf.transaction.{GenTransaction, Node}
import com.digitalasset.daml.lf.value.Value.{
  ContractId,
  ContractInst,
  NodeId,
  RelativeContractId,
  ValueText,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueVersions
import org.scalatest.{Matchers, WordSpec}

class ProjectionsSpec extends WordSpec with Matchers {

  def makeCreateNode(cid: ContractId, signatories: Set[Party], stakeholders: Set[Party]) =
    Node.NodeCreate(
      cid,
      ContractInst(
        Identifier(
          PackageId.assertFromString("some-package"),
          QualifiedName.assertFromString("Foo:Bar")),
        VersionedValue(ValueVersions.acceptedVersions.last, ValueText("foo")),
        "agreement"
      ),
      None,
      signatories,
      stakeholders,
      None
    )

  def makeExeNode(
      target: ContractId,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      children: ImmArray[NodeId]) =
    Node.NodeExercises(
      target,
      Identifier(
        PackageId.assertFromString("some-package"),
        QualifiedName.assertFromString("Foo:Bar")),
      Name.assertFromString("someChoice"),
      None,
      true,
      actingParties,
      VersionedValue(ValueVersions.acceptedVersions.last, ValueText("foo")),
      stakeholders,
      signatories,
      children,
      None,
      None
    )

  def project(tx: Transaction) = {
    val bi = Blinding.blind(tx)
    Projections.computePerPartyProjectionRoots(tx, bi)
  }

  "computePerPartyProjectionRoots" should {

    "yield no roots with empty transaction" in {
      val emptyTransaction: Transaction = GenTransaction(Map.empty, ImmArray.empty, Set.empty)
      project(emptyTransaction) shouldBe List.empty
    }

    "yield two projection roots for single root transaction with two parties" in {
      val nid = NodeId.unsafeFromIndex(1)
      val root = makeCreateNode(
        RelativeContractId(nid),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice"), Party.assertFromString("Bob")))
      val tx =
        GenTransaction(nodes = Map(nid -> root), roots = ImmArray(nid), usedPackages = Set.empty)

      project(tx) shouldBe List(
        ProjectionRoots(Party.assertFromString("Alice"), BackStack(nid)),
        ProjectionRoots(Party.assertFromString("Bob"), BackStack(nid))
      )
    }

    "yield proper projection roots in complex transaction" in {
      val nid1 = NodeId.unsafeFromIndex(1)
      val nid2 = NodeId.unsafeFromIndex(2)
      val nid3 = NodeId.unsafeFromIndex(3)
      val nid4 = NodeId.unsafeFromIndex(4)

      // Alice creates an "offer contract" to Bob as part of her workflow.
      // Alice sees both the exercise and the create, and Bob only
      // sees the offer.
      val create = makeCreateNode(
        RelativeContractId(nid2),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Bob")))
      val exe = makeExeNode(
        RelativeContractId(nid1),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice")),
        ImmArray(nid2)
      )
      val bobCreate = makeCreateNode(
        RelativeContractId(nid3),
        Set(Party.assertFromString("Bob")),
        Set(Party.assertFromString("Bob")))

      val charlieCreate = makeCreateNode(
        RelativeContractId(nid4),
        Set(Party.assertFromString("Charlie")),
        Set(Party.assertFromString("Charlie")))

      val tx =
        GenTransaction(
          nodes = Map(nid1 -> exe, nid2 -> create, nid3 -> bobCreate, nid4 -> charlieCreate),
          roots = ImmArray(nid1, nid3, nid4),
          usedPackages = Set.empty)

      project(tx) shouldBe List(
        // Alice should see the exercise as the root.
        ProjectionRoots(Party.assertFromString("Alice"), BackStack(nid1)),
        // Bob only sees the create that followed the exercise, and his own create.
        ProjectionRoots(Party.assertFromString("Bob"), BackStack(nid2, nid3)),
        // Charlie sees just his create.
        ProjectionRoots(Party.assertFromString("Charlie"), BackStack(nid4))
      )

    }

  }
}
