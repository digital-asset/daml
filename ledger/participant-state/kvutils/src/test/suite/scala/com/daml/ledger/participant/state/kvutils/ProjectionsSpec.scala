// Copyright (c) 2020 The DAML Authors. All rights reserved.
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

import scala.collection.immutable.TreeMap

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
      val emptyTransaction: Transaction = GenTransaction(TreeMap.empty, ImmArray.empty, None)
      project(emptyTransaction) shouldBe List.empty
    }

    "yield two projection roots for single root transaction with two parties" in {
      val nid = NodeId.unsafeFromIndex(1)
      val root = makeCreateNode(
        RelativeContractId.unsafeFromIndex(2),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice"), Party.assertFromString("Bob")))
      val tx =
        GenTransaction(nodes = TreeMap(nid -> root), roots = ImmArray(nid), optUsedPackages = None)

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

      val rcoid1 = RelativeContractId.unsafeFromIndex(1001)
      val rcoid2 = RelativeContractId.unsafeFromIndex(1002)
      val rcoid3 = RelativeContractId.unsafeFromIndex(1003)

      // Alice creates an "offer contract" to Bob as part of her workflow.
      // Alice sees both the exercise and the create, and Bob only
      // sees the offer.
      val create = makeCreateNode(
        rcoid1,
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Bob")))
      val exe = makeExeNode(
        rcoid1,
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice")),
        Set(Party.assertFromString("Alice")),
        ImmArray(nid2)
      )
      val bobCreate = makeCreateNode(
        rcoid2,
        Set(Party.assertFromString("Bob")),
        Set(Party.assertFromString("Bob")))

      val charlieCreate = makeCreateNode(
        rcoid3,
        Set(Party.assertFromString("Charlie")),
        Set(Party.assertFromString("Charlie")))

      val tx =
        GenTransaction(
          nodes = TreeMap(nid1 -> exe, nid2 -> create, nid3 -> bobCreate, nid4 -> charlieCreate),
          roots = ImmArray(nid1, nid3, nid4),
          optUsedPackages = None)

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
