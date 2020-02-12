// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import scala.language.higherKinds
import com.digitalasset.daml.lf.data.Ref.{PackageId, QualifiedName}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction.{
  AliasedNode,
  DanglingNodeId,
  NotWellFormedError,
  OrphanedNode,
}
import com.digitalasset.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.{Value => V}
import V.ContractInst
import com.digitalasset.daml.lf.value.ValueGenerators.danglingRefGenNode
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

import scala.collection.immutable.HashMap
import scala.language.implicitConversions

class TransactionSpec extends FreeSpec with Matchers with GeneratorDrivenPropertyChecks {
  import TransactionSpec._

  "isWellFormed" - {
    "detects dangling references in roots" in {
      val tx = StringTransaction(HashMap.empty, ImmArray("1"))
      tx.isWellFormed shouldBe Set(NotWellFormedError("1", DanglingNodeId))
    }

    "detects dangling references in children" in {
      val tx = StringTransaction(HashMap("1" -> dummyExerciseNode(ImmArray("2"))), ImmArray("1"))
      tx.isWellFormed shouldBe Set(NotWellFormedError("2", DanglingNodeId))
    }

    "detects cycles" in {
      val tx = StringTransaction(HashMap("1" -> dummyExerciseNode(ImmArray("1"))), ImmArray("1"))
      tx.isWellFormed shouldBe Set(NotWellFormedError("1", AliasedNode))
    }

    "detects aliasing from roots and exercise" in {
      val tx = StringTransaction(
        HashMap(
          "0" -> dummyExerciseNode(ImmArray("1")),
          "1" -> dummyExerciseNode(ImmArray("2")),
          "2" -> dummyCreateNode,
        ),
        ImmArray("0", "2"),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError("2", AliasedNode))
    }

    "detects orphans" in {
      val tx = StringTransaction(HashMap("1" -> dummyCreateNode), ImmArray.empty)
      tx.isWellFormed shouldBe Set(NotWellFormedError("1", OrphanedNode))
    }
  }

  /* TODO SC Gen for well-formed Transaction needed first
  "equalForest" - {
    "is reflexive" in forAll(genTransaction) { tx =>
      tx equalForest tx shouldBe true
    }

    "is node-id-parametric" in forAll(genTransaction) { tx =>
      tx mapNodeId ((_, ())) equalForest tx shouldBe true
    }

    "negation implies == negation" in forAll(genTransaction, genTransaction) { (txa, txb) =>
      whenever(!(txa equalForest txb)) {
        txa should not be txb
      }
    }
  }
   */

  "isReplayedBy" - {
    // the whole-transaction-relevant parts are handled by equalForest testing
    import Node.isReplayedBy
    type CidVal[F[_, _]] = F[V.ContractId, V.VersionedValue[V.ContractId]]
    val genEmptyNode
      : Gen[Node.GenNode.WithTxValue[Nothing, V.ContractId]] = danglingRefGenNode map {
      case (_, n: CidVal[Node.LeafOnlyNode]) => n
      case (_, ne: Node.NodeExercises.WithTxValue[_, V.ContractId]) =>
        ne copy (children = ImmArray.empty)
    }

    "is reflexive" in forAll(genEmptyNode) { n =>
      isReplayedBy(n, n) shouldBe true
    }

    "negation implies == negation" in forAll(genEmptyNode, genEmptyNode) { (na, nb) =>
      whenever(!isReplayedBy(na, nb)) {
        na should not be nb
      }
    }

    "ignores location" in forAll(genEmptyNode) { n =>
      val withoutLocation = n match {
        case nc: CidVal[Node.NodeCreate] => nc copy (optLocation = None)
        case nf: Node.NodeFetch[V.ContractId] => nf copy (optLocation = None)
        case ne: Node.NodeExercises.WithTxValue[Nothing, V.ContractId] =>
          ne copy (optLocation = None)
        case nl: CidVal[Node.NodeLookupByKey] => nl copy (optLocation = None)
      }
      isReplayedBy(withoutLocation, n) shouldBe true
      isReplayedBy(n, withoutLocation) shouldBe true
    }
  }
}

object TransactionSpec {
  private[this] type Value = V[V.AbsoluteContractId]
  type StringTransaction = GenTransaction[String, V.AbsoluteContractId, Value]
  def StringTransaction(
      nodes: HashMap[String, GenNode[String, V.AbsoluteContractId, Value]],
      roots: ImmArray[String],
  ): StringTransaction = GenTransaction(nodes, roots, None)

  def dummyExerciseNode(
      children: ImmArray[String],
      hasExerciseResult: Boolean = true,
  ): NodeExercises[String, V.AbsoluteContractId, Value] =
    NodeExercises(
      nodeSeed = None,
      targetCoid = V.AbsoluteContractId(Ref.ContractIdString.assertFromString("dummyCoid")),
      templateId = Ref.Identifier(
        PackageId.assertFromString("-dummyPkg-"),
        QualifiedName.assertFromString("DummyModule:dummyName"),
      ),
      choiceId = "dummyChoice",
      optLocation = None,
      consuming = true,
      actingParties = Set.empty,
      chosenValue = V.ValueUnit,
      stakeholders = Set.empty,
      signatories = Set.empty,
      controllers = Set.empty,
      children = children,
      exerciseResult = if (hasExerciseResult) Some(V.ValueUnit) else None,
      key = None,
    )

  val dummyCreateNode: NodeCreate[V.AbsoluteContractId, Value] =
    NodeCreate(
      nodeSeed = None,
      coid = V.AbsoluteContractId(Ref.ContractIdString.assertFromString("dummyCoid")),
      coinst = ContractInst(
        Ref.Identifier(
          PackageId.assertFromString("-dummyPkg-"),
          QualifiedName.assertFromString("DummyModule:dummyName"),
        ),
        V.ValueUnit,
        ("dummyAgreement"),
      ),
      optLocation = None,
      signatories = Set.empty,
      stakeholders = Set.empty,
      key = None,
    )

  private implicit def toChoiceName(s: String): Ref.Name = Ref.Name.assertFromString(s)

}
