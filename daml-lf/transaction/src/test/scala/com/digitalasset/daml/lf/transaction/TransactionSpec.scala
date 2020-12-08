// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import scala.language.higherKinds
import com.daml.lf.data.{Bytes, ImmArray, Ref}
import com.daml.lf.transaction.GenTransaction.{
  AliasedNode,
  DanglingNodeId,
  NotWellFormedError,
  OrphanedNode
}
import com.daml.lf.transaction.Node.{GenNode, NodeCreate, NodeExercises}
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.test.ValueGenerators.danglingRefGenNode
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec

import scala.collection.immutable.HashMap
import scala.language.implicitConversions
import scala.util.Random

class TransactionSpec extends AnyFreeSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import TransactionSpec._

  "isWellFormed" - {
    "detects dangling references in roots" in {
      val tx = mkTransaction(HashMap.empty, ImmArray(NodeId(1)))
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), DanglingNodeId))
    }

    "detects dangling references in children" in {
      val tx = mkTransaction(
        HashMap(NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2)))),
        ImmArray(NodeId(1)))
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(2), DanglingNodeId))
    }

    "detects cycles" in {
      val tx = mkTransaction(
        HashMap(NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(1)))),
        ImmArray(NodeId(1)))
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), AliasedNode))
    }

    "detects aliasing from roots and exercise" in {
      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyExerciseNode("cid0", ImmArray(NodeId(1))),
          NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2))),
          NodeId(2) -> dummyCreateNode("cid2"),
        ),
        ImmArray(NodeId(0), NodeId(2)),
      )
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(2), AliasedNode))
    }

    "detects orphans" in {
      val tx = mkTransaction(HashMap(NodeId(1) -> dummyCreateNode("cid1")), ImmArray.empty)
      tx.isWellFormed shouldBe Set(NotWellFormedError(NodeId(1), OrphanedNode))
    }
  }

  "cids" - {

    "collects contract IDs" in {
      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyExerciseNode("cid0", ImmArray(NodeId(1))),
          NodeId(1) -> dummyExerciseNode("cid1", ImmArray(NodeId(2))),
          NodeId(2) -> dummyCreateNode("cid2"),
        ),
        ImmArray(NodeId(0), NodeId(2)),
      )

      def collectCids(tx: Transaction): Set[V.ContractId] = {
        val cids = Set.newBuilder[V.ContractId]
        tx.foreach3(_ => (), cids += _, cids ++= _.cids)
        cids.result()
      }

      collectCids(tx) shouldBe Set[V.ContractId]("cid0", "cid1", "cid2", dummyCid)

    }

  }

  "foldInExecutionOrder" - {
    "should traverse the transaction in execution order" in {

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode("cid0"),
          NodeId(1) -> dummyExerciseNode("cid0", ImmArray(NodeId(2))),
          NodeId(2) -> dummyExerciseNode("cid1", ImmArray.empty),
          NodeId(3) -> dummyCreateNode("cid2"),
        ),
        ImmArray(NodeId(0), NodeId(1), NodeId(3)),
      )

      val result = tx.foldInExecutionOrder(List.empty[String])(
        (acc, nid, _) => s"exerciseBegin(${nid.index})" :: acc,
        (acc, nid, _) => s"leaf(${nid.index})" :: acc,
        (acc, nid, _) => s"exerciseEnd(${nid.index})" :: acc,
      )

      result.reverse.mkString(", ") shouldBe
        "leaf(0), exerciseBegin(1), exerciseBegin(2), exerciseEnd(2), exerciseEnd(1), leaf(3)"
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
    def genTrans(node: GenNode.WithTxValue[NodeId, ContractId]) = {
      val nid = NodeId(1)
      VersionedTransaction(node.version, HashMap(nid -> node), ImmArray(nid))
    }

    def isReplayedBy(
        n1: GenNode.WithTxValue[NodeId, ContractId],
        n2: GenNode.WithTxValue[NodeId, ContractId],
    ) = Transaction.isReplayedBy(genTrans(n1), genTrans(n2))

    // the whole-transaction-relevant parts are handled by equalForest testing
    type CidVal[F[_, _]] = F[V.ContractId, V.VersionedValue[V.ContractId]]
    val genEmptyNode: Gen[GenNode.WithTxValue[Nothing, V.ContractId]] =
      for {
        entry <- danglingRefGenNode
        node = entry match {
          case (_, n: CidVal[Node.LeafOnlyNode]) => n
          case (_, ne: Node.NodeExercises.WithTxValue[_, V.ContractId]) =>
            ne.copy(children = ImmArray.empty)
        }
      } yield node

    "is reflexive" in forAll(genEmptyNode) { n =>
      isReplayedBy(n, n) shouldBe Right(())
    }

    "fail if version is different" in {
      val versions = TransactionVersions.acceptedVersions.toIndexedSeq
      def diffVersion(v: TransactionVersion) = {
        val randomVersion = versions(Random.nextInt(versions.length - 1))
        if (randomVersion != v) randomVersion else versions.last
      }
      forAll(genEmptyNode, minSuccessful(10)) { n =>
        val m = n.updateVersion(diffVersion(n.version))
        isReplayedBy(n, m) shouldBe 'left
      }
    }

    "negation implies == negation" in forAll(genEmptyNode, genEmptyNode) { (na, nb) =>
      whenever(isReplayedBy(na, nb).isLeft) {
        na should not be nb
      }
    }

    "ignores location" in forAll(genEmptyNode) { n =>
      val withoutLocation = {
        val nodeWithoutLocation = n match {
          case nc: CidVal[Node.NodeCreate] => nc copy (optLocation = None)
          case nf: Node.NodeFetch.WithTxValue[V.ContractId] => nf copy (optLocation = None)
          case ne: Node.NodeExercises.WithTxValue[Nothing, V.ContractId] =>
            ne copy (optLocation = None)
          case nl: CidVal[Node.NodeLookupByKey] => nl copy (optLocation = None)
        }
        nodeWithoutLocation
      }
      isReplayedBy(withoutLocation, n) shouldBe Right(())
      isReplayedBy(n, withoutLocation) shouldBe Right(())
    }
  }

  "suffixCid" - {
    "suffix non suffixed and only non suffixed contract ids" in {

      val tx = mkTransaction(
        HashMap(
          NodeId(0) -> dummyCreateNode("cid1"),
          NodeId(0) -> dummyExerciseNode("cid1", ImmArray(NodeId(0))),
          NodeId(1) -> dummyExerciseNode("cid2", ImmArray(NodeId(1))),
        ),
        ImmArray(NodeId(0), NodeId(1)),
      )

      val suffix1 = Bytes.assertFromString("01")
      val suffix2 = Bytes.assertFromString("02")

      val cid1 = toCid("cid1")
      val cid2 = toCid("cid2")

      val mapping1: crypto.Hash => Bytes = Map(
        cid1.discriminator -> suffix1,
        cid2.discriminator -> suffix2,
      )

      val mapping2: V.ContractId => V.ContractId = Map(
        cid1 -> V.ContractId.V1.assertBuild(cid1.discriminator, suffix1),
        cid2 -> V.ContractId.V1.assertBuild(cid2.discriminator, suffix2),
      )

      dummyCreateNode("dd").coinst.suffixCid(mapping1)

      val tx1 = tx.suffixCid(mapping1)
      val tx2 = tx.suffixCid(mapping1)

      tx1 shouldNot be(tx)
      tx2 shouldBe tx1
      tx1 shouldBe Right(tx.map3(identity, mapping2, _.map1(mapping2)))

    }
  }
}

object TransactionSpec {
  private[this] type Value = V[V.ContractId]
  type Transaction = GenTransaction[NodeId, V.ContractId, Value]
  def mkTransaction(
      nodes: HashMap[NodeId, GenNode[NodeId, V.ContractId, Value]],
      roots: ImmArray[NodeId],
  ): Transaction = GenTransaction(nodes, roots)

  def dummyExerciseNode(
      cid: V.ContractId,
      children: ImmArray[NodeId],
      hasExerciseResult: Boolean = true,
  ): NodeExercises[NodeId, V.ContractId, Value] =
    NodeExercises(
      targetCoid = cid,
      templateId = Ref.Identifier(
        Ref.PackageId.assertFromString("-dummyPkg-"),
        Ref.QualifiedName.assertFromString("DummyModule:dummyName"),
      ),
      choiceId = "dummyChoice",
      optLocation = None,
      consuming = true,
      actingParties = Set.empty,
      chosenValue = V.ValueUnit,
      stakeholders = Set.empty,
      signatories = Set.empty,
      choiceObservers = Set.empty,
      children = children,
      exerciseResult = if (hasExerciseResult) Some(V.ValueUnit) else None,
      key = None,
      byKey = false,
      version = TransactionVersions.minVersion,
    )

  val dummyCid = V.ContractId.V1.assertBuild(
    toCid("dummyCid").discriminator,
    Bytes.assertFromString("f00d"),
  )

  def dummyCreateNode(cid: String): NodeCreate[V.ContractId, Value] =
    NodeCreate(
      coid = toCid(cid),
      coinst = V.ContractInst(
        Ref.Identifier(
          Ref.PackageId.assertFromString("-dummyPkg-"),
          Ref.QualifiedName.assertFromString("DummyModule:dummyName"),
        ),
        V.ValueContractId(dummyCid),
        "dummyAgreement",
      ),
      optLocation = None,
      signatories = Set.empty,
      stakeholders = Set.empty,
      key = None,
      version = TransactionVersions.minVersion,
    )

  implicit def toChoiceName(s: String): Ref.Name = Ref.Name.assertFromString(s)

  implicit def toCid(s: String): V.ContractId.V1 =
    V.ContractId.V1(crypto.Hash.hashPrivateKey(s))

}
