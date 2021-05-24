// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref
import com.daml.lf.transaction.Node.{GenNode, NodeRollback, NodeCreate, NodeExercises}
import com.daml.lf.transaction.Transaction.LeafNode
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.transaction.{NodeId, GenTransaction}
import com.daml.lf.value.{Value => V}

class NormalizeRollbacksSpec extends AnyWordSpec with Matchers with Inside {

  import NormalizeRollbackSpec._

  // TODO: https://github.com/digital-asset/daml/issues/8020
  // Below we test a set of hand-constructed testcases. Hopefully we got all edge cases!
  // But for more confidence we might consider adding scalacheck based testing.

  // TODO: https://github.com/digital-asset/daml/issues/8020
  // We should test that the `meaning` of a transaction is preserved by normalization.

  def test(name: String)(orig: Shape.Top, expected: Shape.Top): Unit = {
    s"normalize: ($orig) -- $name" should {
      val tx = Shape.toTransaction(orig)
      val txN = NormalizeRollbacks.normalizeTx(tx) // code under test
      val shapeN = Shape.ofTransaction(txN)
      "be as expected" in {
        assert(shapeN == expected)
      }
      "be a normal form" in {
        assert(isNormalized(txN))
      }
      "have increasing node-ids when listed in pre-order" in {
        assert(preOrderNidsOfTxIsIncreasingFromZero(txN))
      }
    }
  }

  // multi arg construction for example convenience
  def Top(xs: Shape*) = Shape.Top(xs.toList)
  def E(xs: Shape*) = Shape.Exercise(xs.toList)
  def R(xs: Shape*) = Shape.Rollback(xs.toList)

  val List(c1, c2, c3, c4) = List[Long](1, 2, 3, 4).map(Shape.Create)

  // no normalization required
  test("empty tx")(
    Top(),
    Top(),
  )
  test("one create")(
    Top(c1),
    Top(c1),
  )
  test("two creates")(
    Top(c1, c2),
    Top(c1, c2),
  )
  test("rollback create")(
    Top(R(c1)),
    Top(R(c1)),
  )
  test("non empty rollback between creates")(
    Top(c1, R(c2), c3),
    Top(c1, R(c2), c3),
  )
  test("empty exercise")(
    Top(E()),
    Top(E()),
  )
  test("exercise and creates")(
    Top(E(c1, E(), c2)),
    Top(E(c1, E(), c2)),
  )

  // normalization rule #1
  test("empty rollback")(
    Top(R()),
    Top(),
  )
  test("empty rollback after create")(
    Top(c1, R()),
    Top(c1),
  )
  test("empty rollback before create")(
    Top(R(), c1),
    Top(c1),
  )
  test("empty rollback between creates")(
    Top(c1, R(), c2),
    Top(c1, c2),
  )
  test("sibling empty rollback")(
    Top(R(), R()),
    Top(),
  )
  test("nested inner empty rollback")(
    Top(R(c1, R(), c2)),
    Top(R(c1, c2)),
  )
  test("inner empty rollback, within exercise")(
    Top(E(c1, R(), c2)),
    Top(E(c1, c2)),
  )
  test("nested empty rollback")(
    Top(R(R())),
    Top(),
  )
  test("nested sibling empty rollback")(
    Top(R(R(), R())),
    Top(),
  )

  // normalization rules #2 or #3
  test("nested-rollback")(
    Top(R(R(c1))),
    Top(R(c1)),
  )

  // normalization rules #2
  test("head-nested-rollback")(
    Top(R(R(c1), c2)),
    Top(R(c1), R(c2)),
  )
  test("head-nested-rollback-cascade")(
    Top(R(R(R(c1), c2), c3)),
    Top(R(c1), R(c2), R(c3)),
  )

  // normalization rules #3
  test("tail-nested-rollback")(
    Top(R(c1, R(c2))),
    Top(R(c1, c2)),
  )
  test("tail-nested-rollback-cascade")(
    Top(R(c1, R(c2, R(c3)))),
    Top(R(c1, c2, c3)),
  )
  test("tail-nested-rollback-cascade-complex")(
    Top(R(c1, c2, R(c3, R(c4)))),
    Top(R(c1, c2, c3, c4)),
  )

  // normalization rules #2 and #3
  test("mixed-rollback-nesting-A")(
    Top(R(R(c1), c2, R(c3))),
    Top(R(c1), R(c2, c3)),
  )
  test("mixed-rollback-nesting-B")(
    Top(R(R(c1), c2, c3, R(c4))),
    Top(R(c1), R(c2, c3, c4)),
  )
  test("mixed-extra-1")(
    Top(R(c1, R(R(c2), c3))),
    Top(R(c1, R(c2), c3)),
  )
  test("mixed-extra-2")(
    Top(R(R(c1, R(c2)), c3)),
    Top(R(c1, c2), R(c3)),
  )

}

object NormalizeRollbackSpec {

  type Nid = NodeId
  type Cid = V.ContractId
  type TX = GenTransaction[Nid, Cid]
  type Node = GenNode[Nid, Cid]
  type RB = NodeRollback[Nid]

  def preOrderNidsOfTxIsIncreasingFromZero(tx: TX): Boolean = {
    def check(x1: Int, xs: List[Int]): Boolean = {
      xs match {
        case Nil => true
        case x2 :: xs => x1 < x2 && check(x2, xs)
      }
    }
    preOrderNidsOfTx(tx) match {
      case Nil => true
      case x :: xs => x == 0 && check(x, xs)
    }
  }

  def preOrderNidsOfTx(tx: TX): List[Int] = {
    def fromNids(acc: List[Int], xs: List[Nid]): List[Int] = {
      xs match {
        case Nil => acc
        case x :: xs =>
          val node = tx.nodes(x)
          fromNids(fromNode(x.index :: acc, node), xs)
      }
    }
    def fromNode(acc: List[Int], node: Node): List[Int] = {
      node match {
        case _: LeafNode => acc
        case node: NodeExercises[_, _] => fromNids(acc, node.children.toList)
        case node: NodeRollback[_] => fromNids(acc, node.children.toList)
      }
    }
    fromNids(Nil, tx.roots.toList).reverse
  }

  def forallNode(tx: TX)(pred: Node => Boolean): Boolean = {
    tx.fold[Boolean](true) { case (acc, (_, node)) =>
      acc && pred(node)
    }
  }

  def forallRB(tx: TX)(pred: RB => Boolean): Boolean = {
    forallNode(tx) {
      case rb: NodeRollback[_] => pred(rb)
      case _ => true
    }
  }

  def isNormalized(tx: TX): Boolean = {
    tx match {
      case GenTransaction(nodes, _) =>
        def isRB(node: Node): Boolean = {
          node match {
            case _: NodeRollback[_] => true
            case _ => false
          }
        }
        def check(rb: RB): Boolean = {
          val n = rb.children.length
          (n > 0) && // Normalization rule #1
          !isRB(nodes(rb.children(0))) && // Normalization rule #2
          !isRB(nodes(rb.children(n - 1))) // Normalization rule #3
        }
        forallRB(tx) { rb =>
          check(rb)
        }
    }
  }

  // Shape: description of a transaction, with conversions to and from a real tx
  sealed trait Shape
  object Shape {

    final case class Top(xs: List[Shape])
    final case class Create(x: Long) extends Shape
    final case class Exercise(x: List[Shape]) extends Shape
    final case class Rollback(x: List[Shape]) extends Shape

    def toTransaction(top: Top): TX = {
      val ids = Iterator.from(0).map(NodeId(_))
      var nodes: Map[Nid, Node] = Map.empty
      def add(node: Node): Nid = {
        val nodeId = ids.next()
        nodes += (nodeId -> node)
        nodeId
      }
      def toNid(shape: Shape): Nid = {
        shape match {
          case Create(n) => add(dummyCreateNode(n))
          case Exercise(shapes) =>
            val children = shapes.map(toNid)
            add(dummyExerciseNode(ImmArray(children)))
          case Rollback(shapes) =>
            val children = shapes.map(toNid)
            add(NodeRollback[Nid](children = ImmArray(children)))
        }
      }
      val roots: List[Nid] = top.xs.map(toNid)
      GenTransaction(nodes, ImmArray(roots))
    }

    def ofTransaction(tx: TX): Top = {
      def ofNid(nid: NodeId): Shape = {
        tx.nodes(nid) match {
          case create: NodeCreate[_] =>
            create.arg match {
              case V.ValueInt64(n) => Create(n)
              case _ => sys.error(s"unexpected create.arg: ${create.arg}")
            }
          case leaf: LeafNode => sys.error(s"Shape.ofTransaction, unexpected leaf: $leaf")
          case node: NodeExercises[_, _] => Exercise(node.children.toList.map(ofNid))
          case node: NodeRollback[_] => Rollback(node.children.toList.map(ofNid))
        }
      }
      Top(tx.roots.toList.map(nid => ofNid(nid)))
    }
  }

  private def toCid(s: String): V.ContractId.V1 =
    V.ContractId.V1(crypto.Hash.hashPrivateKey(s))

  private def dummyCreateNode(n: Long): NodeCreate[V.ContractId] =
    NodeCreate(
      coid = toCid("dummyCid"),
      templateId = Ref.Identifier.assertFromString("-dummyPkg-:DummyModule:dummyName"),
      arg = V.ValueInt64(n),
      agreementText = "dummyAgreement",
      optLocation = None,
      signatories = Set.empty,
      stakeholders = Set.empty,
      key = None,
      version = TransactionVersion.minVersion,
    )

  private def dummyExerciseNode(
      children: ImmArray[NodeId]
  ): NodeExercises[NodeId, V.ContractId] =
    NodeExercises(
      targetCoid = toCid("dummyTargetCoid"),
      templateId = Ref.Identifier(
        Ref.PackageId.assertFromString("-dummyPkg-"),
        Ref.QualifiedName.assertFromString("DummyModule:dummyName"),
      ),
      choiceId = Ref.Name.assertFromString("dummyChoice"),
      optLocation = None,
      consuming = true,
      actingParties = Set.empty,
      chosenValue = V.ValueUnit,
      stakeholders = Set.empty,
      signatories = Set.empty,
      choiceObservers = Set.empty,
      children = children,
      exerciseResult = None,
      key = None,
      byKey = false,
      version = TransactionVersion.minVersion,
    )
}
