// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.transaction.{TxTree, TxTreeTop}
import com.daml.lf.transaction.Node.{GenActionNode, NodeRollback, NodeExercises, LeafOnlyActionNode}
import com.daml.lf.value.Value
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Trampoline.{Bounce, Land, Trampoline}

private[lf] object NormalizeRollbacks {

  private[this] type Cid = Value.ContractId
  private[this] type TX = TxTreeTop[Cid]
  private[this] type Tree = TxTree[Cid]

  // Normalize a transaction so rollback nodes satisfy the normalization rules.
  // see `makeRoll` below

  import Canonical.{Norm, Case, caseNorms}
  // The normalization phase works by constructing intermediate values which are
  // `Canonical` in the sense that only properly normalized nodes can be represented.
  // Although this doesn't ensure correctness, one class of bugs is avoided.

  def normalizeTx(txOriginal: TX): TX = {

    // Here we traverse the original transaction structure.
    // During the transformation, an original `Node` is mapped into a List[Norm]

    // The List is necessary because the rules can:
    // (1) drop nodes; (2) combine nodes (3) lift nodes from a lower level to a higher level.

    txOriginal match {
      case TxTreeTop(roots) =>
        def traverseTrees[R](xs: List[Tree])(k: Vector[Norm] => Trampoline[R]): Trampoline[R] = {
          Bounce { () =>
            xs match {
              case Nil => k(Vector.empty)
              case x :: xs =>
                traverseTree(x) { norms1 =>
                  traverseTrees(xs) { norms2 =>
                    Bounce { () =>
                      k(norms1 ++ norms2)
                    }
                  }
                }
            }
          }
        }

        def traverseTree[R](tree: Tree)(k: Vector[Norm] => Trampoline[R]): Trampoline[R] = {
          Bounce { () =>
            tree.node match {

              case NodeRollback(children) =>
                traverseTrees(children.toList) { norms =>
                  makeRoll(norms)(k)
                }

              case exe: NodeExercises[_, _] =>
                traverseTrees(exe.children.toList) { norms =>
                  val children = collapseNorms(norms)
                  val exe2 = exe.copy(children = ImmArray(children))
                  k(Vector(Norm.Act(exe2)))
                }

              case leaf: LeafOnlyActionNode[Cid] =>
                k(Vector(Norm.Act(leaf)))
            }
          }
        }

        traverseTrees(roots.toList) { roots =>
          Land(TxTreeTop(ImmArray(collapseNorms(roots))))
        }.bounce
    }
  }

  // makeRoll: encodes the normalization transformation rules:
  //   rule #1: ROLL [ ] -> ε
  //   rule #2: ROLL [ ROLL [ xs… ] , ys… ] -> ROLL [ xs… ] , ROLL [ ys… ]
  //   rule #3: ROLL [ xs… , ROLL [ ys… ] ] ->  ROLL [ xs… , ys… ]

  //   rule #2/#3 overlap: ROLL [ ROLL [ xs… ] ] -> ROLL [ xs… ]

  private[this] def makeRoll[R](
      norms: Vector[Norm]
  )(k: Vector[Norm] => Trampoline[R]): Trampoline[R] = {
    caseNorms(norms) match {
      case Case.Empty =>
        // normalization rule #1
        k(Vector.empty)

      case Case.Single(roll: Norm.Roll) =>
        // normalization rule #2/#3 overlap
        k(Vector(roll))

      case Case.Single(act: Norm.Act) =>
        // no rule
        k(Vector(Norm.Roll1(act)))

      case Case.Multi(h: Norm.Roll, m, t) =>
        // normalization rule #2
        makeRoll(m :+ t) { norms =>
          k(h +: norms)
        }

      case Case.Multi(h: Norm.Act, m, t: Norm.Roll) =>
        // normalization rule #3
        k(Vector(pushIntoRoll(h, m, t)))

      case Case.Multi(h: Norm.Act, m, t: Norm.Act) =>
        // no rule
        k(Vector(Norm.Roll2(h, m, t)))
    }
  }

  private def pushIntoRoll(a1: Norm.Act, xs2: Vector[Norm], t: Norm.Roll): Norm.Roll = {
    t match {
      case Norm.Roll1(a3) => Norm.Roll2(a1, xs2, a3)
      case Norm.Roll2(a3, xs4, a5) => Norm.Roll2(a1, xs2 ++ Vector(a3) ++ xs4, a5)
    }
  }

  // The `collapse*` functions convert from Canonical types to standard tree nodes

  def collapseNorm(norm: Norm): Tree = {
    norm match {
      case act: Norm.Act => TxTree(act.node)
      case Norm.Roll1(act) =>
        val child = TxTree(act.node)
        TxTree(NodeRollback(children = ImmArray(List(child))))
      case Norm.Roll2(h, m, t) =>
        val children = (List(h) ++ m ++ List(t)).map(collapseNorm)
        TxTree(NodeRollback(children = ImmArray(children)))
    }
  }

  def collapseNorms(norms: Vector[Norm]): List[Tree] = {
    norms.toList.map(collapseNorm)
  }

  // Types which ensure we can only represent the properly normalized cases.
  private object Canonical {

    // A properly normalized Tx/node
    sealed trait Norm
    object Norm {

      // A non-rollback tx/node
      final case class Act(node: GenActionNode[Tree, Cid]) extends Norm

      // A *normalized* rollback tx/node. 2 cases:
      // - rollback containing a single non-rollback tx/node.
      // - rollback of 2 or more tx/nodes, such that first and last are not rollbacks.
      sealed trait Roll extends Norm
      final case class Roll1(act: Act) extends Roll
      final case class Roll2(head: Act, middle: Vector[Norm], tail: Act) extends Roll
    }

    // Case analysis on a list of Norms, distinuishing: Empty, Single and Multi forms
    // The Multi form separes the head and tail element for the middle-list.
    sealed trait Case
    object Case {
      final case object Empty extends Case
      final case class Single(n: Norm) extends Case
      final case class Multi(h: Norm, m: Vector[Norm], t: Norm) extends Case
    }

    def caseNorms(xs: Vector[Norm]): Case = {
      xs.length match {
        case 0 => Case.Empty
        case 1 => Case.Single(xs(0))
        case n => Case.Multi(xs(0), xs.slice(1, n - 1), xs(n - 1))
      }
    }
  }
}
