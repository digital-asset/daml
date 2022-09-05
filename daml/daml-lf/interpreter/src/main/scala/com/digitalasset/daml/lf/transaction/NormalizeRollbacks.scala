// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.transaction.{NodeId, Transaction}
import com.daml.lf.transaction.Node
import com.daml.lf.data.{BackStack, ImmArray}
import com.daml.lf.data.Trampoline.{Bounce, Land, Trampoline}

private[lf] object NormalizeRollbacks {

  private[this] type TX = Transaction

  // Normalize a transaction so rollback nodes satisfy the normalization rules.
  // see `makeRoll` below

  import Canonical.{Norm, Case, caseNorms}
  // The normalization phase works by constructing intermediate values which are
  // `Canonical` in the sense that only properly normalized nodes can be represented.
  // Although this doesn't ensure correctness, one class of bugs is avoided.

  def normalizeTx(txOriginal: TX): (TX, ImmArray[NodeId]) = {

    // Here we traverse the original transaction structure.
    // During the transformation, an original `Node` is mapped into a List[Norm]

    // The List is necessary because the rules can:
    // (1) drop nodes; (2) combine nodes (3) lift nodes from a lower level to a higher level.

    txOriginal match {
      case Transaction(nodesOriginal, rootsOriginal) =>
        def traverseNodeIds[R](
            xs: List[NodeId]
        )(k: Vector[Norm] => Trampoline[R]): Trampoline[R] = {
          Bounce { () =>
            xs match {
              case Nil => k(Vector.empty)
              case x :: xs =>
                traverseNode(nodesOriginal(x)) { norms1 =>
                  traverseNodeIds(xs) { norms2 =>
                    Bounce { () =>
                      k(norms1 ++ norms2)
                    }
                  }
                }
            }
          }
        }

        def traverseNode[R](node: Node)(k: Vector[Norm] => Trampoline[R]): Trampoline[R] = {
          Bounce { () =>
            node match {

              case Node.Rollback(children) =>
                traverseNodeIds(children.toList) { norms =>
                  makeRoll(norms)(k)
                }

              case exe: Node.Exercise =>
                traverseNodeIds(exe.children.toList) { norms =>
                  k(Vector(Norm.Exe(exe, norms.toList)))
                }

              case leaf: Node.LeafOnlyAction =>
                k(Vector(Norm.Leaf(leaf)))
            }
          }
        }
        // pass 1
        traverseNodeIds(rootsOriginal.toList) { norms =>
          // pass 2
          pushNorms(initialState, norms.toList) { (finalState, roots) =>
            Land(
              (
                Transaction(finalState.nodeMap, roots.to(ImmArray)),
                finalState.seedIds.toImmArray,
              )
            )
          }
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

  // State manages a counter for node-id generation, and accumulates the nodes-map for
  // the normalized transaction

  // There is no connection between the ids in the original and normalized transaction.

  private[this] case class State(
      index: Int,
      nodeMap: Map[NodeId, Node],
      seedIds: BackStack[NodeId],
  ) {

    def next[R](k: (State, NodeId) => Trampoline[R]): Trampoline[R] = {
      k(copy(index = index + 1), NodeId(index))
    }

    def push[R](nid: NodeId, node: Node)(k: (State, NodeId) => Trampoline[R]): Trampoline[R] = {
      k(copy(nodeMap = nodeMap + (nid -> node)), nid)
    }
    def pushSeedId[R](nid: NodeId)(k: State => Trampoline[R]): Trampoline[R] =
      k(copy(seedIds = seedIds :+ nid))
  }

  // The `push*` functions convert the Canonical types to the tx being collected in State.
  // Ensuring:
  // - The final tx has increasing node-ids when nodes are listed in pre-order.
  // - The root node-id is 0 (we have tests that rely on this)

  private val initialState = State(0, Map.empty, BackStack.empty)

  private def pushAct[R](s: State, x: Norm.Act)(
      k: (State, NodeId) => Trampoline[R]
  ): Trampoline[R] = {
    Bounce { () =>
      s.next { (s, me) =>
        x match {
          case Norm.Leaf(node) =>
            node match {
              case _: Node.Create =>
                s.pushSeedId(me) { s =>
                  s.push(me, node)(k)
                }
              case _: Node.Fetch | _: Node.LookupByKey =>
                s.push(me, node)(k)
            }
          case Norm.Exe(exe, subs) =>
            s.pushSeedId(me) { s =>
              pushNorms(s, subs) { (s, children) =>
                val node = exe.copy(children = children.to(ImmArray))
                s.push(me, node)(k)
              }
            }
        }
      }
    }
  }

  private def pushRoll[R](s: State, x: Norm.Roll)(
      k: (State, NodeId) => Trampoline[R]
  ): Trampoline[R] = {
    s.next { (s, me) =>
      x match {
        case Norm.Roll1(act) =>
          pushAct(s, act) { (s, child) =>
            val node = Node.Rollback(children = ImmArray(child))
            s.push(me, node)(k)
          }
        case Norm.Roll2(h, m, t) =>
          pushAct(s, h) { (s, hh) =>
            pushNorms(s, m.toList) { (s, mm) =>
              pushAct(s, t) { (s, tt) =>
                val children = List(hh) ++ mm ++ List(tt)
                val node = Node.Rollback(children = children.to(ImmArray))
                s.push(me, node)(k)
              }
            }
          }
      }
    }
  }

  private def pushNorm[R](s: State, x: Norm)(k: (State, NodeId) => Trampoline[R]): Trampoline[R] = {
    x match {
      case act: Norm.Act => pushAct(s, act)(k)
      case roll: Norm.Roll => pushRoll(s, roll)(k)
    }
  }

  private def pushNorms[R](s: State, xs: List[Norm])(
      k: (State, List[NodeId]) => Trampoline[R]
  ): Trampoline[R] = {
    Bounce { () =>
      xs match {
        case Nil => k(s, Nil)
        case x :: xs =>
          pushNorm(s, x) { (s, y) =>
            pushNorms(s, xs) { (s, ys) =>
              Bounce { () =>
                k(s, y :: ys)
              }
            }
          }
      }
    }
  }

  // Types which ensure we can only represent the properly normalized cases.
  private object Canonical {

    // A properly normalized Tx/node
    sealed trait Norm
    object Norm {

      // A non-rollback tx/node
      sealed trait Act extends Norm
      final case class Leaf(node: Node.LeafOnlyAction) extends Act
      final case class Exe(node: Node.Exercise, children: List[Norm]) extends Act

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
