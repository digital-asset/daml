// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.collection._
import stainless.lang.{unfold, decreases, BooleanDecorations}
import stainless.annotation._
import SetProperties._
import ListProperties._
import SetAxioms._

/** Type class representing forests in manner to avoid measure problems. In the rest of the file and the package we will
  * make no distinction between a tree and a forest.
  *
  * A forest can either be:
  * - A content node: a forest with a node on top of it, that is a tree
  * - An articulation node: the union between a tree (on the left) and a forest (on the right)
  * - The empty forest which does not contain any node.
  *
  * It can be therefore seen as a linear sequence of trees.
  */
sealed trait Tree[T] {

  /** Numbers of node in the tree
    */
  @pure
  @opaque
  def size: BigInt = {
    decreases(this)
    this match {
      case Endpoint() => BigInt(0)
      case ArticulationNode(l, r) => l.size + r.size
      case ContentNode(n, sub) => sub.size + 1
    }
  }.ensuring(res =>
    res >= 0 &&
      ((res == 0) ==> (this == Endpoint[T]()))
  )

  /** Whether the tree contains every node at most once
    */
  @pure
  @opaque
  def isUnique: Boolean = {
    decreases(this)
    this match {
      case Endpoint() => true
      case ArticulationNode(l, r) => l.isUnique && r.isUnique && l.content.disjoint(r.content)
      case ContentNode(n, sub) => sub.isUnique && !sub.contains(n)
    }
  }

  /** Set of nodes
    */
  @pure @opaque
  def content: Set[T] = {
    decreases(this)
    this match {
      case Endpoint() => Set.empty[T]
      case ArticulationNode(l, r) => l.content ++ r.content
      case ContentNode(n, sub) => sub.content + n
    }
  }

  /** Checks if the tree contains a node
    */
  @pure
  def contains(e: T): Boolean = {
    decreases(this)
    unfold(content)
    this match {
      case ArticulationNode(l, r) =>
        SetProperties.unionContains(l.content, r.content, e)
        l.contains(e) || r.contains(e)
      case Endpoint() =>
        SetProperties.emptyContains(e)
        false
      case ContentNode(n, sub) =>
        SetProperties.inclContains(sub.content, n, e)
        sub.contains(e) || (e == n)
    }
  }.ensuring(res => (res == content.contains(e)))

  /** Inorder traversal where each node is visited twice. When entering a node we apply a given function and when exiting
    * the node we apply an other function. While doing this we update a state and return it at the end of the traversal.
    * @param init The initial state of the traversal
    * @param f1 The function that is applied everytime we visit a node for the first time (i.e. we are entering it)
    * @param f2 The function that is applied everytime we visit a node for the second time (i.e. we are exiting it)
    */
  @pure @opaque
  def traverse[Z](init: Z, f1: (Z, T) => Z, f2: (Z, T) => Z): Z = {
    decreases(this)
    unfold(size)
    this match {
      case ArticulationNode(l, r) =>
        r.traverse(l.traverse(init, f1, f2), f1, f2)
      case Endpoint() => init
      case ContentNode(n, sub) => f2(sub.traverse(f1(init, n), f1, f2), n)
    }
  }.ensuring(res => (size == 0) ==> (res == init))

  /** In order traversal where each node is visited twice. When entering a node we apply a given function and when exiting
    * the node we apply an other function. While doing this we update a state and return a list of triple whose entries
    * are:
    *  - Each intermediate state
    *  - The node we have visited at each step
    *  - The traversal direction of the step, i.e. if that's the first or the second time we are visiting it
    *
    * @param init The initial state of the traversal
    * @param f1   The function that is applied everytime we visit a node for the first time (i.e. we are entering it)
    * @param f2   The function that is applied everytime we visit a node for the second time (i.e. we are exiting it)
    */
  @pure
  @opaque
  def scan[Z](init: Z, f1: (Z, T) => Z, f2: (Z, T) => Z): List[(Z, T, TraversalDirection)] = {
    decreases(this)
    unfold(size)
    this match {
      case ArticulationNode(l, r) =>
        concatIndex(l.scan(init, f1, f2), r.scan(l.traverse(init, f1, f2), f1, f2), 2 * size - 1)
        l.scan(init, f1, f2) ++ r.scan(l.traverse(init, f1, f2), f1, f2)
      case Endpoint() => Nil[(Z, T, TraversalDirection)]()
      case ContentNode(n, sub) =>
        concatIndex(
          sub.scan(f1(init, n), f1, f2),
          List((sub.traverse(f1(init, n), f1, f2), n, TraversalDirection.Up)),
          2 * size - 2,
        )
        (init, n, TraversalDirection.Down) :: (sub.scan(f1(init, n), f1, f2) ++ List(
          (sub.traverse(f1(init, n), f1, f2), n, TraversalDirection.Up)
        ))
    }
  }.ensuring(res =>
    (res.size == 2 * size) &&
      ((size > 0) ==> (res(2 * size - 1)._3 == TraversalDirection.Up))
  )

}

sealed trait StructuralNode[T] extends Tree[T]

case class ArticulationNode[T](left: ContentNode[T], right: StructuralNode[T])
    extends StructuralNode[T]

case class Endpoint[T]() extends StructuralNode[T]

case class ContentNode[T](nodeContent: T, sub: StructuralNode[T]) extends Tree[T]

/** Trait describing if the node of a tree is being visited for the first time (Down) or the second time (Up)
  */
sealed trait TraversalDirection

object TraversalDirection {
  case object Up extends TraversalDirection

  case object Down extends TraversalDirection
}

object TreeProperties {

  /** Express an intermediate state of a tree traversal in function of the one of the subtree when the tree is a [[ContentNode]]
    *
    * @param n The node of the content tree
    * @param sub The subtree of the content tree
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param i The step number of the intermediate state
    */
  @pure @opaque
  def scanIndexing[T, Z](
      n: T,
      sub: StructuralNode[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      i: BigInt,
  ): Unit = {
    require(i >= 0)
    require(i < 2 * ContentNode(n, sub).size)
    unfold(ContentNode(n, sub).size)
    unfold(ContentNode(n, sub).scan(init, f1, f2))
    if (i != 0) {
      concatIndex(
        sub.scan(f1(init, n), f1, f2),
        List((sub.traverse(f1(init, n), f1, f2), n, TraversalDirection.Up)),
        i - 1,
      )
    }
  }.ensuring(
    ContentNode(n, sub).scan(init, f1, f2)(i) ==
      (
        if (i == BigInt(0)) (init, n, TraversalDirection.Down)
        else if (i == 2 * (ContentNode(n, sub).size) - 1)
          (sub.traverse(f1(init, n), f1, f2), n, TraversalDirection.Up)
        else (sub.scan(f1(init, n), f1, f2))(i - 1)
      )
  )

  /** Express an intermediate state of a tree traversal in function of the one of the left or the right subtree
    * when the tree is am [[ArticulationNode]]
    *
    * @param l    The left subtree
    * @param r    The right subtree
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param i The step number of the intermediate state
    */
  @pure
  @opaque
  def scanIndexing[T, Z](
      l: ContentNode[T],
      r: StructuralNode[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      i: BigInt,
  ): Unit = {
    require(i >= 0)
    require(i < 2 * ArticulationNode(l, r).size)
    unfold(ArticulationNode(l, r).size)
    unfold(ArticulationNode(l, r).scan(init, f1, f2))
    concatIndex(l.scan(init, f1, f2), r.scan(l.traverse(init, f1, f2), f1, f2), i)
  }.ensuring(
    ArticulationNode(l, r).scan(init, f1, f2)(i) ==
      (
        if (i < 2 * l.size) l.scan(init, f1, f2)(i)
        else r.scan(l.traverse(init, f1, f2), f1, f2)(i - 2 * l.size)
      )
  )

  /** Express an intermediate state of a tree traversal in function of the one before
    *
    * @param n    The node of the content tree
    * @param sub  The subtree of the content tree
    * @param init The initial state of the traversal
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param i The step number of the intermediate state
    */
  @pure @opaque
  def scanIndexingState[T, Z](
      tr: Tree[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      i: BigInt,
  ): Unit = {
    decreases(tr)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(tr.size > 0)

    unfold(tr.scan(init, f1, f2))
    unfold(tr.traverse(init, f1, f2))
    unfold(tr.size)

    tr match {
      case Endpoint() => Trivial()
      case ContentNode(n, sub) =>
        scanIndexing(n, sub, init, f1, f2, i)
        scanIndexing(n, sub, init, f1, f2, 2 * tr.size - 1)

        if (i == 0) {
          Trivial()
        } else if (i == 2 * tr.size - 1) {
          if (sub.size > 0) {
            scanIndexingState(sub, f1(init, n), f1, f2, 0)
            scanIndexing(n, sub, init, f1, f2, i - 1)
          }
        } else {
          scanIndexingState(sub, f1(init, n), f1, f2, i - 1)
          scanIndexing(n, sub, init, f1, f2, i - 1)
        }
      case ArticulationNode(le, ri) =>
        scanIndexing(le, ri, init, f1, f2, 2 * tr.size - 1)
        scanIndexingState(le, init, f1, f2, 0) // traverse

        if (ri.size == 0) {
          scanIndexing(le, ri, init, f1, f2, 2 * le.size - 1)
        } else {
          scanIndexingState(ri, le.traverse(init, f1, f2), f1, f2, 0) // traverse
        }

        scanIndexing(le, ri, init, f1, f2, i)

        if (i == 0) {
          scanIndexingState(le, init, f1, f2, i)
        } else {
          scanIndexing(le, ri, init, f1, f2, i - 1)
          if (i < 2 * le.size) {
            scanIndexingState(le, init, f1, f2, i)
          } else {
            scanIndexingState(ri, le.traverse(init, f1, f2), f1, f2, i - 2 * le.size)
            scanIndexing(le, ri, init, f1, f2, 2 * le.size - 1)
          }
        }

    }

  }.ensuring(
    (tr.scan(init, f1, f2)(i)._1 == (
      if (i == 0) init
      else if (tr.scan(init, f1, f2)(i - 1)._3 == TraversalDirection.Down) {
        f1(tr.scan(init, f1, f2)(i - 1)._1, tr.scan(init, f1, f2)(i - 1)._2)
      } else {
        f2(tr.scan(init, f1, f2)(i - 1)._1, tr.scan(init, f1, f2)(i - 1)._2)
      }
    )) &&
      (tr.traverse(init, f1, f2) == f2(
        tr.scan(init, f1, f2)(2 * tr.size - 1)._1,
        tr.scan(init, f1, f2)(2 * tr.size - 1)._2,
      ))
  )

  /** The nodes and the directions of traversal are only dependent of the tree. That is they are independent of the initial
    * state and of which functions are used to traverse it.
    *
    * @param tr The tree that is being traversed
    * @param init1 The initial state of the first traversal
    * @param init2 The initial state of the second traversal
    * @param f11 The functions that is used when visiting the nodes for the first time in the first traversal
    * @param f12 The functions that is used when visiting the nodes for the secondS time in the first traversal
    * @param f21 The functions that is used when visiting the nodes for the first time in the second traversal
    * @param f22 The functions that is used when visiting the nodes for the second time in the second traversal
    * @param i The place in which the node appears
    */
  @pure
  @opaque
  def scanIndexingNode[T, Z1, Z2](
      tr: Tree[T],
      init1: Z1,
      init2: Z2,
      f11: (Z1, T) => Z1,
      f12: (Z1, T) => Z1,
      f21: (Z2, T) => Z2,
      f22: (Z2, T) => Z2,
      i: BigInt,
  ): Unit = {
    decreases(tr)
    require(i >= 0)
    require(i < 2 * tr.size)

    unfold(tr.scan(init1, f11, f12))
    unfold(tr.scan(init2, f21, f22))
    unfold(tr.size)

    tr match {
      case Endpoint() => Trivial()
      case ContentNode(n, sub) =>
        scanIndexing(n, sub, init1, f11, f12, i)
        scanIndexing(n, sub, init2, f21, f22, i)
        if ((i == 0) || (i == 2 * tr.size - 1)) {
          Trivial()
        } else {
          scanIndexingNode(sub, f11(init1, n), f21(init2, n), f11, f12, f21, f22, i - 1)
        }
      case ArticulationNode(l, r) =>
        scanIndexing(l, r, init1, f11, f12, i)
        scanIndexing(l, r, init2, f21, f22, i)
        if (i < 2 * l.size) {
          scanIndexingNode(l, init1, init2, f11, f12, f21, f22, i)
        } else {
          scanIndexingNode(
            r,
            l.traverse(init1, f11, f12),
            l.traverse(init2, f21, f22),
            f11,
            f12,
            f21,
            f22,
            i - 2 * l.size,
          )
        }

    }

  }.ensuring(
    (tr.scan(init1, f11, f12)(i)._2 == tr.scan(init2, f21, f22)(i)._2) &&
      (tr.scan(init1, f11, f12)(i)._3 == tr.scan(init2, f21, f22)(i)._3)
  )

  /** If an intermediate state of a tree traversal does not satisfy a property but the initial state does, then there
    * is an intermediate state before that does satisfy this property but whose the next one does not.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param p    The propery that is fulfilled by the initial state but not the intermediate one
    * @param i    The step number of the state that does not satisfy p
    */
  @pure
  @opaque
  def scanNotProp[T, Z](
      tr: Tree[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      p: Z => Boolean,
      i: BigInt,
  ): BigInt = {
    decreases(i)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(p(init))
    require(!p(tr.scan(init, f1, f2)(i)._1))

    if (i == 0) {
      scanIndexingState(tr, init, f1, f2, 0)
      Unreachable()
    } else if (p(tr.scan(init, f1, f2)(i - 1)._1)) {
      i - 1
    } else {
      scanNotProp(tr, init, f1, f2, p, i - 1)
    }

  }.ensuring(j =>
    j >= 0 && j < i && p(tr.scan(init, f1, f2)(j)._1) && !p(tr.scan(init, f1, f2)(j + 1)._1)
  )

  /** If an intermediate state of a tree traversal does not satisfy a property but the final state does, then there
    * is an intermediate state after that one that does not satisfy this property but whose the next one does.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param p    The propery that is fulfilled by the final state but not the intermediate one
    * @param i    The step number of the state that does not satisfy p
    */
  @pure
  @opaque
  def scanNotPropRev[T, Z](
      tr: Tree[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      p: Z => Boolean,
      i: BigInt,
  ): BigInt = {
    decreases(2 * tr.size - i)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(p(tr.traverse(init, f1, f2)))
    require(!p(tr.scan(init, f1, f2)(i)._1))

    scanIndexingState(tr, init, f1, f2, i)

    if (i == 2 * tr.size - 1) {
      i
    } else if (p(tr.scan(init, f1, f2)(i + 1)._1)) {
      i
    } else {
      scanNotPropRev(tr, init, f1, f2, p, i + 1)
    }

  }.ensuring(j =>
    (j >= i && j < 2 * tr.size - 1 && !p(tr.scan(init, f1, f2)(j)._1) && p(
      tr.scan(init, f1, f2)(j + 1)._1
    )) ||
      ((j == 2 * tr.size - 1) && !p(tr.scan(init, f1, f2)(j)._1) && p(tr.traverse(init, f1, f2)))
  )

  /** If the state obtained after traversing a tree does not satisfy a property but the initial state does, then there
    * is an intermediate state of the traversal such that it does satisfy this property but the next one does not.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param p     The propery that is fulfilled by the initial state but not the final one
    */
  @pure
  @opaque
  def traverseNotProp[T, Z](
      tr: Tree[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      p: Z => Boolean,
  ): BigInt = {
    require(p(init))
    require(!p(tr.traverse(init, f1, f2)))

    scanIndexingState(tr, init, f1, f2, 0)
    if (p(tr.scan(init, f1, f2)(2 * tr.size - 1)._1)) {
      2 * tr.size - 1
    } else {
      scanNotProp(tr, init, f1, f2, p, 2 * tr.size - 1)
    }
  }.ensuring(i =>
    i >= 0 && ((i < 2 * tr.size - 1 && p(tr.scan(init, f1, f2)(i)._1) && !p(
      tr.scan(init, f1, f2)(i + 1)._1
    ))
      || (i == 2 * tr.size - 1 && p(tr.scan(init, f1, f2)(i)._1)))
  )

  /** If a node appears in a tree traversal then the tree contains it.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param i The step during which the node appears
    */
  @pure @opaque
  def scanContains[T, Z](
      tr: Tree[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      i: BigInt,
  ): Unit = {
    decreases(tr)
    require(0 <= i)
    require(i < 2 * tr.size)
    unfold(tr.size)
    unfold(tr.contains(tr.scan(init, f1, f2)(i)._2))
    tr match {
      case Endpoint() => Unreachable()
      case ContentNode(n, sub) =>
        scanIndexing(n, sub, init, f1, f2, i)
        if (i > 0 && i < 2 * tr.size - 1) {
          scanContains(sub, f1(init, n), f1, f2, i - 1)
        }
      case ArticulationNode(l, r) =>
        scanIndexing(l, r, init, f1, f2, i)
        if (i < 2 * l.size) {
          scanContains(l, init, f1, f2, i)
        } else {
          scanContains(r, l.traverse(init, f1, f2), f1, f2, i - 2 * l.size)
        }
    }
  }.ensuring(tr.contains(tr.scan(init, f1, f2)(i)._2))

  /** If a tree is unique and a node is visited at two locations in the same [[TraversalDirection]], then those two locations
    * are the same.
    *
    * @param tr   The tree that is being traversed
    * @param init The initial state of the traversal
    * @param f1   The function that is used when visiting the nodes for the first time
    * @param f2   The function that is used when visiting the nodes for the second time
    * @param i The first locationof the node
    * @param j The second location of the node
    */
  @pure @opaque
  def isUniqueIndexing[T, Z](
      tr: Tree[T],
      init: Z,
      f1: (Z, T) => Z,
      f2: (Z, T) => Z,
      i: BigInt,
      j: BigInt,
  ): Unit = {
    decreases(tr)
    require(tr.isUnique)
    require(0 <= i)
    require(i < 2 * tr.size)
    require(0 <= j)
    require(j < 2 * tr.size)
    require(tr.scan(init, f1, f2)(i)._2 == tr.scan(init, f1, f2)(j)._2)
    require(tr.scan(init, f1, f2)(i)._3 == tr.scan(init, f1, f2)(j)._3)

    unfold(tr.size)
    unfold(tr.isUnique)

    tr match {
      case Endpoint() => Unreachable()
      case ContentNode(n, sub) =>
        scanIndexing(n, sub, init, f1, f2, i)
        scanIndexing(n, sub, init, f1, f2, j)
        if ((i == 0) && ((j == 0) || (j == 2 * tr.size - 1))) {
          Trivial()
        } else if ((i == 2 * tr.size - 1) && ((j == 0) || (j == 2 * tr.size - 1))) {
          Trivial()
        } else if ((i == 0) || (i == 2 * tr.size - 1)) {
          scanContains(sub, f1(init, n), f1, f2, j - 1)
        } else if ((j == 0) || (j == 2 * tr.size - 1)) {
          scanContains(sub, f1(init, n), f1, f2, i - 1)
        } else {
          isUniqueIndexing(sub, f1(init, n), f1, f2, i - 1, j - 1)
        }
      case ArticulationNode(l, r) =>
        scanIndexing(l, r, init, f1, f2, i)
        scanIndexing(l, r, init, f1, f2, j)
        if ((i < 2 * l.size) && (j < 2 * l.size)) {
          isUniqueIndexing(l, init, f1, f2, i, j)
        } else if ((i >= 2 * l.size) && (j >= 2 * l.size)) {
          isUniqueIndexing(r, l.traverse(init, f1, f2), f1, f2, i - 2 * l.size, j - 2 * l.size)
        } else if (i < 2 * l.size) {
          scanContains(l, init, f1, f2, i)
          scanContains(r, l.traverse(init, f1, f2), f1, f2, j - 2 * l.size)
          disjointContains(l.content, r.content, tr.scan(init, f1, f2)(i)._2)
          disjointContains(l.content, r.content, tr.scan(init, f1, f2)(j)._2)
        } else {
          scanContains(l, init, f1, f2, j)
          scanContains(r, l.traverse(init, f1, f2), f1, f2, i - 2 * l.size)
          disjointContains(l.content, r.content, tr.scan(init, f1, f2)(j)._2)
          disjointContains(l.content, r.content, tr.scan(init, f1, f2)(i)._2)
        }
    }
  }.ensuring(i == j)

  /** Given a node that has been visited for the second time in a traversal, returns when it has been visited for the
    * first time.
    * @param tr The tree that is being traversed
    * @param init The initial state of the traversal
    * @param f1 The function that is used when visiting the nodes for the first time
    * @param f2 The function that is used when visiting the nodes for the second time
    * @param i The step number during whch the node is visited for the second time.
    */
  @pure @opaque
  def findDown[T, Z](tr: Tree[T], init: Z, f1: (Z, T) => Z, f2: (Z, T) => Z, i: BigInt): BigInt = {
    decreases(tr)
    require(i >= 0)
    require(i < 2 * tr.size)
    require(tr.scan(init, f1, f2)(i)._3 == TraversalDirection.Up)
    unfold(tr.scan(init, f1, f2))
    unfold(tr.size)
    tr match {
      case Endpoint() => Unreachable()
      case ContentNode(c, sub) =>
        scanIndexing(c, sub, init, f1, f2, i)
        if (i == 2 * tr.size - 1) {
          scanIndexing(c, sub, init, f1, f2, 0)
          BigInt(0)
        } else {
          val j = findDown(sub, f1(init, c), f1, f2, i - 1)
          scanIndexing(c, sub, init, f1, f2, j + 1)
          j + 1
        }
      case ArticulationNode(l, r) =>
        scanIndexing(l, r, init, f1, f2, i)
        if (i < 2 * l.size) {
          val j = findDown(l, init, f1, f2, i)
          scanIndexing(l, r, init, f1, f2, j)
          j
        } else {
          val j = findDown(r, l.traverse(init, f1, f2), f1, f2, i - 2 * l.size)
          scanIndexing(l, r, init, f1, f2, j + 2 * l.size)
          j + 2 * l.size
        }
    }
  }.ensuring(j =>
    (j < i) &&
      (tr.scan(init, f1, f2)(j)._3 == TraversalDirection.Down) &&
      (tr.scan(init, f1, f2)(i)._2 == tr.scan(init, f1, f2)(j)._2)
  )

}
