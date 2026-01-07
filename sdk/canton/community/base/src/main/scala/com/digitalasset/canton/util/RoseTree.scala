// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Apply
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.util.RoseTree.{HashCodeState, MapState, hashCodeInit}
import monocle.macros.syntax.lens.*

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3

/** Implements a finitely branching tree, also known as a rose tree. All methods are stack-safe.
  */
final class RoseTree[+A] private (
    val root: A,
    val children: Seq[RoseTree[A]],
    val size: Int,
) extends Product
    with Serializable {

  /** Iterator over all elements in this tree in preorder */
  def preorder: Iterator[A] = RoseTree.PreorderIterator(this)

  /** Tail-recursive implementation equivalent to
    *
    * {{{
    *   def foldl[State, Result](tree: RoseTree[A])(init: RoseTree[A] => State)(finish: State => Result)(update: (State, Result) => State): Result =
    *     finish(children.foldLeft(init(this))((acc, child) => update(acc, child.foldl(init)(finish)(update))))
    * }}}
    */
  def foldl[State, Result](init: RoseTree[A] => State)(finish: State => Result)(
      update: (State, Result) => State
  ): Result = RoseTree.foldLeft(RoseTree.roseTreeOps[A], this)(init)(finish)(update)

  /** Tail-recursive implementation equivalent to
    *
    * {{{
    *   def map[A](f: A => B): RoseTree[B] = RoseTree(f(root), children.map(_.map(f))*)
    * }}}
    */
  def map[B](f: A => B): RoseTree[B] =
    RoseTree.foldLeft(RoseTree.roseTreeOps[A], this)(
      init = t => MapState(f(t.root), t.size, List.empty)
    )(finish = { case MapState(mappedRoot, size, reverseVisitedChildren) =>
      new RoseTree(mappedRoot, reverseVisitedChildren.reverse, size)
    })(update = (state, child) => state.focus(_.visitedReverse).modify(child +: _))

  /** Tail-recursive implementation equivalent to
    * {{{
    *   def zipWith[B, C](that: RoseTree[B])(f: (A, B) => C): RoseTree[C] =
    *     RoseTree(
    *       f(this.root, that.root),
    *       (this.children.zip(that.children).map { case (l, r) => l.zipWith(r)(f) }) *,
    *     )
    * }}}
    */
  def zipWith[B, C](that: RoseTree[B])(f: (A, B) => C): RoseTree[C] =
    RoseTree.zipWith(this, that)(f)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def equals(other: Any): Boolean =
    if (this eq other.asInstanceOf[Object]) true
    else
      other match {
        case that: RoseTree[?] => that.canEqual(this) && RoseTree.equals(this, that)
        case _ => false
      }

  /** Tail-recursive implementation for [[RoseTree]] equivalent to
    * [[scala.util.hashing.MurmurHash3$.productHash(x:Product):Int*]]
    */
  override def hashCode(): Int =
    RoseTree.foldLeft(RoseTree.roseTreeOps[A], this)(
      init = t => HashCodeState(MurmurHash3.mix(hashCodeInit, t.root.##), 1)
    )(
      finish = { case HashCodeState(h, arity) => MurmurHash3.finalizeHash(h, arity) }
    )(update = (state, child) => HashCodeState(MurmurHash3.mix(state.hash, child), state.arity + 1))

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[RoseTree[?]]

  override def productArity: Int = 1 + children.size
  override def productElement(n: Int): Any =
    if (n == 0) root else children(n - 1)
  override def productPrefix: String = RoseTree.productPrefix

  override def toString: String = {
    val builder = new mutable.StringBuilder()
    RoseTree.foldLeft(RoseTree.roseTreeOps[A], this)(
      init = t => builder.append(", RoseTree(").append(t.root).discard
    )(
      finish = _ => builder.append(")").discard
    )(update = (_, _) => ())

    builder.drop(2).toString()
  }
}

object RoseTree {
  private def productPrefix = "RoseTree"

  def apply[A](root: A, children: RoseTree[A]*): RoseTree[A] =
    new RoseTree(root, children, 1 + children.map(_.size).sum)

  def unapply[A](tree: RoseTree[A]): Some[(A, Seq[RoseTree[A]])] =
    Some((tree.root, tree.children))

  implicit val applyRoseTree: Apply[RoseTree] = new Apply[RoseTree] {
    override def map[A, B](fa: RoseTree[A])(f: A => B): RoseTree[B] = fa.map(f)

    override def ap[A, B](ff: RoseTree[A => B])(fa: RoseTree[A]): RoseTree[B] =
      zipWith(ff, fa)((f, a) => f(a))

    override def map2[A, B, Z](fa: RoseTree[A], fb: RoseTree[B])(f: (A, B) => Z): RoseTree[Z] =
      zipWith(fa, fb)(f)
  }

  /** Pre-order iterator for a rose tree. Not an inner class of [[RoseTree]] so that the iterator
    * does not prevent garbage collection of already visited nodes by storing references to them.
    */
  private final class PreorderIterator[A] private (
      stack: ListBuffer[RoseTree[A]],
      initialSize: Int,
  ) extends Iterator[A] {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var sizeV: Int = initialSize

    override def knownSize: Int = sizeV

    override def hasNext: Boolean = stack.nonEmpty

    override def next(): A = {
      if (stack.isEmpty)
        throw new NoSuchElementException("No further elements in the rose tree")

      val RoseTree(root, children) = stack.remove(0)
      stack.prependAll(children)
      sizeV -= 1
      root
    }
  }
  private object PreorderIterator {
    def apply[A](tree: RoseTree[A]): PreorderIterator[A] =
      new PreorderIterator[A](mutable.ListBuffer(tree), tree.size)
  }

  trait TreeOps[Node] {
    def children(node: Node): Iterator[Node]
  }

  private[this] case object RoseTreeOps extends TreeOps[RoseTree[?]] {
    override def children(node: RoseTree[?]): Iterator[RoseTree[?]] = node.children.iterator
  }
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def roseTreeOps[A]: TreeOps[RoseTree[A]] = RoseTreeOps.asInstanceOf[TreeOps[RoseTree[A]]]

  /** The `foldLeft` Zipper data structure for an individual node
    * @param state
    *   The accumulated state of the `foldLeft` for all children visited so far
    * @param unvisitedSiblings
    *   An iterator over the remaining siblings of the current node
    */
  private final case class FoldlZipperLevel[Node, B](
      state: B,
      unvisitedSiblings: Iterator[Node],
  )

  def foldLeft[Node, State, Result](treeOps: TreeOps[Node], tree: Node)(
      init: Node => State
  )(
      finish: State => Result
  )(
      update: (State, Result) => State
  ): Result = {
    // Called when we have folded over all children of a node. Moves up the tree
    // until we find an unvisited sibling or end at the root.
    @tailrec def moveUp(
        c: Result,
        parents: List[FoldlZipperLevel[Node, State]],
    ): Either[Result, (Node, List[FoldlZipperLevel[Node, State]])] =
      NonEmpty.from(parents) match {
        case None => Left(c)
        case Some(parentsNE) =>
          val parentLevel = parentsNE.head1
          val siblingIter = parentLevel.unvisitedSiblings
          val newState = update(parentLevel.state, c)
          if (siblingIter.hasNext) {
            val nextSibling = siblingIter.next()
            val newParentLevel = parentLevel.copy(state = newState)
            Right((nextSibling, newParentLevel +: parentsNE.tail1))
          } else {
            moveUp(finish(newState), parentsNE.tail1)
          }
      }

    @tailrec def go(
        current: Node,
        parents: List[FoldlZipperLevel[Node, State]],
    ): Result = {
      val state = init(current)
      val childIter = treeOps.children(current)
      if (childIter.hasNext) {
        // Descent to the next level in the tree
        go(childIter.next(), FoldlZipperLevel(state, childIter) +: parents)
      } else {
        // Move up in the tree until we find further siblings to process
        val c = finish(state)
        moveUp(c, parents) match {
          case Left(result) => result
          case Right((nextSibling, newParents)) =>
            go(nextSibling, newParents)
        }
      }
    }
    go(tree, List.empty)
  }

  /** The `zipWith` Zipper data structure for an individual node
    * @param state
    *   The accumulated result of zipping the already visited children
    * @param unvisitedSiblingsL
    *   An iterator over the remaining siblings of the left node
    * @param unvisitedSiblingsR
    *   An iterator over the remaining siblings of the right node
    */
  private final case class ZipWithZipperLevel[A, B, S](
      state: S,
      unvisitedSiblingsL: Iterator[RoseTree[A]],
      unvisitedSiblingsR: Iterator[RoseTree[B]],
  )

  private final case class ZipWithState[C](zippedRoot: C, visitedReverse: List[RoseTree[C]])

  private def zipWith[A, B, C](left: RoseTree[A], right: RoseTree[B])(
      f: (A, B) => C
  ): RoseTree[C] = {
    type State = ZipWithState[C]

    // Called when we have zipped over all the children of two nodes. Moves up the two trees
    // until we find unvisited siblings or end at the roots.
    @tailrec def moveUp(
        c: RoseTree[C],
        parents: List[ZipWithZipperLevel[A, B, State]],
    ): Either[
      RoseTree[C],
      (RoseTree[A], RoseTree[B], List[ZipWithZipperLevel[A, B, State]]),
    ] =
      NonEmpty.from(parents) match {
        case None => Left(c)
        case Some(parentsNE) =>
          val parentLevel = parentsNE.head1
          val siblingIterL = parentLevel.unvisitedSiblingsL
          val siblingIterR = parentLevel.unvisitedSiblingsR
          val newState = parentLevel.state.focus(_.visitedReverse).modify(c +: _)
          if (siblingIterL.hasNext && siblingIterR.hasNext) {
            val nextSiblingL = siblingIterL.next()
            val nextSiblingR = siblingIterR.next()
            val newParentLevel = parentLevel.copy(state = newState)
            Right((nextSiblingL, nextSiblingR, newParentLevel +: parentsNE.tail1))
          } else {
            val assembled = RoseTree(newState.zippedRoot, newState.visitedReverse.reverse*)
            moveUp(assembled, parentsNE.tail1)
          }
      }

    @tailrec def go(
        currentL: RoseTree[A],
        currentR: RoseTree[B],
        zipper: List[ZipWithZipperLevel[A, B, State]],
    ): RoseTree[C] = {
      val zippedRoot = f(currentL.root, currentR.root)
      val childIterL = currentL.children.iterator
      val childIterR = currentR.children.iterator
      if (childIterL.hasNext && childIterR.hasNext) {
        val state = ZipWithState(zippedRoot, List.empty)
        go(
          childIterL.next(),
          childIterR.next(),
          ZipWithZipperLevel(state, childIterL, childIterR) +: zipper,
        )
      } else {
        val zipped = RoseTree(zippedRoot)
        moveUp(zipped, zipper) match {
          case Left(result) => result
          case Right((nextL, nextR, newParents)) =>
            go(nextL, nextR, newParents)
        }
      }
    }

    go(left, right, List.empty)
  }

  private final case class MapState[B](
      mappedRoot: B,
      size: Int,
      visitedReverse: List[RoseTree[B]],
  )

  private def equals[A, B](first: RoseTree[A], second: RoseTree[B]): Boolean = {
    @tailrec def go(
        stack: List[(Iterator[RoseTree[A]], Iterator[RoseTree[B]])]
    ): Boolean =
      NonEmpty.from(stack) match {
        case None => true
        case Some(stackNE) =>
          val (next1, next2) = stackNE.head1
          if (next1.hasNext != next2.hasNext) false
          else if (!next1.hasNext) go(stackNE.tail1)
          else {
            val current1 = next1.next()
            val current2 = next2.next()
            if (current1.size != current2.size || current1.root != current2.root) false
            else {
              val children1 = current1.children.iterator
              val children2 = current2.children.iterator
              go((children1, children2) +: stack)
            }
          }
      }

    go(List(Iterator(first) -> Iterator(second)))
  }

  private final case class HashCodeState(hash: Int, arity: Int)

  private val hashCodeInit: Int =
    MurmurHash3.mix(MurmurHash3.productSeed, productPrefix.hashCode)

}
