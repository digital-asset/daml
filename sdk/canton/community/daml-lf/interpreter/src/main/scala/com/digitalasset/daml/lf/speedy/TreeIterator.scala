// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.scalautil.Statement.discard

/** A factory for iterators over the reachable nodes of a tree.
  * The tree is defined by the function `children` that maps each node to its children.
  *
  * Note: There are no checks that `children` actually defines a tree.
  * If `children` defines a graph, then the iterator will return each node `x` in the DAG
  * as many times as there are paths from the start node to `x`.
  * In particular, if the cycle is reachable from the start node, then the iterator will not terminate.
  * Yet, it will still ensure that all reachable nodes eventually show up.
  */
class TreeIterator[A](children: A => Iterator[A]) {

  /** Returns an iterator over all nodes reachable from `x` via a possibly empty path in `children`.
    * Uses dove-tailing to support both infinitely branching and infinitely deep trees.
    * Callers must not rely on any particular order in which the nodes are returned.
    */
  def apply(x: A): Iterator[A] = new Iterator[A] {

    // INVARIANT: All Iterators in the work queue are non-empty,
    // i.e., next() can be called on them without chechking hasNext first.
    private[this] val work: scala.collection.mutable.Queue[Iterator[A]] =
      new scala.collection.mutable.Queue()
    discard(work.enqueue(Iterator(x)))

    // Since the work queue contains only non-empty iterators by the above invariant,
    // there are more elements iff the queue is non-empty.
    override def hasNext: Boolean = work.nonEmpty

    override def next(): A = {
      val nextIter = work.dequeue()
      val nextElem = nextIter.next()
      val childrenIter = children(nextElem)
      // Dove-tailing happens here: We alternate between the children and the siblings of the nodes.
      if (childrenIter.hasNext) {
        discard(work.enqueue(childrenIter))
      }
      if (nextIter.hasNext) {
        discard(work.enqueue(nextIter))
      }
      nextElem
    }
  }
}
