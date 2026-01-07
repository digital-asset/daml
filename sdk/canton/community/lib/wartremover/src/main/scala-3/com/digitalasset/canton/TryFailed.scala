// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Wart such that we avoid using Try.failed
  *
  * Using Try.failed seems to be a bad idea because it will create an UnsupportedOperationException
  * for each Success, which constantly builds a stack trace and then throws it away. Seems to be
  * quite expensive in particular in tight loops.
  *
  * Better match on a Try. If you need failed.foreach, use TryUtil.forFailed instead.
  */
object TryFailed extends WartTraverser {

  val message = "(Try|Success).failed should not be used. Use e.g. .forFailed from TryUtil instead."

  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          if tree.isExpr then
            tree.asExpr match
              case '{ ($r: scala.util.Try[t]).failed } => error(tree.pos, message)
              case '{ ($r: scala.util.Success[t]).failed } => error(tree.pos, message)
              case _ => ()
          super.traverseTree(tree)(owner)
    }
}
