// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Wart such that we avoid using [[scala.concurrent.Future.andThen]]
  *
  * Using [[scala.concurrent.Future.andThen]] seems to be a bad idea because it swallows the
  * exceptions in the `andThen` part.
  *
  * Better use our own implementation in `Thereafter`.
  */
object FutureAndThen extends WartTraverser {

  val message =
    "Future.andThen swallows exceptions. Use e.g. the typeclass Thereafter with `.thereafter` or `.thereafterP` for partial functions."

  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else
          if tree.isExpr then
            tree.asExpr match
              case '{ ($r: scala.concurrent.Future[t]).andThen($pf)(using $e) } =>
                error(tree.pos, message)
              case _ =>
          super.traverseTree(tree)(owner)
    }
}
