// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Wart such that we avoid using [[scala.concurrent.Future.andThen]]
  *
  * Using [[scala.concurrent.Future.andThen]] seems to be a bad idea
  * because it swallows the exceptions in the `andThen` part.
  *
  * Better use our own implementation in `Thereafter`.
  */
object FutureAndThen extends WartTraverser {

  val message =
    "Future.andThen swallows exceptions. Use e.g. the typeclass Thereafter with `.thereafter` or `.thereafterP` for partial functions."

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val futureTypeSymbol = typeOf[scala.concurrent.Future[?]].typeSymbol
    val andThenMethodName = TermName("andThen")

    new Traverser {
      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          case Select(receiver, methodName)
              if receiver.tpe.typeSymbol == futureTypeSymbol && methodName == andThenMethodName =>
            error(u)(tree.pos, message)
            super.traverse(tree)
          case _ =>
            super.traverse(tree)
        }
    }
  }
}
