// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Wart such that we avoid using Try.failed
  *
  * Using Try.failed seems to be a bad idea because it will create an UnsupportedOperationException
  * for each Success, which constantly builds a stack trace and then throws it away. Seems to be quite
  * expensive in particular in tight loops.
  *
  * Better match on a Try. If you need failed.foreach, use TryUtil.forFailed instead.
  */
object TryFailed extends WartTraverser {

  val message = "(Try|Success).failed should not be used. Use e.g. .forFailed from TryUtil instead."

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val tryTypeSymbol = typeOf[scala.util.Try[?]].typeSymbol
    val successTypeSymbol = typeOf[scala.util.Success[?]].typeSymbol
    val failedMethodName = TermName("failed")

    new Traverser {
      override def traverse(tree: Tree): Unit = {
        tree match {
          case t if hasWartAnnotation(u)(t) => // Ignore trees marked by SuppressWarnings
          case Select(receiver, methodName)
              if (receiver.tpe.typeSymbol == tryTypeSymbol || receiver.tpe.typeSymbol == successTypeSymbol)
                && methodName == failedMethodName =>
            error(u)(tree.pos, message)
            super.traverse(tree)
          case _ =>
            super.traverse(tree)
        }
      }
    }

  }
}
