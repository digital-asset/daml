// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** The global execution context [[scala.concurrent.ExecutionContext.global]] should be avoided
  * because it is integrated with neither of the following:
  * - Canton's error reporting
  * - execution context supervision
  * - thread count configuration
  * - the workarounds for the bugs in the ForkJoinPool
  * Moreover, if it is used in tests, the tests may interfere in unexpected ways.
  *
  * For unit tests, mix in the traits `com.daml.resources.HasExecutionContext` or `com.digitalasset.canton.HasExecutorService`,
  * which provide a suitably configured [[scala.concurrent.ExecutionContext]].
  * For integration tests, an execution context should already be in scope.
  * In production code, properly pass the right [[scala.concurrent.ExecutionContext]] into where it is needed.
  * Create a new one using `com.digitalasset.canton.concurrent.Threading` if absolutely necessary.
  *
  * For similar reasons, the [[scala.concurrent.ExecutionContext.parasitic]] should not be used.
  * Use `com.digitalasset.canton.concurrent.DirectExecutionContext` instead.
  *
  * [[scala.concurrent.ExecutionContext.opportunistic]] should be avoided as well,
  * but given that [[scala.concurrent.ExecutionContext.opportunistic]] is private to the scala package,
  * this wart does not check for it.
  */
object GlobalExecutionContext extends WartTraverser {

  val messageGlobal: String = "The global execution context should not be used."
  val messageParasitic: String =
    "Use a DirectExecutionContext instead of ExecutionContext.parasitic."

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val globalName = TermName("global")
    val parasiticName = TermName("parasitic")
    val executionContextName = "scala.concurrent.ExecutionContext"
    val executionContextImplicitsName = "scala.concurrent.ExecutionContext.Implicits"

    new Traverser {
      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          // scala.concurrent.ExecutionContext.global and scala.concurrent.ExecutionContext.Implicits.global
          case Select(receiver, `globalName`)
              if receiver.symbol != null &&
                (receiver.symbol.fullName == executionContextName || receiver.symbol.fullName == executionContextImplicitsName) =>
            error(u)(tree.pos, messageGlobal)
            super.traverse(tree)
          // scala.concurrent.ExecutionContext.parasitic
          case Select(receiver, `parasiticName`)
              if receiver.symbol != null && receiver.symbol.fullName == executionContextName =>
            error(u)(tree.pos, messageParasitic)
            super.traverse(tree)
          case _ =>
            super.traverse(tree)
        }
      }
    }
  }

}
