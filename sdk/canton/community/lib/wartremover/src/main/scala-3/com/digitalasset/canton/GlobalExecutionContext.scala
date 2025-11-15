// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.concurrent.ExecutionContext

/** The global execution context [[scala.concurrent.ExecutionContext.global]] should be avoided
  * because it is integrated with neither of the following:
  *   - Canton's error reporting
  *   - execution context supervision
  *   - thread count configuration
  *   - the workarounds for the bugs in the ForkJoinPool Moreover, if it is used in tests, the tests
  *     may interfere in unexpected ways.
  *
  * For unit tests, mix in the traits `com.daml.resources.HasExecutionContext` or
  * `com.digitalasset.canton.HasExecutorService`, which provide a suitably configured
  * [[scala.concurrent.ExecutionContext]]. For integration tests, an execution context should
  * already be in scope. In production code, properly pass the right
  * [[scala.concurrent.ExecutionContext]] into where it is needed. Create a new one using
  * `com.digitalasset.canton.concurrent.Threading` if absolutely necessary.
  *
  * For similar reasons, the [[scala.concurrent.ExecutionContext.parasitic]] should not be used. Use
  * `com.digitalasset.canton.concurrent.DirectExecutionContext` instead.
  *
  * [[scala.concurrent.ExecutionContext.opportunistic]] should be avoided as well, but given that
  * [[scala.concurrent.ExecutionContext.opportunistic]] is private to the scala package, this wart
  * does not check for it.
  */
object GlobalExecutionContext extends WartTraverser {

  val messageGlobal: String = "The global execution context should not be used."
  val messageParasitic: String =
    "Use a DirectExecutionContext instead of ExecutionContext.parasitic."

  @SuppressWarnings(Array("org.wartremover.warts.GlobalExecutionContext"))
  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else if tree.isExpr then
          tree.asExpr match
            case '{ ExecutionContext.global } => error(tree.pos, messageGlobal)
            case '{ ExecutionContext.Implicits.global } => error(tree.pos, messageGlobal)
            case '{ ExecutionContext.parasitic } => error(tree.pos, messageParasitic)
            case _ => super.traverseTree(tree)(owner)
        else super.traverseTree(tree)(owner)
    }

}
