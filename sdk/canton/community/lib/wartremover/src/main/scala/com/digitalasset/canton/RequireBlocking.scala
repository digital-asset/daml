// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** All blocking method calls should be wrapped in [[scala.concurrent.blocking]]
  * so that the execution context knows that it may have to spawn a new process.
  *
  * This wart checks that all @{code synchronized} calls are surrounded immediately by a [[scala.concurrent.blocking]] call.
  * It also complains about calls to [[java.lang.Thread]]`.sleep` as `Threading.sleep` should be used instead.
  *
  * The logic can currently be fooled by renaming `synchronized` and `sleep`.
  */
object RequireBlocking extends WartTraverser {

  val messageSynchronized: String = "synchronized blocks must be surrounded by blocking"
  val messageThreadSleep: String = "Use Threading.sleep instead of Thread.sleep"

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val synchronizedName = TermName("synchronized")
    val blockingFullName = "scala.concurrent.blocking"
    val sleepName = TermName("sleep")
    val threadFullName = "java.lang.Thread"

    new Traverser {
      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          // blocking(receiver.synchronized(body))
          case Apply(
                TypeApply(blocking, _tyarg1),
                List(
                  Apply(TypeApply(Select(receiver, `synchronizedName`), _tyarg2), List(body))
                ),
              ) if blocking.symbol.fullName == blockingFullName =>
            // Look for further synchronized blocks in the receiver and the body
            // Even if they are Syntactically inside a `blocking` call,
            // we still want to be conservative as further synchronized calls may escape the blocking call
            // due to lazy evaluation.
            //
            // This heuristics will give false positives on immediately nested synchronized blocks.
            traverse(receiver)
            traverse(body)
          case Select(_receiver, synchronizedN) if synchronizedN.toTermName == synchronizedName =>
            error(u)(tree.pos, messageSynchronized)
            super.traverse(tree)
          // Thread.sleep
          case Select(receiver, `sleepName`)
              if receiver.symbol != null && receiver.symbol.fullName == threadFullName =>
            error(u)(tree.pos, messageThreadSleep)
            super.traverse(tree)
          case _ =>
            super.traverse(tree)
        }
      }
    }
  }
}
