// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

/** Use Mutex.exclusive instead of blocking synchronized
  *
  * This wart checks that @{code synchronized} is not used directly, but only through
  * Mutex.exclusive which provides locking and thread compensation, but avoids leaking threads due
  * to too many calls to `blocking`
  *
  * Also, fails on [[java.lang.Thread]]`.sleep` as `Threading.sleep` should be used instead.
  *
  * The logic can currently be fooled by renaming `synchronized` and `sleep`.
  */
object RequireBlocking extends WartTraverser {

  val messageSynchronized: String = s"Do not use synchronized. Use Mutex.exclusive instead."
  val messageThreadSleep: String = "Use Threading.sleep instead of Thread.sleep"

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val synchronizedName = TermName("synchronized")
    val sleepName = TermName("sleep")
    val threadFullName = "java.lang.Thread"

    new Traverser {
      override def traverse(tree: Tree): Unit =
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
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
