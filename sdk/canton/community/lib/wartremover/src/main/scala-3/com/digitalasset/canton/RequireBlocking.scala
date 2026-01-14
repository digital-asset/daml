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

  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else if tree.isExpr then
          tree.asExpr match
            case '{ ($receiver: AnyRef).synchronized($body) } =>
              error(tree.pos, messageSynchronized)
              traverseTree(receiver.asTerm)(owner)
              traverseTree(body.asTerm)(owner)
            case '{ Thread.sleep($_ : Long) } | '{ Thread.sleep($_ : Long, ${ _ }: Int) } =>
              error(tree.pos, messageThreadSleep)
            case _ => super.traverseTree(tree)(owner)
        else super.traverseTree(tree)(owner)
    }
}
