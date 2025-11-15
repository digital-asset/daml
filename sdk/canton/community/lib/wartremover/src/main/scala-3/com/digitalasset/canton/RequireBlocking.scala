// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.concurrent.blocking

/** All blocking method calls should be wrapped in [[scala.concurrent.blocking]] so that the
  * execution context knows that it may have to spawn a new process.
  *
  * This wart checks that all @{code synchronized} calls are surrounded immediately by a
  * [[scala.concurrent.blocking]] call. It also complains about calls to
  * [[java.lang.Thread]]`.sleep` as `Threading.sleep` should be used instead.
  *
  * The logic can currently be fooled by renaming `synchronized` and `sleep`.
  */
object RequireBlocking extends WartTraverser {

  val messageSynchronized: String = "synchronized blocks must be surrounded by blocking"
  val messageThreadSleep: String = "Use Threading.sleep instead of Thread.sleep"

  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*
      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else if tree.isExpr then
          tree.asExpr match
            case '{ blocking[t](($receiver: AnyRef).synchronized($body)) } =>
              traverseTree(receiver.asTerm)(owner)
              traverseTree(body.asTerm)(owner)
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
