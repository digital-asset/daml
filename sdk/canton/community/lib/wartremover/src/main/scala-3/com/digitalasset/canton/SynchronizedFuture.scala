// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.quoted.Type

/** When a `synchronized` block returns a [[scala.concurrent.Future]], then the synchronization
  * typically does not extend to the computations performed inside the future. This often hides a
  * concurrency bug that is hard to spot during review because the computation's code is lexically
  * inside the synchronized block and it may not be obvious that [[scala.concurrent.Future]]s are in
  * play.
  *
  * For example, `synchronized` blocks are often used to read and update the state of an object
  * atomically. When the update operation involves a [[scala.concurrent.Future]] like below, then
  * the write to the state is actually not guarded by the synchronized call and the update is not
  * atomic.
  *
  * {{{
  *   synchronized {
  *     val x = this.atomicRef.get()
  *     futureComputation(x).map { y =>
  *       this.atomicRef.set(y)
  *       y
  *     }
  *   }
  * }}}
  *
  * The proper approach in the above example is to use a semaphore instead of a `synchronized`
  * block.
  *
  * {{{
  *   blocking { this.semaphore.acquire() }
  *   val x = this.atomicRef.get()
  *   futureComputation(x).map { y =>
  *     this.atomicRef.set(y)
  *     y
  *   }.thereafter { _ => this.semaphore.release() }
  * }}}
  *
  * If the synchronization intentionally does not have to extend over the
  * [[scala.concurrent.Future]] computation, it usually helps with readability to move the future
  * out of the synchronized block. For example,
  *
  * {{{
  *   blocking(synchronized {
  *     val x = criticalSection()
  *     Future { nonCriticalSection(x) }
  *   })
  * }}}
  *
  * should be written as follows:
  *
  * {{{
  *   val x = blocking(synchronized { criticalSection() })
  *   Future { nonCriticalSection(x) }
  * }}}
  *
  * There are cases where the `synchronized` block is supposed to return a
  * [[scala.concurrent.Future]], for example when dealing with the promise of a future. In such a
  * case, the warning should simply be suppressed locally.
  */
object SynchronizedFuture extends WartTraverser {

  val messageSynchronized: String = "synchronized blocks must not return a Future"

  def apply(u: WartUniverse): u.Traverser =
    new u.Traverser(this) {
      import q.reflect.*

      override def traverseTree(tree: Tree)(owner: Symbol): Unit =
        if hasWartAnnotation(tree) then () // Ignore trees marked by SuppressWarnings
        else if tree.isExpr then
          tree.asExpr match
            case '{ ($receiver: AnyRef).synchronized[tpe]($body) } =>
              if isFutureLike[tpe] then error(tree.pos, messageSynchronized)
              traverseTree(receiver.asTerm)(owner)
              traverseTree(body.asTerm)(owner)
            case _ => super.traverseTree(tree)(owner)
        else super.traverseTree(tree)(owner)

      private val futureLikeTester =
        FutureLikeTester.tester[DoNotReturnFromSynchronizedLikeFuture](u)
      private def isFutureLike[T: Type] = futureLikeTester(TypeRepr.of[T])
    }
}
