// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.wartremover.{WartTraverser, WartUniverse}

import scala.annotation.StaticAnnotation

/** When a `synchronized` block returns a [[scala.concurrent.Future]], then the synchronization typically
  * does not extend to the computations performed inside the future.
  * This often hides a concurrency bug that is hard to spot during review
  * because the computation's code is lexically inside the synchronized block
  * and it may not be obvious that [[scala.concurrent.Future]]s are in play.
  *
  * For example, `synchronized` blocks are often used to read and update the state of an object
  * atomically. When the update operation involves a [[scala.concurrent.Future]] like below,
  * then the write to the state is actually not guarded by the synchronized call and the update is not atomic.
  *
  * <pre>
  *   synchronized {
  *     val x = this.atomicRef.get()
  *     futureComputation(x).map { y =>
  *       this.atomicRef.set(y)
  *       y
  *     }
  *   }
  * </pre>
  *
  * The proper approach in the above example is to use a semaphore instead of a `synchronized` block.
  *
  * <pre>
  *   blocking { this.semaphore.acquire() }
  *   val x = this.atomicRef.get()
  *   futureComputation(x).map { y =>
  *     this.atomicRef.set(y)
  *     y
  *   }.thereafter { _ => this.semaphore.release() }
  * </pre>
  *
  * If the synchronization intentionally does not have to extend over the [[scala.concurrent.Future]] computation,
  * it usually helps with readability to move the future out of the synchronized block. For example,
  *
  * <pre>
  *   blocking(synchronized {
  *     val x = criticalSection()
  *     Future { nonCriticalSection(x) }
  *   })
  * </pre>
  *
  * should be written as follows:
  *
  * <pre>
  *   val x = blocking(synchronized { criticalSection() })
  *   Future { nonCriticalSection(x) }
  * </pre>
  *
  * There are cases where the `synchronized` block is supposed to return a [[scala.concurrent.Future]],
  * for example when dealing with the promise of a future. In such a case, the warning should simply be suppressed locally.
  */
object SynchronizedFuture extends WartTraverser {

  val messageSynchronized: String = "synchronized blocks must not return a Future"

  def apply(u: WartUniverse): u.Traverser = {
    import u.universe.*

    val synchronizedName = TermName("synchronized")

    val futureLikeType = typeOf[DoNotReturnFromSynchronizedLikeFuture]
    val futureLikeTester = FutureLikeTester.tester(u)(futureLikeType)

    new Traverser {

      override def traverse(tree: Tree): Unit = {
        tree match {
          // Ignore trees marked by SuppressWarnings
          case t if hasWartAnnotation(u)(t) =>
          case Apply(TypeApply(Select(receiver, `synchronizedName`), _tyarg2), List(body)) =>
            val tpe = tree.tpe
            if (tpe != null && futureLikeTester(tpe.dealias)) {
              error(u)(tree.pos, messageSynchronized)
            } else {
              // Keep looking for other instances in the receiver and the body
              traverse(receiver)
              traverse(body)
            }
          case _ =>
            super.traverse(tree)
        }
      }
    }
  }
}

/** Annotated type constructors will be treated like a [[scala.concurrent.Future]]
  * when looking at the return types of synchronized blocks.
  */
final class DoNotReturnFromSynchronizedLikeFuture extends StaticAnnotation
