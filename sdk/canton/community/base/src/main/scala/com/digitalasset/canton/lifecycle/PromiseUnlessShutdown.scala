// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.Id
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.ref.WeakReference

object PromiseUnlessShutdown {

  /** Close the type abstraction of [[PromiseUnlessShutdown]].
    */
  private[lifecycle] def wrap[A](promise: Promise[UnlessShutdown[A]]): PromiseUnlessShutdown[A] = {
    type K[T[_]] = Id[T[A]]
    PromiseUnlessShutdownImpl.Instance.subst[K](promise)
  }

  /** Creates an unsupervised promise just like [[scala.concurrent.Promise.apply]] */
  def unsupervised[A](): PromiseUnlessShutdown[A] =
    wrap(Promise[UnlessShutdown[A]]())

  /** Creates a supervised promise. Supervision starts when the promises future
    * [[scala.concurrent.Promise.future]] is accessed for the first time.
    */
  def supervised[A](
      description: String,
      futureSupervisor: FutureSupervisor,
      logAfter: Duration = 10.seconds,
      logLevel: Level = Level.DEBUG,
  )(implicit ecl: ErrorLoggingContext): PromiseUnlessShutdown[A] = {
    val supervisedPromise =
      new SupervisedPromise[UnlessShutdown[A]](description, futureSupervisor, logAfter, logLevel)
    wrap(supervisedPromise)
  }

  /** Creates a supervised promise that will be completed with
    * [[UnlessShutdown.AbortedDueToShutdown]] when the `onShutdownRunner` is closed, unless the
    * promise had been completed before.
    */
  def abortOnShutdown[A](
      description: String,
      onShutdownRunner: OnShutdownRunner,
      futureSupervisor: FutureSupervisor,
      logAfter: Duration = 10.seconds,
      logLevel: Level = Level.DEBUG,
  )(implicit ecl: ErrorLoggingContext): PromiseUnlessShutdown[A] = {
    val promise = supervised[A](description, futureSupervisor, logAfter, logLevel)
    val abortRunner = new AbortPromiseOnShutdown(description, WeakReference(promise))
    onShutdownRunner.runOnShutdown_(abortRunner)(ecl.traceContext)
    // We do not need to cancel the registration when the promise is completed
    // because the abort runner will report itself as done and then be cleaned up by the onShutdownRunner anyway.
    promise
  }

  /** Completes the given promise with [[UnlessShutdown.AbortedDueToShutdown]].
    * @param promiseRef
    *   A weak reference to the promise to complete. We use a weak reference for two reasons here:
    *   1. We do not want to prevent the completed promise (and the possibly large value it
    *      contains) from being garbage collected as long as this object is still registered for
    *      being run on shutdown.
    *   1. If the promise is created, but never completed and no longer referenced anywhere by the
    *      user code, then there is no longer any point to complete the promise. The weak reference
    *      allows the GC to collect such promises. This runner will then mark itself as `done` and
    *      be eventually removed from the onShutdownRunner, avoiding a memory leak due to
    *      never-completed promises.
    */
  @VisibleForTesting
  private[lifecycle] final class AbortPromiseOnShutdown(
      override val name: String,
      @VisibleForTesting private[lifecycle] val promiseRef: WeakReference[PromiseUnlessShutdown[?]],
  ) extends RunOnShutdown {
    override def done: Boolean = promiseRef match {
      case WeakReference(promise) => promise.isCompleted
      case _ => true
    }

    override def run(): Unit = WeakReference.unapply(promiseRef).foreach(_.shutdown())
  }
}

/** Combines a [[scala.concurrent.Promise]] with [[UnlessShutdown]].
  *
  * We avoid wrapping and unwrapping it by emulating Scala 3's opaque types.
  */
sealed abstract class PromiseUnlessShutdownImpl {

  /** The abstract type of a [[scala.concurrent.Promise]] containing a [[UnlessShutdown]]. The
    * canonical name for this type would be `T`, but `PromiseUnlessShutdown` gives better error
    * messages.
    */
  type PromiseUnlessShutdown[A] <: Promise[UnlessShutdown[A]]

  /** Methods to evidence that [[PromiseUnlessShutdown]] and
    * [[scala.concurrent.Promise]]`[`[[UnlessShutdown]]`]` can be replaced in any type context `K`.
    */
  private[lifecycle] def subst[K[_[_]]](
      ff: K[Lambda[a => Promise[UnlessShutdown[a]]]]
  ): K[PromiseUnlessShutdown]
  // Technically, we could implement `unsubst` using `subst`, but it may be clearer if we make both directions explicit.
  private[lifecycle] def unsubst[K[_[_]]](
      ff: K[PromiseUnlessShutdown]
  ): K[Lambda[a => Promise[UnlessShutdown[a]]]]
}

object PromiseUnlessShutdownImpl {
  val Instance: PromiseUnlessShutdownImpl = new PromiseUnlessShutdownImpl {
    override type PromiseUnlessShutdown[A] = Promise[UnlessShutdown[A]]

    final override private[lifecycle] def subst[K[_[_]]](
        ff: K[Lambda[a => Promise[UnlessShutdown[a]]]]
    ): K[PromiseUnlessShutdown] = ff

    final override private[lifecycle] def unsubst[K[_[_]]](
        ff: K[PromiseUnlessShutdown]
    ): K[Lambda[a => Promise[UnlessShutdown[a]]]] = ff
  }

  /** Extension methods for [[PromiseUnlessShutdown]]
    * @tparam X
    *   Type parameter to make the type of `self` visible to the Scala compiler when it is more
    *   specific than [[PromiseUnlessShutdown]]`[A]`, e.g., for methods returning `self.type`.
    */
  final class Ops[A, X <: PromiseUnlessShutdown[A]](val self: X) extends AnyVal {

    /** Open the type abstraction */
    def unwrap: Promise[UnlessShutdown[A]] = {
      type K[T[_]] = Id[T[A]]
      Instance.unsubst[K](self)
    }

    /** Analog to [[scala.concurrent.Promise.completeWith]] */
    def completeWithUS(other: FutureUnlessShutdown[A]): X =
      Instance
        .subst[FreeCompleteWith](freeCompleteWith)
        .completeWith(self, other.unwrap)

    /** Analog to [[scala.concurrent.Promise.future]] */
    def futureUS: FutureUnlessShutdown[A] = FutureUnlessShutdown(self.future)

    /** Tries to complete the promise with `value` as a [[UnlessShutdown.Outcome]]. Does nothing if
      * the promise has already been completed.
      */
    def outcome(value: A): Unit =
      self.trySuccess(UnlessShutdown.Outcome(value)).discard[Boolean]

    /** Tries to complete the promise with [[UnlessShutdown.AbortedDueToShutdown]]. Does nothing if
      * the promise has already been completed.
      */
    def shutdown(): Unit =
      self.trySuccess(UnlessShutdown.AbortedDueToShutdown).discard[Boolean]
  }
  // Use `implicit def` instead of `implicit class` so that self.type is inferred for the Ops type parameter.
  import scala.language.implicitConversions
  implicit def ops[A](self: PromiseUnlessShutdown[A]): Ops[A, self.type] =
    new Ops[A, self.type](self)

  /** Helper trait to define the type context for `subst` to be used to lift
    * [[scala.concurrent.Promise.completeWith]].
    */
  private sealed trait FreeCompleteWith[F[_]] {
    def completeWith[A, X <: F[A]](
        self: X,
        other: Future[UnlessShutdown[A]],
    ): X
  }
  private object freeCompleteWith
      extends FreeCompleteWith[Lambda[a => Promise[UnlessShutdown[a]]]] {
    override final def completeWith[B, X <: Promise[UnlessShutdown[B]]](
        self: X,
        other: Future[UnlessShutdown[B]],
    ): X =
      self.completeWith(other)
  }

}
