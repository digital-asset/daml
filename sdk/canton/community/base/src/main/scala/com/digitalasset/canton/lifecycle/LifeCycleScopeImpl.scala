// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.Eval
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{LazyValWithContext, Thereafter, TracedLazyVal}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.util.{Failure, Success, Try}

/** Combines multiple [[LifeCycleManager]]s into a single scope such that
  *   - [[RunOnClosing]] tasks of the scope are run when the first manager closes.
  *   - [[HasSynchronizeWithClosing.synchronizeWithClosingF]] synchronizes with all the managers in
  *     the scope.
  */
private[lifecycle] final class LifeCycleScopeImpl(
    private[lifecycle] val managers: Set[LifeCycleManager]
) extends HasSynchronizeWithClosing
    with PrettyPrinting {

  import LifeCycleScopeImpl.*

  override protected def pretty: Pretty[LifeCycleScopeImpl] =
    prettyNode("LifeCycleScope", param("managers", _.managers.map(_.name.unquoted)))

  override def isClosing: Boolean = managers.exists(_.isClosing)

  override def runOnClose(task: RunOnClosing): UnlessShutdown[LifeCycleRegistrationHandle] = {
    import RunOnClosingLifeCycleState.*
    // Do not bake the state into the task so that the task can be garbage-collected after it has run
    // even if someone still clings to the handle
    val state = new AtomicReference[State](RunOnClosingLifeCycleState.Registering)
    val runOnClosingTask = new RunOnClosingLifeCycleScope(task, state)

    val handlesB = Seq.newBuilder[LifeCycleRegistrationHandle]
    val iter = managers.iterator

    // Register the task with all managers. Stop if one manager is already closing.
    val registered = iter
      .map { manager =>
        manager.runOnClose(runOnClosingTask).map(handle => handlesB += handle)
      }
      .takeWhile(_.isOutcome)
    val success = registered.size == managers.size
    val handles = handlesB.result()

    if (success && state.compareAndSet(Registering, ToBeInvoked(handles))) {
      Outcome(new LifeCycleScopeRegistrationHandle(state))
    } else {
      deregisterHandles(handles)
      AbortedDueToShutdown
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  override def synchronizeWithClosingF[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): UnlessShutdown[F[A]] = {
    type FF[X] = Try[UnlessShutdown[F[X]]]
    implicit val FF: Thereafter[FF] = ThereafterTryUnlessShutdownF.instance[F]

    def synchronizeWithManager(acc: Eval[FF[A]], manager: LifeCycleManager): Eval[FF[A]] =
      Eval.always(Try {
        manager.synchronizeWithClosingF(name)(acc.value)(traceContext, FF)
      } match {
        case Success(Outcome(fa)) => fa
        case Success(AbortedDueToShutdown) => Success(AbortedDueToShutdown)
        case Failure(ex) => Failure(ex)
      })

    val initial = Eval.always(Try(Outcome(f))): Eval[FF[A]]

    // This is not stack-safe! We just hope that a scope does not contain 10000s of managers.
    managers.foldLeft(initial)(synchronizeWithManager).value.get
  }

  override protected[this] def runTaskUnlessDone(task: RunOnClosing)(implicit
      traceContext: TraceContext
  ): Unit = if (!task.done) task.run()
}

private[lifecycle] object LifeCycleScopeImpl {

  /** The empty scope without managers. This is not very useful for synchronizing on, but is the
    * neutral element in the scope monoid defined by taking unions of [[LifeCycleManager]]s.
    */
  val empty: LifeCycleScopeImpl = new LifeCycleScopeImpl(Set.empty)

  private def deregisterHandles(handles: Seq[LifeCycleRegistrationHandle]): Unit =
    handles.foreach(_.cancel().discard[Boolean])

  /** The [[RunOnClosing]] job registered with all [[LifeCycleManager]]s of a
    * [[LifeCycleScopeImpl]].
    */
  private final class RunOnClosingLifeCycleScope(
      task: RunOnClosing,
      state: AtomicReference[RunOnClosingLifeCycleState.State],
  ) extends RunOnClosing {
    import RunOnClosingLifeCycleState.*

    override def name: String = task.name

    override def done: Boolean =
      if (task.done) {
        state.getAndUpdate {
          case Invoked => Invoked
          case _ => Aborted
        } match {
          case ToBeInvoked(handles) =>
            deregisterHandles(handles)
            true
          case Invoked =>
            // If the task has already been started, we must not report it as done any more.
            // Otherwise, it would be considered obsolete and removed by some of the life cycle managers.
            // And then their closing would no longer wait until the task has finished.
            false
          case Registering | Aborted => true
        }
      } else false

    /** Use lazy-val memoization to ensure that the task runs only once. This memoization would fail
      * if the task closes another [[LifeCycleManager]] in this [[LifeCycleScopeImpl scope]] and
      * this [[LifeCycleManager]] then called [[run]] again from the same thread, because lazy val
      * initializers are re-entrant. However, the [[LifeCycleManager]] executes the [[RunOnClosing]]
      * tasks in a separate future. So this is not a problem.
      */
    private val taskResult: LazyValWithContext[Try[Unit], TraceContext] =
      TracedLazyVal(implicit traceContext => Try(task.run()))

    @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
    override def run()(implicit traceContext: TraceContext): Unit =
      state.getAndUpdate {
        case ToBeInvoked(_) | Invoked => Invoked
        case Registering | Aborted => Aborted
      } match {
        case ToBeInvoked(handles) =>
          // This is the first invocation of run and no cancellation has happened.
          // We're responsible for running the task and deregistering the handles.
          val result = taskResult.get
          deregisterHandles(handles)
          result.get
        case Invoked =>
          // run was invoked previously. We synchronize via the lazy val.
          // Discard any exception because it was already propagated by the first invocation of run
          // and we should avoid flooding the logs with the same exception.
          taskResult.get.discard[Try[Unit]]
        case Registering | Aborted =>
      }
  }

  private[lifecycle] object RunOnClosingLifeCycleState {

    /** State machine for [[RunOnClosingLifeCycleScope]].
      *
      *   - The initial state is [[Registering]].
      *   - Cancellation can only happen after a successful registration. We therefore don't need
      *     such a transition for [[Registering]].
      *   - Prior to a successful registration, the registration call is responsible for
      *     deregistering the handles upon a failed registration. After successful registration, the
      *     transition out of [[ToBeInvoked]] must deregister the handles.
      *   - If the task is run, the deregistration of the handles must happen only after the task
      *     has finished. This ensures that all registered [[LifeCycleManager]]s will delay their
      *     closing until the task has finished.
      *   - The task may be run at most once.
      *
      * {{{
      *        ┌─────────────┐   successful       ┌─────────────┐
      *        │             │   registration     │             │
      *   ─────► Registering ├────────────────────► ToBeInvoked │
      *        │             │                    │             │
      *        └──────┬──────┘                    └───┬──┬──────┘
      *               │                               │  │
      *           run │                               │  │ run
      *               │          done                 │  │
      *          done │          cancel->true         │  │
      *               │  ┌────────────────────────────┘  │
      *               │  │                               │
      *               │  │                               │
      *   cancel      │  │                               │
      *  ->false      │  │                     run       │
      * ┌──────────┐  │  │                 ┌──────────┐  │
      * │ run      │  │  │                 │          │  │
      * │      ┌───▼──▼──▼───┐      cancel │      ┌───▼──▼──────┐
      * │      │             │     ->false │      │             │
      * └──────┤ Aborted     │             └──────┤ Invoked     │
      *   done │             │               done │             │
      *        └─────────────┘                    └─────────────┘
      * }}}
      */
    private[LifeCycleScopeImpl] sealed trait State extends Product with Serializable
    private[LifeCycleScopeImpl] case object Registering extends State
    private[LifeCycleScopeImpl] case object Aborted extends State
    private[LifeCycleScopeImpl] final case class ToBeInvoked(
        handles: Seq[LifeCycleRegistrationHandle]
    ) extends State
    private[LifeCycleScopeImpl] final case object Invoked extends State
  }

  private class LifeCycleScopeRegistrationHandle(
      state: AtomicReference[RunOnClosingLifeCycleState.State]
  ) extends LifeCycleRegistrationHandle {
    import RunOnClosingLifeCycleState.*

    override def cancel(): Boolean =
      state.getAndUpdate {
        case Invoked => Invoked
        case _ => Aborted
      } match {
        case ToBeInvoked(tokens) =>
          // The task has not yet run and was not observed as being done.
          // So we can cancel it now.
          deregisterHandles(tokens)
          true
        case Invoked | Aborted =>
          // The task has already been run or is already done or has already been cancelled or registration failed.
          // So this cancellation does nothing
          false
        case Registering =>
          // The handle is created only after a successful registration. So we cannot be in this state.
          throw new IllegalStateException(
            "Cancellation called before registration was successful."
          )
      }

    override def isScheduled: Boolean = state.get() match {
      case ToBeInvoked(_) => true
      case _ => false
    }
  }

  private[lifecycle] type ThereafterTryUnlessShutdownFContent[C[_], A] = Try[UnlessShutdown[C[A]]]

  @VisibleForTesting
  private[lifecycle] trait ThereafterTryUnlessShutdownF[F[_], C[_]]
      extends Thereafter[Lambda[a => Try[UnlessShutdown[F[a]]]]] {
    def F: Thereafter.Aux[F, C]
    override type Content[A] = ThereafterTryUnlessShutdownFContent[C, A]

    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    override def thereafter[A](
        x: Try[UnlessShutdown[F[A]]]
    )(body: Try[UnlessShutdown[C[A]]] => Unit): Try[UnlessShutdown[F[A]]] = x match {
      case Success(Outcome(fa)) =>
        Try(Outcome(F.thereafter(fa)(c => body(Success(Outcome(c))))))
      case other =>
        Thereafter[Try].thereafter(other.asInstanceOf[Try[UnlessShutdown[Nothing]]])(body)
    }

    override def maybeContent[A](content: Try[UnlessShutdown[C[A]]]): Option[A] =
      content match {
        case Success(Outcome(c)) => F.maybeContent(c)
        case _ => None
      }
  }

  object ThereafterTryUnlessShutdownF {
    def instance[F[_]](implicit FF: Thereafter[F]): Thereafter.Aux[
      Lambda[a => Try[UnlessShutdown[F[a]]]],
      ThereafterTryUnlessShutdownFContent[FF.Content, *],
    ] = new ThereafterTryUnlessShutdownF[F, FF.Content] {
      override def F: Thereafter.Aux[F, FF.Content] = FF
    }
  }
}
