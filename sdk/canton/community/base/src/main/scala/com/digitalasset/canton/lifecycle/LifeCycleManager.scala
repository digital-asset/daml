// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.LifeCycleManager.LifeCycleManagerImpl
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.*
import com.digitalasset.canton.util.TwoPhasePriorityAccumulator.Priority
import com.digitalasset.canton.util.{
  ErrorUtil,
  SingleUseCell,
  Thereafter,
  TwoPhasePriorityAccumulator,
}

import java.util.concurrent.Semaphore
import scala.annotation.tailrec
import scala.collection.BufferedIterator
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

trait LifeCycleRegistrationHandle {
  def cancel(): Boolean
  def isScheduled: Boolean
}

trait HasRunOnClosing {

  /** Returns whether the component is closing or has already been closed. No new tasks can be added
    * with [[runOnClose]] during closing.
    */
  def isClosing: Boolean

  /** Schedules the given task to be run upon closing.
    *
    * @return
    *   An [[com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome]] indicates that the task will
    *   have been run when the `LifeCycleManager`'s `closeAsync` method completes or when
    *   `AutoCloseable`'s `close` method returns, unless the returned `LifeCycleRegistrationHandle`
    *   was used to cancel the task or the task has been done beforehand.
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if the task is not
    *   run due to closing. This always happens if [[isClosing]] returns true.
    */
  def runOnClose(task: RunOnClosing): UnlessShutdown[LifeCycleRegistrationHandle]

  /** Register a task to run when closing is initiated, or run it immediately if closing is already
    * ongoing. Unlike [[runOnClose]], this method does not guarantee that this task will have run by
    * the time the `LifeCycleManager`'s `closeAsync` method completes or `AutoCloseable`'s `close`
    * returns. This is because the task is run immediately if the component has already been closed.
    */
  def runOnOrAfterClose(
      task: RunOnClosing
  )(implicit traceContext: TraceContext): LifeCycleRegistrationHandle =
    runOnClose(task).onShutdown {
      runTaskUnlessDone(task)
      LifeCycleManager.DummyHandle
    }

  /** Variant of [[runOnOrAfterClose]] that does not return a
    * [[com.digitalasset.canton.lifecycle.LifeCycleRegistrationHandle]].
    */
  final def runOnOrAfterClose_(task: RunOnClosing)(implicit traceContext: TraceContext): Unit =
    runOnOrAfterClose(task).discard

  protected[this] def runTaskUnlessDone(task: RunOnClosing)(implicit
      traceContext: TraceContext
  ): Unit
}

trait HasSynchronizeWithClosing extends HasRunOnClosing {

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has finished or the
    * [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def synchronizeWithClosing[A](name: String)(f: => A)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[A] =
    synchronizeWithClosingF(name)(Try(f)).map(_.get)

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the
    * [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
    *
    * @return
    *   The computation completes with
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise it is the result of running `f`.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosingUSF[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
      A: AbsorbUnlessShutdown[F],
  ): F[A] = A.absorbOuter(synchronizeWithClosingF(name)(f))

  /** Runs the computation `f` only if the component is not yet closing. If so, the component will
    * delay releasing its resources until `f` has completed (as defined by the
    * [[com.digitalasset.canton.util.Thereafter]] instance) or the
    * [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if `f` has not
    *   run. Otherwise the result of running `f`.
    *
    * @see
    *   HasRunOnClosing.isClosing
    */
  def synchronizeWithClosingF[F[_], A](name: String)(f: => F[A])(implicit
      traceContext: TraceContext,
      F: Thereafter[F],
  ): UnlessShutdown[F[A]]
}

/** Ensures orderly shutdown for a set of [[LifeCycleManager.ManagedResource]]s and dependent
  * [[LifeCycleManager]]s. This creates a shutdown hierarchy.
  *
  * When a [[LifeCycleManager]] is closed with [[LifeCycleManager.closeAsync]], the following
  * shutdown procedure is applied:
  *   1. '''Close signal propagation''': The [[LifeCycleManager]] and all its dependent
  *      [[LifeCycleManager]]s are notified that closing is in progress. Thereafter, their
  *      [[HasRunOnClosing.isClosing]] returns true. Transitive dependents are also notified, but
  *      this need not be synchronized with the next step.
  *   1. '''Run on closing''': The [[RunOnClosing]] tasks registered with the [[LifeCycleManager]].
  *      Exceptions from the [[RunOnClosing]] tasks are logged and then discarded. The dependent
  *      [[LifeCycleManager]]s also start running their [[RunOnClosing]] tasks.
  *   1. '''Synchronize with closing''': The [[LifeCycleManager]] waits until all its
  *      [[HasSynchronizeWithClosing.synchronizeWithClosing]] computations have finished or until
  *      [[LifeCycleManager.synchronizeWithClosingPatience]] has elapsed.
  *   1. '''Release''': The [[LifeCycleManager]] releases all its
  *      [[LifeCycleManager.ManagedResource]]s and instructs dependent [[LifeCycleManager]]s to do
  *      so in ascending priority order. Within the same priority, releasing is unordered and
  *      possibly in parallel. When a dependent [[LifeCycleManager]] is instructed, it waits until
  *      step 2 has finished and then performs steps 3 and 4 of this procedure.
  *
  * Exceptions thrown in the last step "release" are propagated as a [[ShutdownFailedException]]
  * with proper exception chaining. If the third step completes because the patience has elapsed and
  * some [[HasSynchronizeWithClosing.synchronizeWithClosing]] computations are still running at the
  * end of the last step, a [[ShutdownFailedException]] signals such a synchronization failure.
  *
  * Rationale for this exception handling policy:
  *   - When an exception happens in the last step, it is unknown whether all dependencies have
  *     successfully finished all their business. So we must signal to the caller that the
  *     unexpected happened.
  *   - When some [[HasSynchronizeWithClosing.synchronizeWithClosing]] is still running after
  *     everything has been released, we cannot guarantee that all side effects guarded by this
  *     shutdown hierarchy are over. We signal this to the caller via an exception.
  *   - In contrast, [[RunOnClosing]] tasks are meant to quickly clean up stuff upon closing. They
  *     are not synchronized with closing, and therefore we merely log exceptions, but do not
  *     propagate them.
  */
sealed trait LifeCycleManager extends HasSynchronizeWithClosing { self =>
  def name: String

  /** If a token is returned, the task will have run before the [[LifeCycleManager]]'s `closeAsync`
    * method returns.
    */
  override final def runOnClose(
      task: RunOnClosing
  ): UnlessShutdown[LifeCycleRegistrationHandle] =
    register(task, LifeCycleManagerImpl.runOnClosingPriority) match {
      case Right(handle) => Outcome(handle)
      case Left(_) => AbortedDueToShutdown
    }

  /** Registers a [[LifeCycleManager.ManagedResource]] with this [[LifeCycleManager]] so that it
    * will be released as part of the last step of the shutdown procedure, as part of the given
    * priority group.
    *
    * @return
    *   [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] if the
    *   [[LifeCycleManager]] has already been closed and therefore it will not close the
    *   [[LifeCycleManager.ManagedResource]]. Otherwise, the returned
    *   [[LifeCycleRegistrationHandle]] allows to deregister the resource again.
    */
  def registerManaged(
      managed: LifeCycleManager.ManagedResource,
      priority: Short = 0,
  ): UnlessShutdown[LifeCycleRegistrationHandle] =
    register(managed, priority.toInt) match {
      case Right(handle) => Outcome(handle)
      case Left(_) => AbortedDueToShutdown
    }

  /** Internal implementation method for [[runOnClose]] and [[registerManaged]].
    *
    * @return
    *   The closing [[com.digitalasset.canton.tracing.TraceContext]] if registration fails because
    *   the [[LifeCycleManager]] is already closing.
    */
  protected def register(
      task: LifeCycleManager.LifeCycleManagerTask,
      priority: Int,
  ): Either[TraceContext, LifeCycleRegistrationHandle]

  /** Closes the [[LifeCycleManager]] and alls its registered [[LifeCycleManager.ManagedResource]]s.
    *
    * May be called multiple times, but subsequent calls merely return the same future as the first
    * call.
    */
  def closeAsync()(implicit traceContext: TraceContext): Future[Unit]

  def synchronizeWithClosingPatience: FiniteDuration
}

object LifeCycleManager {

  /** Creates a [[LifeCycleManager]] that is at the very root of the overall shutdown hierarchy */
  def root(
      name: String,
      synchronizeWithClosingPatience: FiniteDuration,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): LifeCycleManager =
    new LifeCycleManagerImpl(name, None, synchronizeWithClosingPatience, loggerFactory, ec)

  /** Creates a [[LifeCycleManager]] that registers itself as a direct dependency to `parent`. As
    * such, it will be closed automatically when the `parent` closes in the given priority group,
    * but it can still be closed independently by invoking [[LifeCycleManager.closeAsync]].
    */
  def dependent(
      name: String,
      parent: LifeCycleManager,
      synchronizeWithClosingPatience: FiniteDuration,
      loggerFactory: NamedLoggerFactory,
      parentPriority: Short = 0,
  )(implicit ec: ExecutionContext): LifeCycleManager =
    new LifeCycleManagerImpl(
      name,
      Some(parent -> parentPriority),
      synchronizeWithClosingPatience,
      loggerFactory,
      ec,
    )

  sealed trait LifeCycleManagerTask

  private object LifeCycleManagerTask {
    def isObsolete(task: LifeCycleManagerTask): Boolean = task match {
      case runOnClosing: RunOnClosing => runOnClosing.done
      case _: ManagedResource => false
    }
  }

  /** The implementation of [[LifeCycleManager]]. It lives in a separate class so that the public
    * and protected API is fully captured in [[LifeCycleManager]] and all private members in this
    * class are accessed only from `this`.
    *
    * Closing for a [[LifeCycleManager.root]] manager is simple: [[LifeCycleManager.closeAsync]]
    * merely goes through the prescribed protocol. For a [[LifeCycleManager.dependent]] manager,
    * this manager registers itself via several [[LifeCycleManagerTask]]s with the `parent` manager
    * as part of the constructor. This then leads to three different possible paths through closing:
    *   1. An explicit call to [[LifeCycleManager.closeAsync]]. This is only possible once the
    *      manager has been fully constructed (as no reference to `this` should escape from the
    *      constructor).
    *   1. An invocation of the registered [[LifeCycleManagerTask]]s by the parent manager.
    *   1. A failure to register the [[LifeCycleManagerTask]]s with the `parent` manager because the
    *      `parent` manager has already been closed. In this case, this manager closes itself
    *      immediately during construction, too.
    *
    * The first path to set the closing flag is the one responsible for fully carrying out the
    * shutdown procedure. The other paths merely synchronize with this one to the extent required.
    * Note that all paths can interleave due to concurrent invocations except for 1 and 3.
    */
  private final class LifeCycleManagerImpl(
      override val name: String,
      parent: Option[(LifeCycleManager, Short)],
      override val synchronizeWithClosingPatience: FiniteDuration,
      override protected val loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ) extends LifeCycleManager
      with NamedLogging {
    self =>

    import LifeCycleManagerImpl.*

    private[this] val directExecutionContext: DirectExecutionContext =
      DirectExecutionContext(noTracingLogger)

    private[this] val tasks =
      new TwoPhasePriorityAccumulator[LifeCycleManagerTask, CloseInfo](
        // When new tasks are registered, automatically remove obsolete RunOnClosing tasks
        // to avoid memory leaks
        obsoleteO = Some(LifeCycleManagerTask.isObsolete)
      )

    private[this] val parentHandles: Option[ParentHandles] =
      parent.flatMap { case (parentManager, priority) =>
        // We register three types of handlers with the parent lifecycle manager:
        //
        //   1. The close signal handler
        //   2. The runOnClosing signal handler
        //   3. The release signal handler
        //
        // If any of these registrations fails, the parent manager has already been closed.
        // So this manager should also close. Yet, it is not clear whether any of the
        // already registered handlers has already been run. So we just deregister all
        // of the handlers and then close this manager explicitly. Since all this happens
        // in the constructor of this manager, we know that no tasks have been registered
        // with this manager. So we don't have to worry about executing any such task
        // and the synchronization guarantees they are given. In particular, we can simply
        // cancel all registrations and thus bypass the shutdown protocol.
        //
        // We explicitly manage those handles and explicitly deregister them when expected.
        // In particular, we never mark them as done so that we do not have to worry about
        // them being removed from the parent manager concurrently.
        val onCloseSignalHandleE =
          parentManager.register(new CloseSignalPropagationTask, closeSignalPropagationPriority)
        onCloseSignalHandleE match {
          case Right(onCloseSignalHandle) =>
            val runOnClosingTask = new RunOnClosingTask
            val onRunOnClosingSignalHandleE =
              parentManager.register(runOnClosingTask, runOnClosingPriority)
            onRunOnClosingSignalHandleE match {
              case Right(onRunOnClosingSignalHandle) =>
                val closeByParent = new ReleaseByParentManagerTask
                val closeByParentHandleE = parentManager.register(closeByParent, priority.toInt)
                closeByParentHandleE match {
                  case Right(closeByParentHandle) =>
                    Some(
                      ParentHandles(
                        onCloseSignal = onCloseSignalHandle,
                        onRunOnClosingSignal = onRunOnClosingSignalHandle,
                        onReleaseByParentSignal = closeByParentHandle,
                      )
                    )
                  case Left(closeTraceContext) =>
                    onCloseSignalHandle.cancel().discard[Boolean]
                    onRunOnClosingSignalHandle.cancel().discard[Boolean]
                    closeFromConstructor()(closeTraceContext)
                    None
                }
              case Left(closeTraceContext) =>
                onCloseSignalHandle.cancel().discard[Boolean]
                closeFromConstructor()(closeTraceContext)
                None
            }
          case Left(closeTraceContext) =>
            closeFromConstructor()(closeTraceContext)
            None
        }
      }

    override def isClosing: Boolean = !tasks.isAccumulating

    override protected def register(
        task: LifeCycleManagerTask,
        priority: Int,
    ): Either[TraceContext, LifeCycleRegistrationHandle] =
      tasks
        .accumulate(task, priority)
        .bimap(_.traceContext, handle => new LifeCycleRegistrationHandleImpl(handle))

    override def synchronizeWithClosingF[F[_], A](name: String)(f: => F[A])(implicit
        traceContext: TraceContext,
        F: Thereafter[F],
    ): UnlessShutdown[F[A]] =
      if (!addReader(name)) {
        logger.debug(s"Won't run the task '$name' as the manager ${this.name} is closing")
        UnlessShutdown.AbortedDueToShutdown
      } else {
        val fa = Try(f) match {
          case Success(fa) =>
            fa.thereafter { _ =>
              removeReader(name)
            }
          case Failure(error) =>
            removeReader(name)
            throw error
        }
        UnlessShutdown.Outcome(fa)
      }

    override def closeAsync()(implicit traceContext: TraceContext): Future[Unit] =
      withCloseTraceContext { implicit traceContext =>
        val closingInfo = CloseInfo(traceContext, ExplicitlyClosing)
        // DECISION POINT for which shutdown path is responsible for the shutdown procedure
        val prev = tasks.stopAccumulating(closingInfo)
        prev match {
          case None =>
            // This is the first call to close this manager. So this method runs through all the closing phases
            // and concurrent close invocations from the parent manager or further shutdowns will rely on
            // this method to run through the protocol.
            logger.info(s"Closing life cycle manager $name with trace ID $traceContext.")
            val drainingIterator = tasks.drain().buffered
            // Propagate the close signal synchronously
            doRunOnClosingUpTo(
              drainingIterator,
              LifeCycleManagerImpl.closeSignalPropagationPriority,
            )

            implicit val ec: ExecutionContext = directExecutionContext
            // Execute the RunOnClosing tasks in another thread
            doRunOnClosingAsync(drainingIterator)
              .flatMap(_ => synchronizeAndReleaseRegistered(drainingIterator))
              .thereafter { _ =>
                // We remove the handlers from the parent manager only once the closing has completed.
                // This ensures that the parent manager's close method will not return before this dependency
                // has finished closing.
                parentHandles.foreach(_.cancelAll())
                logger.debug(s"Closing life cycle manager $name has completed")
                closingInfo.closingCompleted.success(())
              }
          case Some(prevCloseInfo) =>
            logger.debug(
              s"Closing of life cycle manager $name has previously been initiated with trace ID ${prevCloseInfo.traceContext}. Skipping propagation of the close signal."
            )
            prevCloseInfo.closingCompleted.future
        }
      }

    private def doRunOnClosingAsync(
        iterator: BufferedIterator[(LifeCycleManagerTask, TwoPhasePriorityAccumulator.Priority)]
    )(implicit traceContext: TraceContext): Future[Unit] =
      Future {
        doRunOnClosingUpTo(
          iterator,
          LifeCycleManagerImpl.runOnClosingPriority,
        )
      }(executionContext)

    /** Called when the constructor detects that the parent manager has already been closed.
      * Fast-forwards to the final closing state because we know that there cannot be any tasks
      * registered with this manager.
      */
    private[this] def closeFromConstructor()(implicit traceContext: TraceContext): Unit =
      withCloseTraceContext { implicit traceContext =>
        val mode = new ClosingInitiatedFromParent
        mode.iteratorCell.putIfAbsent(Iterator.empty.buffered).discard
        val closingInfo = CloseInfo(traceContext, mode)
        closingInfo.closingCompleted.success(())
        // DECISION POINT for which shutdown path is responsible for the shutdown procedure
        val prev = tasks.stopAccumulating(closingInfo)
        prev.foreach { prevCloseInfo =>
          // We may end up here if the parent manager has started to invoke the registered tasks for this manager.
          // Then we simply fast-forward to the final state because we know that there are no tasks to be run.
          // This is important as we do not know to what extent the task registrations have succeeded.
          prevCloseInfo.closingCompleted.trySuccess(())
        }
      }

    private[this] final class CloseSignalPropagationTask extends RunOnClosing {
      override val name: String = s"${self.name}-close-signal-propagation"

      override def done: Boolean = false

      override def run()(implicit traceContext: TraceContext): Unit = {
        val outerTraceContext = traceContext
        withCloseTraceContext { implicit traceContext =>
          val mode = new ClosingInitiatedFromParent
          val closingInfo = CloseInfo(traceContext, mode)
          // DECISION POINT for which shutdown path is responsible for the shutdown procedure
          val prev = tasks.stopAccumulating(closingInfo)
          prev match {
            case None =>
              logger.info(
                s"Closing life cycle manager ${self.name} is starting with trace ID $traceContext, initiated by parent life cycle manager."
              )(traceContext)
              val drainingIterator = tasks.drain().buffered
              mode.iteratorCell.putIfAbsent(drainingIterator).discard

              // Immediately propagate the close signal to all dependents.
              doRunOnClosingUpTo(
                drainingIterator,
                LifeCycleManagerImpl.closeSignalPropagationPriority,
              )(traceContext)

            case Some(closeInfo) =>
              // It is fine to immediately return because the "close signal propagation" step in the shutdown
              // procedure does not require that dependents of this manager receive the signal before the
              // parent starts with the "run on shutdown" step.
              logger.debug(
                s"Closure of life cycle manager ${self.name} has previously been initiated with trace ID ${closeInfo.traceContext}. Skipping propagation of the close signal."
              )(outerTraceContext)
          }
        }
      }
    }

    private[this] final class RunOnClosingTask extends RunOnClosing {
      override val name: String = s"${self.name}-run-on-closing"
      override def done: Boolean = false
      override def run()(implicit traceContext: TraceContext): Unit = {
        val closeInfo = tasks.getPhase.getOrElse(
          ErrorUtil.invalidState(
            "Parent manager invoked the run-on-closing task without propagating the close signal previously."
          )
        )
        closeInfo.mode match {
          case mode: ClosingInitiatedFromParent =>
            logger.debug(
              s"Running tasks on closing for ${self.name}, initiated by parent life cycle manager"
            )
            val iterator = mode.iteratorCell.getOrElse(
              ErrorUtil.invalidState("CloseInfo is not properly initialized")
            )
            doRunOnClosingUpTo(iterator, LifeCycleManagerImpl.runOnClosingPriority)
          case ExplicitlyClosing =>
            // It is fine to immediately return because the "run on shutdown" step in the shutdown procedure
            // does not require that all registered `RunOnClosing` tasks have been run by the time the
            // parent manager starts with the "synchronize with closing" or "actually close" steps
            // of the shutdown procedure.
            logger.debug(
              "Skipping the run-on-close tasks because the manager has been explicitly closed."
            )
        }
      }
    }

    private[this] final class ReleaseByParentManagerTask extends ManagedResource {
      override val name: String = s"LifeCycleManager(${self.name})"

      override protected def releaseByManager()(implicit
          traceContext: TraceContext
      ): Future[Unit] = {
        val closeInfo = tasks.getPhase.getOrElse(
          ErrorUtil.invalidState(
            "Parent manager called releaseByManager without propagating the close signal previously"
          )
        )
        closeInfo.mode match {
          case mode: ClosingInitiatedFromParent =>
            logger.debug(
              s"Running tasks on closing for ${self.name}, initiated by parent life cycle manager"
            )
            val iterator = mode.iteratorCell.getOrElse(
              ErrorUtil.invalidState("CloseInfo's iterator cell is not properly initialized")
            )
            synchronizeAndReleaseRegistered(iterator)
          case ExplicitlyClosing =>
            closeInfo.closingCompleted.future
        }
      }
    }

    private[this] def doRunOnClosingUpTo(
        iterator: BufferedIterator[(LifeCycleManagerTask, TwoPhasePriorityAccumulator.Priority)],
        priorityInclusive: Int,
    )(implicit traceContext: TraceContext): Unit = {
      @tailrec def go(): Unit =
        if (iterator.hasNext) {
          iterator.head match {
            case (task: RunOnClosing, priority) if priority <= priorityInclusive =>
              iterator.next().discard
              runTaskUnlessDone(task)
              go()
            case _ =>
              // This method is not meant to be used to close managed resources. So stop here.
              ()
          }
        }

      logger.debug(
        s"Running tasks on closing for $name up to priority $priorityInclusive inclusive."
      )
      go()
    }

    override protected[this] def runTaskUnlessDone(task: RunOnClosing)(implicit
        traceContext: TraceContext
    ): Unit =
      // TODO(#16601): Time-box these tasks!
      Try(task.run()).forFailed(t => logger.warn(s"Task '${task.name}' failed on closing!", t))

    private[this] def releaseRegistered(
        iterator: BufferedIterator[(LifeCycleManagerTask, TwoPhasePriorityAccumulator.Priority)]
    )(implicit traceContext: TraceContext): Future[Unit] = {
      def drainSamePriority(): Seq[LifeCycleManagerTask] = {
        val drainedB = Seq.newBuilder[LifeCycleManagerTask]

        @tailrec def go(previousPriority: Option[TwoPhasePriorityAccumulator.Priority]): Unit =
          if (iterator.hasNext) {
            val (task, priority) = iterator.head
            if (previousPriority.forall(_ == priority)) {
              drainedB.addOne(task)
              iterator.next().discard
              go(Some(priority))
            }
          }

        go(None)
        drainedB.result()
      }

      type CollectedErrors = Seq[(String, Throwable)]

      def releaseInParallel(
          tasks: Seq[LifeCycleManagerTask],
          acc: CollectedErrors,
      ): Future[CollectedErrors] = {
        val runFutures = tasks.map {
          case managed: ManagedResource =>
            // TODO(#16601) time limit!
            Future.unit
              .flatMap(_ => managed.releaseByManagerInternal())(executionContext)
              .transform {
                case Success(_) => Success(None)
                case Failure(ex) =>
                  val taskName = managed.nameInternal
                  logger
                    .warn(s"Releasing managed resource '$taskName' from manager '$name' failed", ex)
                  Success(Some(taskName -> ex))
              }(directExecutionContext)
          case _: RunOnClosing =>
            // RunOnClosing tasks have lower priorities than ManagedResources
            // and we have drained all of them before ManagedResources. Since this draining needs a
            // look-ahead, we therefore have drained the first ManagedResource from the `tasks`
            // and therefore another RunOnClosing task added afterwards cannot appear in the iterator any more.
            ErrorUtil.invalidState(
              "RunOnClosing tasks should have been run before ManagedResources."
            )
        }
        implicit val ec: DirectExecutionContext = directExecutionContext
        Future.sequence(runFutures).map(errors => errors.flatten ++ acc)
      }

      def goByPriority(acc: CollectedErrors): Future[CollectedErrors] =
        if (iterator.hasNext) {
          val drained = drainSamePriority()
          implicit val ec: DirectExecutionContext = directExecutionContext
          releaseInParallel(drained, acc).flatMap(goByPriority)
        } else {
          Future.successful(acc)
        }

      goByPriority(Seq.empty).map { errors =>
        if (errors.nonEmpty) {
          val shutdownError = new ShutdownFailedException(NonEmpty(Seq, s"LifeCycleManager($name)"))
          errors.foreach { case (_, ex) =>
            shutdownError.addSuppressed(ex)
          }
          throw shutdownError
        }
      }(directExecutionContext)
    }

    /** Semaphore for all the [[HasSynchronizeWithClosing.synchornizeWithClosing]] calls. Each such
      * call obtains a permit for the time the computation is running. When the [[LifeCycleManager]]
      * closes, it grabs all permits and thereby prevents further calls from succeeding.
      */
    private[this] val readerSemaphore = new Semaphore(Int.MaxValue)

    /** An underapproximation of the [[HasSynchronizeWithClosing.synchornizeWithClosing]] calls that
      * currently hold a [[readerSemaphore]] permit. This represents a multiset of those call's
      * names, mapping each name to its multiplicity. Used for logging the calls that interfere with
      * closing for too long.
      *
      * Invariant: Does not contain the value 0.
      */
    private[this] val readerMultisetUnderapproximation = new TrieMap[String, Int]

    private[this] def synchronizeAndReleaseRegistered(
        iterator: BufferedIterator[(LifeCycleManagerTask, TwoPhasePriorityAccumulator.Priority)]
    )(implicit traceContext: TraceContext): Future[Unit] =
      // Synchronize asynchronously so that the parent manager can continue closing
      // its other managed resources in parallel.
      Future {
        val allReadersClosed = synchronizeWithReaders()
        releaseRegistered(iterator).map { _ =>
          if (!allReadersClosed) {
            // If we were not able to synchronize with all readers above, then make another attempt
            // at grabbing all of them. Maybe they all have finished by now?
            if (!readerSemaphore.tryAcquire(Int.MaxValue)) {
              val remainingReaders = readerMultisetUnderapproximation.keys.toSeq
              NonEmpty.from(remainingReaders).foreach { readers =>
                val shutdownError = new ShutdownFailedException(readers)
                throw shutdownError
              }
            }
          }
        }(directExecutionContext)
      }(executionContext).flatten

    private[this] def synchronizeWithReaders()(implicit traceContext: TraceContext): Boolean = {
      val deadline = synchronizeWithClosingPatience.fromNow

      @tailrec def poll(patienceMillis: Long): Boolean = {
        val acquired = readerSemaphore.tryAcquire(
          // Grab all of the permits at once
          Int.MaxValue,
          patienceMillis,
          java.util.concurrent.TimeUnit.MILLISECONDS,
        )
        if (acquired) true
        else {
          val timeLeft = deadline.timeLeft
          if (timeLeft < zeroDuration) {
            logger.warn(
              s"Timeout $synchronizeWithClosingPatience expired, but tasks still running. Shutting down forcibly."
            )
            logger.debug(s"Tasks sill running: ${readerMultisetUnderapproximation.toString}")
            false
          } else {
            val readerCount = Int.MaxValue - readerSemaphore.availablePermits()
            val nextPatienceMillis =
              (patienceMillis * 2) min maxSleepMillis min timeLeft.toMillis
            logger.debug(
              s"$readerCount active tasks prevent closing. Next log message in ${nextPatienceMillis}ms. Tasks: ${readerMultisetUnderapproximation.toString}"
            )
            poll(nextPatienceMillis)
          }
        }
      }

      poll(10L)
    }

    private def addReader(reader: String)(implicit traceContext: TraceContext): Boolean =
      // Abort early if we are closing.
      // This prevents new readers from registering themselves so that eventually all permits become available
      // to grab in one go
      if (isClosing) false
      else if (readerSemaphore.tryAcquire()) {
        readerMultisetUnderapproximation
          .updateWith(reader) {
            case None => Some(1)
            case Some(i) => Some(i + 1)
          }
          .discard
        true
      } else if (isClosing) {
        // We check again for closing because the closing may have been initiated concurrently since the previous check
        false
      } else {
        logger.error(
          s"All ${Int.MaxValue} reader locks for life cycle manager $name have been taken. Is there a memory/task leak somewhere?"
        )
        logger.debug(s"Currently registered readers: ${readerMultisetUnderapproximation.toString}")
        throw new IllegalStateException(
          s"All ${Int.MaxValue} reader locks for manager $name have been taken."
        )
      }

    private def removeReader(reader: String): Unit = {
      readerMultisetUnderapproximation
        .updateWith(reader) {
          case None => throw new IllegalStateException(s"Reader $reader is unknown.")
          case Some(i) => Option.when(i > 1)(i - 1)
        }
        .discard
      readerSemaphore.release()
    }

    override def toString: String = s"LifeCycleManagerImpl(name=$name)"
  }

  private object LifeCycleManagerImpl {

    private val closeSignalPropagationPriority: Int = Int.MinValue
    private[LifeCycleManager] val runOnClosingPriority: Int = Int.MinValue + 1

    private[LifeCycleManagerImpl] final case class ParentHandles(
        onCloseSignal: LifeCycleRegistrationHandle,
        onRunOnClosingSignal: LifeCycleRegistrationHandle,
        onReleaseByParentSignal: LifeCycleRegistrationHandle,
    ) {
      def cancelAll(): Unit = {
        onCloseSignal.cancel().discard[Boolean]
        onRunOnClosingSignal.cancel().discard[Boolean]
        onReleaseByParentSignal.cancel().discard[Boolean]
      }
    }

    private def withCloseTraceContext[A](f: TraceContext => A)(implicit
        traceContext: TraceContext
    ): A = {
      val closeTraceContext =
        if (traceContext == TraceContext.empty) TraceContext.createNew() else traceContext
      f(closeTraceContext)
    }

    /** Indicates that the life cycle manager is being closed.
      */
    private final case class CloseInfo(traceContext: TraceContext, mode: ClosingMode) {

      /** This promise completes when the [[LifeCycleManager]] has closed successfully. It is used
        * to synchronize multiple calls to [[LifeCycleManager.closeAsync]].
        */
      val closingCompleted: Promise[Unit] = Promise[Unit]()
    }

    /** Defines which closing code path is responsible for closing the [[LifeCycleManager]]. */
    private sealed trait ClosingMode

    /** The invocation of [[LifeCycleManager.closeAsync]] caused the closing of the
      * [[LifeCycleManager]].
      */
    private case object ExplicitlyClosing extends ClosingMode

    /** The [[LifeCycleManager]] is closed because its parent is closing or has been closed. */
    private final class ClosingInitiatedFromParent extends ClosingMode {
      val iteratorCell: SingleUseCell[BufferedIterator[(LifeCycleManagerTask, Priority)]] =
        new SingleUseCell[
          BufferedIterator[(LifeCycleManagerTask, TwoPhasePriorityAccumulator.Priority)]
        ]
    }

    private val zeroDuration: FiniteDuration =
      FiniteDuration(0, java.util.concurrent.TimeUnit.MILLISECONDS)

    /** How often to poll to check that all tasks have completed. */
    private val maxSleepMillis: Long = 500
  }

  private[lifecycle] final class LifeCycleRegistrationHandleImpl(
      handle: TwoPhasePriorityAccumulator.ItemHandle
  ) extends LifeCycleRegistrationHandle {
    override def cancel(): Boolean = handle.remove()
    override def isScheduled: Boolean = handle.accumulated
  }

  private[lifecycle] object DummyHandle extends LifeCycleRegistrationHandle {
    override def cancel(): Boolean = false
    override def isScheduled: Boolean = false
  }

  /** A managed resource is supposed to be released by its associated life cycle manager
    */
  trait ManagedResource extends LifeCycleManager.LifeCycleManagerTask {
    protected def name: String

    @inline private[LifeCycleManager] final def nameInternal: String = name

    protected def releaseByManager()(implicit traceContext: TraceContext): Future[Unit]

    @inline private[LifeCycleManager] final def releaseByManagerInternal()(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      releaseByManager()
  }
}

/** Trait that can be registered with a [[LifeCycleManager]] or [[OnShutdownRunner]] to run on
  * closing / shutdown.
  */
trait RunOnClosing extends LifeCycleManager.LifeCycleManagerTask {

  /** The name, used for logging during shutdown */
  def name: String

  /** Indicates whether the task does not have to be run any more and can be removed. Must not have
    * side effects and run quickly.
    */
  def done: Boolean

  /** Invoked by [[LifeCycleManager]] during shutdown. Do not run blocking computations here!
    */
  def run()(implicit traceContext: TraceContext): Unit
}
