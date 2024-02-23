// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Id
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{DirectExecutionContext, Threading}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.TryUtil.*
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, FlowOps, FlowOpsMat, Keep, RunnableGraph, Source}
import org.apache.pekko.stream.stage.{
  GraphStageLogic,
  GraphStageWithMaterializedValue,
  InHandler,
  OutHandler,
}
import org.apache.pekko.stream.{
  ActorAttributes,
  Attributes,
  FlowShape,
  Inlet,
  KillSwitch,
  KillSwitches,
  Materializer,
  Outlet,
  QueueCompletionResult,
  QueueOfferResult,
  Supervision,
  UniqueKillSwitch,
}
import org.apache.pekko.{Done, NotUsed}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object PekkoUtil extends HasLoggerName {

  /** Utility function to run the graph supervised and stop on an unhandled exception.
    *
    * By default, an Pekko flow will discard exceptions. Use this method to avoid discarding exceptions.
    */
  def runSupervised[T](
      reporter: Throwable => Unit,
      graph: RunnableGraph[T],
      debugLogging: Boolean = false,
  )(implicit
      mat: Materializer
  ): T = {
    val tmp = graph
      .addAttributes(ActorAttributes.supervisionStrategy { ex =>
        reporter(ex)
        Supervision.Stop
      })
    (if (debugLogging)
       tmp.addAttributes(ActorAttributes.debugLogging(true))
     else tmp)
      .run()
  }

  /** Create an Actor system using the existing execution context `ec`
    */
  def createActorSystem(namePrefix: String)(implicit ec: ExecutionContext): ActorSystem =
    ActorSystem(
      namePrefix + "-actor-system",
      defaultExecutionContext = Some(ec),
      config = Some(ConfigFactory.load),
    )

  /** Create a new execution sequencer factory (mainly used to create a ledger client) with the existing actor system `actorSystem`
    */
  def createExecutionSequencerFactory(namePrefix: String, logger: Logger)(implicit
      actorSystem: ActorSystem
  ): ExecutionSequencerFactory =
    new PekkoExecutionSequencerPool(
      namePrefix + "-execution-sequencer",
      actorCount = Threading.detectNumberOfThreads(logger),
    )

  /** Remembers the last `memory` many elements that have already been emitted previously.
    * Passes those remembered elements downstream with each new element.
    * The current element is the [[com.daml.nonempty.NonEmptyCollInstances.NEPreservingOps.last1]]
    * of the sequence.
    *
    * [[remember]] differs from [[org.apache.pekko.stream.scaladsl.FlowOps.sliding]] in
    * that [[remember]] emits elements immediately when the given source emits,
    * whereas [[org.apache.pekko.stream.scaladsl.FlowOps.sliding]] only after the source has emitted enough elements to fill the window.
    */
  def remember[A, Mat](
      graph: FlowOps[A, Mat],
      memory: NonNegativeInt,
  ): graph.Repr[NonEmpty[Seq[A]]] = {
    // Prepend window many None to the given source
    // so that sliding starts emitting upon the first element received
    graph
      .map(Some(_))
      .prepend(Source(Seq.fill(memory.value)(None)))
      .sliding(memory.value + 1)
      .mapConcat { noneOrElems =>
        // dropWhile is enough because None can only appear in the prefix
        val elems = noneOrElems
          .dropWhile(_.isEmpty)
          .map(_.getOrElse(throw new NoSuchElementException("Some did not contain a value")))
        // Do not emit anything if `noneOrElems` is all Nones,
        // because then the source completed before emitting any elements
        NonEmpty.from(elems)
      }
  }

  /** A version of [[org.apache.pekko.stream.scaladsl.FlowOps.mapAsync]] that additionally allows to pass state of type `S` between
    * every subsequent element. Unlike [[org.apache.pekko.stream.scaladsl.FlowOps.statefulMapConcat]], the state is passed explicitly.
    * Must not be run with supervision strategies [[org.apache.pekko.stream.Supervision.Restart]] nor [[org.apache.pekko.stream.Supervision.Resume]]
    */
  def statefulMapAsync[Out, Mat, S, T](graph: FlowOps[Out, Mat], initial: S)(
      f: (S, Out) => Future[(S, T)]
  )(implicit loggingContext: NamedLoggingContext): graph.Repr[T] = {
    val directExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
    graph
      .scanAsync((initial, Option.empty[T])) { case ((state, _), next) =>
        f(state, next)
          .map { case (newState, out) => (newState, Some(out)) }(directExecutionContext)
      }
      .drop(1) // The first element is `(initial, empty)`, which we want to drop
      .map(
        _._2.getOrElse(
          ErrorUtil.internalError(new NoSuchElementException("scanAsync did not return an element"))
        )
      )
  }

  /** Version of [[org.apache.pekko.stream.scaladsl.FlowOps.mapAsync]] for a [[com.digitalasset.canton.lifecycle.FutureUnlessShutdown]].
    * If `f` returns [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] on one element of
    * `source`, then the returned source returns [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]
    * for all subsequent elements as well.
    *
    * If `parallelism` is one, ensures that `f` is called sequentially for each element of `source`
    * and that `f` is not invoked on later stream elements if `f` returns
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]] for an earlier element.
    * If `parellelism` is greater than one, `f` may be invoked on later stream elements
    * even though an earlier invocation results in `f` returning
    * [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]].
    *
    * '''Emits when''' the Future returned by the provided function finishes for the next element in sequence
    *
    * '''Backpressures when''' the number of futures reaches the configured parallelism and the downstream
    * backpressures or the first future is not completed
    *
    * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted,
    * including those for which the future did not run due to earlier [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]s.
    *
    * '''Cancels when''' downstream cancels
    *
    * @param parallelism The parallelism level. Must be at least 1.
    * @throws java.lang.IllegalArgumentException if `parallelism` is not positive.
    */
  def mapAsyncUS[A, Mat, B](graph: FlowOps[A, Mat], parallelism: Int)(
      f: A => FutureUnlessShutdown[B]
  )(implicit loggingContext: NamedLoggingContext): graph.Repr[UnlessShutdown[B]] = {
    require(parallelism > 0, "Parallelism must be positive")
    // If parallelism is 1, then the caller expects that the futures run in sequential order,
    // so if one of them aborts due to shutdown we must not run the subsequent ones.
    // For parallelism > 1, we do not have to stop immediately, as there is always a possible execution
    // where the future may have been started before the first one aborted.
    // So we just need to throw away the results of the futures and convert them into aborts.
    if (parallelism == 1) {
      val directExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
      statefulMapAsync(graph, initial = false) { (aborted, next) =>
        if (aborted) Future.successful(true -> AbortedDueToShutdown)
        else f(next).unwrap.map(us => !us.isOutcome -> us)(directExecutionContext)
      }
    } else {
      val discardedInitial: UnlessShutdown[B] = AbortedDueToShutdown
      // Mutable reference to short-circuit one we've observed the first aborted due to shutdown.
      val abortedFlag = new AtomicBoolean(false)
      graph
        .mapAsync(parallelism)(elem =>
          if (abortedFlag.get()) Future.successful(AbortedDueToShutdown)
          else f(elem).unwrap
        )
        .scan((false, discardedInitial)) { case ((aborted, _), next) =>
          if (aborted) (true, AbortedDueToShutdown)
          else {
            val abort = !next.isOutcome
            if (abort) abortedFlag.set(true)
            (abort, next)
          }
        }
        .drop(1) // The first element is `(false, discardedInitial)`, which we want to drop
        .map(_._2)
    }
  }

  /** Version of [[mapAsyncUS]] that discards the [[com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown]]s.
    *
    * '''Completes when''' upstream completes and all futures have been completed and all elements have been emitted.
    */
  def mapAsyncAndDrainUS[A, Mat, B](graph: FlowOps[A, Mat], parallelism: Int)(
      f: A => FutureUnlessShutdown[B]
  )(implicit loggingContext: NamedLoggingContext): graph.Repr[B] = {
    mapAsyncUS(graph, parallelism)(f)
      // Important to use `collect` instead of `takeWhile` here
      // so that the return source completes only after all `source`'s elements have been consumed.
      // TODO(#13789) Should we cancel/pull a kill switch to signal upstream that no more elements are needed?
      .collect { case Outcome(x) => x }
  }

  /** Lifts [[statefulMapAsyncUS]] over a context. */
  def statefulMapAsyncContextualizedUS[Out, Mat, S, T, Context[_], C](
      graph: FlowOps[Context[Out], Mat],
      initial: S,
  )(
      f: (S, C, Out) => FutureUnlessShutdown[(S, T)]
  )(implicit
      loggingContext: NamedLoggingContext,
      Context: SingletonTraverse.Aux[Context, C],
  ): graph.Repr[Context[UnlessShutdown[T]]] = {
    implicit val directExecutionContext: ExecutionContext =
      DirectExecutionContext(loggingContext.tracedLogger)
    statefulMapAsync(graph, initial = Option(initial)) {
      case (oldState @ Some(s), next) =>
        // Since the context contains at most one element, it is fine to use traverse with futures here
        @SuppressWarnings(Array("com.digitalasset.canton.FutureTraverse"))
        val resultF = next.traverseSingleton(f(s, _, _).unwrap)
        resultF.map { contextualizedStateAndResult =>
          // Since the type class ensures that the context `next` contains at most one element,
          // we can look for the last element in the context `result`.
          val theResult = Context
            .foldLeft(contextualizedStateAndResult, Option.empty[UnlessShutdown[(S, T)]])((_, t) =>
              Some(t)
            )
          val nextS = theResult match {
            case Some(Outcome((nextS, _))) => Some(nextS)
            case Some(AbortedDueToShutdown) => None
            case None =>
              // There is no element in the context, so `f` was never run and we keep the old state
              oldState
          }
          val contextualizedResult =
            Context.map(contextualizedStateAndResult)(_.map { case (_, t) => t })
          nextS -> contextualizedResult
        }
      case (None, next) =>
        val abortWithoutRunning = Context.map(next)(_ => AbortedDueToShutdown: UnlessShutdown[T])
        Future.successful(None -> abortWithoutRunning)
    }
  }

  /** Combines [[mapAsyncUS]] with [[statefulMapAsync]]. */
  def statefulMapAsyncUS[Out, Mat, S, T](graph: FlowOps[Out, Mat], initial: S)(
      f: (S, Out) => FutureUnlessShutdown[(S, T)]
  )(implicit loggingContext: NamedLoggingContext): graph.Repr[UnlessShutdown[T]] =
    statefulMapAsyncContextualizedUS[Out, Mat, S, T, cats.Id, Unit](graph, initial)((s, _, out) =>
      f(s, out)
    )

  def statefulMapAsyncUSAndDrain[Out, Mat, S, T](graph: FlowOps[Out, Mat], initial: S)(
      f: (S, Out) => FutureUnlessShutdown[(S, T)]
  )(implicit loggingContext: NamedLoggingContext): graph.Repr[T] = {
    statefulMapAsyncUS(graph, initial)(f)
      // Important to use `collect` instead of `takeWhile` here
      // so that the return source completes only after all `source`'s elements have been consumed.
      // TODO(#13789) Should we cancel/pull a kill switch to signal upstream that no more elements are needed?
      .collect { case Outcome(x) => x }
  }

  /** Combines two kill switches into one */
  class CombinedKillSwitch(private val killSwitch1: KillSwitch, private val killSwitch2: KillSwitch)
      extends KillSwitch {
    override def shutdown(): Unit = {
      killSwitch1.shutdown()
      killSwitch2.shutdown()
    }

    override def abort(ex: Throwable): Unit = {
      killSwitch1.abort(ex)
      killSwitch2.abort(ex)
    }
  }

  /** Defines the policy when [[restartSource]] should restart the source, and the state from which the source should be restarted from. */
  trait RetrySourcePolicy[S, -A] {

    /** Determines whether the source should be restarted, and if so (([[scala.Some$]])),
      * the backoff duration and the new state to restart from.
      * Called after the current source has terminated normally or with an error.
      *
      * @param lastState The state that was used to create the current source
      * @param lastEmittedElement The last element emitted by the current source and passed downstream.
      *                           Downstream obviously need not yet have fully processed the element though.
      *                           [[scala.None$]] if the current source did not emit anything,
      *                           even if previous sources have emitted elements.
      * @param lastFailure The error the current source failed with, if any.
      */
    def shouldRetry(
        lastState: S,
        lastEmittedElement: Option[A],
        lastFailure: Option[Throwable],
    ): Option[(FiniteDuration, S)]
  }

  object RetrySourcePolicy {
    private val NEVER: RetrySourcePolicy[Any, Any] = new RetrySourcePolicy[Any, Any] {
      override def shouldRetry(
          lastState: Any,
          lastEmittedElement: Option[Any],
          lastFailure: Option[Throwable],
      ): Option[Nothing] = None
    }
    @SuppressWarnings(Array("org.wartremover.wart.AsInstanceOf"))
    def never[S, A]: RetrySourcePolicy[S, A] = NEVER.asInstanceOf[RetrySourcePolicy[S, A]]
  }

  /** Creates a source from `mkSource` from the `initial` state.
    * Whenever this source terminates, `policy` determines whether another source shall be constructed (after a given delay) from a possibly new state.
    * The returned source concatenates the output of all the constructed sources in order.
    * At most one constructed source is active at any given point in time.
    *
    * Failures in the constructed sources are passed to the `policy`, but do not make it downstream.
    * The `policy` is responsible for properly logging these errors if necessary.
    *
    * @return The concatenation of all constructed sources.
    *         This source is NOT a blueprint and MUST therefore be materialized at most once.
    *         Its materialized value provides a kill switch to stop retrying.
    *         Only the [[org.apache.pekko.stream.KillSwitch.shutdown]] method should be used;
    *         The switch does not short-circuit the already constructed sources though.
    *         synchronization may not work correctly with [[org.apache.pekko.stream.KillSwitch.abort]].
    *         Downstream should not cancel; use the kill switch instead.
    *
    *         The materialized [[scala.concurrent.Future]] can be used to synchronize on the computations for restarts:
    *         if the source is stopped with the kill switch, the future completes after the computations have finished.
    */
  def restartSource[S: Pretty, A](
      name: String,
      initial: S,
      mkSource: S => Source[A, (KillSwitch, Future[Done])],
      policy: RetrySourcePolicy[S, A],
  )(implicit
      loggingContext: NamedLoggingContext,
      materializer: Materializer,
  ): Source[WithKillSwitch[A], (KillSwitch, Future[Done])] = {
    val directExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)

    // Use immediate acknowledgements and buffer size 1 to minimize the risk that
    // several materializations of the returned source concurrently restart stuff.
    val (boundedSourceQueue, source) = Source.queue[S](bufferSize = 1).preMaterialize()
    val flushFuture = new FlushFuture(s"RestartSource $name", loggingContext.loggerFactory)

    def idempotentComplete(): Unit =
      try {
        boundedSourceQueue.complete()
      } catch {
        case _: IllegalStateException =>
      }

    trait KillSwitchForRestartSource extends KillSwitch {
      type Handle

      /** Register a function to be executed when the kill switch is pulled.
        *
        * @return A handle with which the function can be removed again using [[removeOnClose]].
        */
      def runOnClose(f: () => Unit): Handle
      def removeOnClose(handle: Handle): Unit
    }

    class KillSwitchForRestartSourceImpl extends KillSwitchForRestartSource {
      override type Handle = AnyRef

      private val isClosing = new AtomicBoolean(false)

      private val completeOnClosing: scala.collection.concurrent.Map[Any, () => Unit] =
        TrieMap.empty[Any, () => Unit]

      private def onClose(): Unit = {
        isClosing.set(true)
        completeOnClosing.foreach { case (_, f) => f() }
      }

      def runOnClose(f: () => Unit): Handle = {
        val handle = new Object()
        completeOnClosing.put(handle, f).discard[Option[() => Unit]]
        if (isClosing.get()) f()
        handle
      }

      def removeOnClose(handle: Handle): Unit =
        completeOnClosing.remove(handle).discard[Option[() => Unit]]

      override def shutdown(): Unit = {
        onClose()
        idempotentComplete()
      }

      override def abort(ex: Throwable): Unit = {
        onClose()
        try {
          boundedSourceQueue.fail(ex)
        } catch {
          case _: IllegalStateException =>
        }
      }
    }
    val killSwitchForSourceQueue: KillSwitchForRestartSource = new KillSwitchForRestartSourceImpl

    def restartFrom(nextState: S): Unit = {
      loggingContext.debug(show"(Re)Starting the source $name from state $nextState")
      boundedSourceQueue.offer(nextState) match {
        case QueueOfferResult.Enqueued =>
          loggingContext.debug(s"Restarted the source $name with state $nextState")
        case QueueOfferResult.Dropped =>
          // This should not happen
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Could not restart the source $name because the state queue is full. Has the returned source been materialized multiple times?"
            )
          )
        case _: QueueCompletionResult =>
          loggingContext.debug(
            s"Not restarting $name because the restart source has already been completed"
          )
      }
    }

    // Kick it off with the initial state
    restartFrom(initial)

    source
      .flatMapConcat { state =>
        val lastObservedElem: AtomicReference[Option[A]] = new AtomicReference[Option[A]](None)
        val lastObservedError: AtomicReference[Option[Throwable]] =
          new AtomicReference[Option[Throwable]](None)

        def observeSuccess(elem: Try[A]): Try[A] = {
          elem.foreach(x => lastObservedElem.set(Some(x)))
          elem
        }
        val observeError: Throwable PartialFunction Try[A] = { case NonFatal(ex) =>
          lastObservedError.set(Some(ex))
          Failure(ex)
        }

        // flatMapConcat swallows the materialized value of the inner sources
        // So we make them accessible to the retry directly.
        def uponTermination(handleKillSwitch: killSwitchForSourceQueue.Handle, doneF: Future[Done])
            : NotUsed = {
          val afterTerminationF = doneF
            .thereafter { outcome =>
              ErrorUtil.requireArgument(
                outcome.isSuccess,
                s"RestartSource $name: recover did not catch the error $outcome",
              )
              // Deregister the inner streams kill switch upon termination to prevent memory leaks
              killSwitchForSourceQueue.removeOnClose(handleKillSwitch)
              policy.shouldRetry(state, lastObservedElem.get, lastObservedError.get) match {
                case Some((backoff, nextState)) =>
                  implicit val ec: ExecutionContext = directExecutionContext

                  val delayedPromise = Promise[UnlessShutdown[Unit]]()
                  val handleDelayedPromise = killSwitchForSourceQueue.runOnClose { () =>
                    delayedPromise.trySuccess(AbortedDueToShutdown).discard[Boolean]
                  }
                  val delayedF = DelayUtil.delay(backoff).thereafter { _ =>
                    killSwitchForSourceQueue.removeOnClose(handleDelayedPromise)
                    delayedPromise.trySuccess(Outcome(())).discard[Boolean]
                  }
                  FutureUtil.doNotAwait(
                    delayedF,
                    s"DelayUtil.delay for RestartSource $name failed",
                  )

                  val restartF = delayedPromise.future.map {
                    case AbortedDueToShutdown =>
                      loggingContext.debug(s"Not restarting $name due to shutdown")
                    case Outcome(()) => restartFrom(nextState)
                  }
                  FutureUtil.doNotAwait(
                    restartF,
                    s"Restart future for RestartSource $name failed",
                  )
                case None =>
                  loggingContext.debug(s"Not retrying $name any more. Completing the source.")
                  idempotentComplete()
              }
            }(materializer.executionContext)
            .thereafter(_.forFailed { ex =>
              loggingContext.error(
                s"The retry policy for RestartSource $name failed with an error. Stop retrying.",
                ex,
              )
              idempotentComplete()
            })(materializer.executionContext)
          flushFuture.addToFlushAndLogError(show"RestartSource ${name.unquoted} at state $state")(
            afterTerminationF
          )
          NotUsed
        }

        mkSource(state)
          // Register the kill switch of the new source with the kill switch of the restart source
          .mapMaterializedValue { case (killSwitch, doneF) =>
            val handle = killSwitchForSourceQueue.runOnClose(() => killSwitch.shutdown())
            // The completion future terminates with an exception when the source itself aborts with the same exception
            // Since it is the responsibility of the policy to triage such exceptions, we do not log it here.
            flushFuture.addToFlushWithoutLogging(
              show"RestartSource ${name.unquoted}: completion future of $state"
            )(doneF)
            handle
          }
          .map(Success.apply)
          // Grab any upstream errors of the current source
          // before they escape to the concatenated source and bypass the restart logic
          .recover(observeError)
          // Observe elements only after recovering from errors so that the error cannot jump over the map.
          .map(observeSuccess)
          // Do not use the `doneF` future from the source to initiate the retry
          // because it is unclear how long `doneF` will take to complete after the source has terminated.
          // Instead, decide on a retry eagerly as soon as we know that the last element of the source has been emitted
          .watchTermination()(uponTermination)
      }
      // Filter out the exceptions from the recover
      .mapConcat(_.toOption.map(WithKillSwitch(_)(killSwitchForSourceQueue)))
      .watchTermination() { case (NotUsed, doneF) =>
        val everythingTerminatedF =
          doneF.thereafterF { _ =>
            // Complete the queue of elements again, to make sure that
            // downstream cancellations do not race with a restart.
            idempotentComplete()
            flushFuture.flush()
          }(
            // The direct execution context ensures that this runs as soon as the future's promise is completed,
            // i.e., a downstream cancellation signal cannot propagate upstream while this is running.
            directExecutionContext
          )
        killSwitchForSourceQueue -> everythingTerminatedF
      }
  }

  /** Adds a [[org.apache.pekko.stream.KillSwitches.single]] into the stream after the given source
    * and injects the created kill switch into the stream
    */
  def withUniqueKillSwitch[A, Mat, Mat2](
      graph: FlowOpsMat[A, Mat]
  )(mat: (Mat, UniqueKillSwitch) => Mat2): graph.ReprMat[WithKillSwitch[A], Mat2] = {
    withMaterializedValueMat(new AtomicReference[UniqueKillSwitch])(graph)(Keep.both)
      .viaMat(KillSwitches.single) { case ((m, ref), killSwitch) =>
        ref.set(killSwitch)
        mat(m, killSwitch)
      }
      .map { case (a, ref) => WithKillSwitch(a)(ref.get()) }
  }

  def injectKillSwitch[A, Mat](
      graph: FlowOpsMat[A, Mat]
  )(killSwitch: Mat => KillSwitch): graph.ReprMat[WithKillSwitch[A], Mat] = {
    withMaterializedValueMat(new AtomicReference[KillSwitch])(graph)(Keep.both)
      .mapMaterializedValue { case (mat, ref) =>
        ref.set(killSwitch(mat))
        mat
      }
      .map { case (a, ref) => WithKillSwitch(a)(ref.get()) }
  }

  /** Drops the first `count` many elements from the `graph` that satisfy the `condition`.
    * Keeps all elements that do not satisfy the `condition`.
    */
  def dropIf[A, Mat](graph: FlowOps[A, Mat], count: Int, condition: A => Boolean): graph.Repr[A] =
    graph.statefulMapConcat(() => {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var remaining = count
      elem =>
        if (remaining > 0 && condition(elem)) {
          remaining -= 1
          Seq.empty
        } else Seq(elem)
    })

  private[util] def withMaterializedValueMat[M, A, Mat, Mat2](create: => M)(
      graph: FlowOpsMat[A, Mat]
  )(combine: (Mat, M) => Mat2): graph.ReprMat[(A, M), Mat2] =
    graph.viaMat(new WithMaterializedValue[M, A](() => create))(combine)

  private def logOnThrow[A](logger: Logger, name: => String)(action: => A): A =
    try { action }
    catch {
      case NonFatal(e) =>
        logger.error(s"$name failed", e)
        throw e
    }

  /** Pekko by default swallows exceptions thrown in [[org.apache.pekko.stream.stage.OutHandler]]s.
    * This wrapper makes sure that they are logged.
    */
  class LoggingOutHandler(delegate: OutHandler, logger: Logger, name: String) extends OutHandler {
    override def onPull(): Unit =
      logOnThrow(logger, s"$name onPull")(delegate.onPull())

    override def onDownstreamFinish(cause: Throwable): Unit =
      logOnThrow(logger, s"$name onDownstreamFinish")(delegate.onDownstreamFinish(cause))
  }
  object LoggingOutHandler {
    def apply(logger: Logger, name: String)(delegate: OutHandler): OutHandler =
      new LoggingOutHandler(delegate, logger, name)
  }

  /** Pekko by default swallows exceptions thrown in [[org.apache.pekko.stream.stage.InHandler]]s.
    * This wrapper makes sure that they are logged.
    */
  class LoggingInHandler(delegate: InHandler, logger: Logger, name: String) extends InHandler {
    override def onPush(): Unit =
      logOnThrow(logger, s"$name onPush")(delegate.onPush())

    override def onUpstreamFinish(): Unit =
      logOnThrow(logger, s"$name onUpstreamFinish")(delegate.onUpstreamFinish())

    override def onUpstreamFailure(ex: Throwable): Unit =
      logOnThrow(logger, s"$name onUpstreamFailure")(delegate.onUpstreamFailure(ex))
  }
  object LoggingInHandler {
    def apply(logger: Logger, name: String)(delegate: InHandler): InHandler =
      new LoggingInHandler(delegate, logger, name)
  }

  /** Pekko by default swallows exceptions thrown in async callbacks.
    * This wrapper makes sure that they are logged.
    */
  def loggingAsyncCallback[A](logger: Logger, name: String)(asyncCallback: A => Unit): A => Unit =
    x => logOnThrow(logger, name)(asyncCallback(x))

  /** Creates a value upon materialization that is added to every element of the stream.
    *
    * WARNING: This flow breaks the synchronization abstraction of Pekko streams,
    * as the created value is accessible from within the stream and from the outside through the materialized value.
    * Users of this flow must make sure that accessing the value is thread-safe!
    */
  private class WithMaterializedValue[M, A](create: () => M)
      extends GraphStageWithMaterializedValue[FlowShape[A, (A, M)], M] {
    private val in: Inlet[A] = Inlet[A]("withMaterializedValue.in")
    private val out: Outlet[(A, M)] = Outlet[(A, M)]("withMaterializedValue.out")
    override val shape: FlowShape[A, (A, M)] = FlowShape(in, out)

    override def initialAttributes: Attributes = Attributes.name("withMaterializedValue")

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes
    ): (GraphStageLogic, M) = {
      val m: M = create()
      val logic = new GraphStageLogic(shape) with InHandler with OutHandler {
        override def onPush(): Unit = push(out, grab(in) -> m)

        override def onPull(): Unit = pull(in)

        setHandlers(in, out, this)
      }
      (logic, m)
    }
  }

  /** Container class for adding a [[org.apache.pekko.stream.KillSwitch]] to a single value.
    * Two containers are equal if their contained values are equal.
    *
    * (Equality ignores the [[org.apache.pekko.stream.KillSwitch]]es because it is usually not very meaningful.
    * The [[org.apache.pekko.stream.KillSwitch]] is therefore in the second argument list.)
    */
  final case class WithKillSwitch[+A](override val value: A)(val killSwitch: KillSwitch)
      extends WithGeneric[A, KillSwitch, WithKillSwitch] {
    override protected def added: KillSwitch = killSwitch
    override protected def update[AA](newValue: AA): WithKillSwitch[AA] = copy(newValue)(killSwitch)
  }
  object WithKillSwitch extends WithGenericCompanion {
    implicit val singletonTraverseWithKillSwitch
        : SingletonTraverse.Aux[WithKillSwitch, KillSwitch] =
      singletonTraverseWithGeneric[KillSwitch, WithKillSwitch]
  }

  /** Passes through all elements of the source until and including the first element that satisfies the condition.
    * Thereafter pulls the kill switch of the first such element and drops all remaining elements of the source.
    *
    * '''Emits when''' upstream emits and all previously emitted elements do not meet the condition.
    *
    * '''Backpressures when''' downstream backpressures
    *
    * '''Completes when upstream''' completes
    *
    * '''Cancels when''' downstream cancels
    */
  def takeUntilThenDrain[A, Mat](
      graph: FlowOps[WithKillSwitch[A], Mat],
      condition: A => Boolean,
  ): graph.Repr[WithKillSwitch[A]] =
    graph.statefulMapConcat(() => {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var draining = false
      elem => {
        if (draining) Iterable.empty[WithKillSwitch[A]]
        else {
          if (condition(elem.value)) {
            draining = true
            elem.killSwitch.shutdown()
          }
          Iterable.single(elem)
        }
      }
    })

  val noOpKillSwitch = new KillSwitch {
    override def shutdown(): Unit = ()
    override def abort(ex: Throwable): Unit = ()
  }

  /** Delegates to a future [[org.apache.pekko.stream.KillSwitch]] once the kill switch becomes available.
    * If both [[com.digitalasset.canton.util.PekkoUtil.DelayedKillSwitch.shutdown]] and
    * [[com.digitalasset.canton.util.PekkoUtil.DelayedKillSwitch.abort]] are called or
    * [[com.digitalasset.canton.util.PekkoUtil.DelayedKillSwitch.abort]] is called multiple times before the delegate
    * is available, then the winning call is non-deterministic.
    */
  class DelayedKillSwitch(delegate: Future[KillSwitch], logger: Logger) extends KillSwitch {
    private implicit val directExecutionContext: ExecutionContext = DirectExecutionContext(logger)

    override def shutdown(): Unit = delegate.onComplete(_.foreach(_.shutdown()))

    override def abort(ex: Throwable): Unit = delegate.onComplete(_.foreach(_.abort(ex)))
  }

  object syntax {

    /** Defines extension methods for [[org.apache.pekko.stream.scaladsl.FlowOpsMat]] that map to the methods defined in this class.
      *
      * The construction with type parameter `U` follows
      * <a href="https://typelevel.org/blog/2017/03/01/four-ways-to-escape-a-cake.html">Stephen's blog post about relatable variables</a>
      * to ensure that we can uniformly abstract over [[org.apache.pekko.stream.scaladsl.Source]]s and [[org.apache.pekko.stream.scaladsl.Flow]]s.
      * In particular, we cannot use an implicit class here. Unlike in the blog post, the implicit conversion [[pekkoUtilSyntaxForFlowOps]]
      * does not extract [[org.apache.pekko.stream.scaladsl.FlowOpsMat]] into a separate type parameter because this would confuse
      * type inference.
      */
    private[util] class PekkoUtilSyntaxForFlowOps[A, Mat, U <: FlowOps[A, Mat]](
        private val graph: U
    ) extends AnyVal {
      def remember(window: NonNegativeInt): U#Repr[NonEmpty[Seq[A]]] =
        PekkoUtil.remember(graph, window)

      def statefulMapAsync[S, T](initial: S)(
          f: (S, A) => Future[(S, T)]
      )(implicit loggingContext: NamedLoggingContext): U#Repr[T] =
        PekkoUtil.statefulMapAsync(graph, initial)(f)

      def statefulMapAsyncUS[S, T](initial: S)(
          f: (S, A) => FutureUnlessShutdown[(S, T)]
      )(implicit loggingContext: NamedLoggingContext): U#Repr[UnlessShutdown[T]] =
        PekkoUtil.statefulMapAsyncUS(graph, initial)(f)

      def statefulMapAsyncUSAndDrain[S, T](initial: S)(f: (S, A) => FutureUnlessShutdown[(S, T)])(
          implicit loggingContext: NamedLoggingContext
      ): U#Repr[T] =
        PekkoUtil.statefulMapAsyncUSAndDrain(graph, initial)(f)

      def mapAsyncUS[B](parallelism: Int)(f: A => FutureUnlessShutdown[B])(implicit
          loggingContext: NamedLoggingContext
      ): U#Repr[UnlessShutdown[B]] =
        PekkoUtil.mapAsyncUS(graph, parallelism)(f)

      def mapAsyncAndDrainUS[B](parallelism: Int)(
          f: A => FutureUnlessShutdown[B]
      )(implicit loggingContext: NamedLoggingContext): U#Repr[B] =
        PekkoUtil.mapAsyncAndDrainUS(graph, parallelism)(f)

      def batchN(maxBatchSize: Int, maxBatchCount: Int): U#Repr[immutable.Iterable[A]] =
        graph.via(BatchN(maxBatchSize, maxBatchCount))

      def dropIf(count: Int)(condition: A => Boolean): U#Repr[A] =
        PekkoUtil.dropIf(graph, count, condition)
    }
    // Use separate implicit conversions for Sources and Flows to help IntelliJ
    // Otherwise IntelliJ gets very resource hungry.
    implicit def pekkoUtilSyntaxForFlowOpsSource[A, Mat](
        graph: Source[A, Mat]
    ): PekkoUtilSyntaxForFlowOps[A, Mat, graph.type] =
      new PekkoUtilSyntaxForFlowOps(graph)
    implicit def pekkoUtilSyntaxForFlowOpsFlow[A, B, Mat](
        graph: Flow[A, B, Mat]
    ): PekkoUtilSyntaxForFlowOps[B, Mat, graph.type] =
      new PekkoUtilSyntaxForFlowOps(graph)

    /** Defines extension methods for [[org.apache.pekko.stream.scaladsl.FlowOps]] with a [[org.apache.pekko.stream.KillSwitch]].
      * @see PekkoUtilSyntaxForFlowOps for an explanation of the type parameter U
      */
    private[util] class PekkoUtilSyntaxForFLowOpsWithKillSwitch[
        A,
        Mat,
        U <: FlowOps[WithKillSwitch[A], Mat],
    ](private val graph: U)
        extends AnyVal {
      def takeUntilThenDrain(condition: A => Boolean): U#Repr[WithKillSwitch[A]] =
        PekkoUtil.takeUntilThenDrain(graph, condition)
    }
    // Use separate implicit conversions for Sources and Flows to help IntelliJ
    // Otherwise IntelliJ gets very resource hungry.
    implicit def pekkoUtilSyntaxForFlowOpsWithKillSwitchSource[A, Mat](
        graph: Source[WithKillSwitch[A], Mat]
    ): PekkoUtilSyntaxForFLowOpsWithKillSwitch[A, Mat, graph.type] =
      new PekkoUtilSyntaxForFLowOpsWithKillSwitch(graph)
    implicit def pekkoUtilSyntaxForFlowOpsWithKillSwitchFlow[A, B, Mat](
        graph: Flow[A, WithKillSwitch[B], Mat]
    ): PekkoUtilSyntaxForFLowOpsWithKillSwitch[B, Mat, graph.type] =
      new PekkoUtilSyntaxForFLowOpsWithKillSwitch(graph)

    /** Defines extension methods for [[org.apache.pekko.stream.scaladsl.FlowOpsMat]] that map to the methods defined in this class.
      * @see PekkoUtilSyntaxForFlowOps for an explanation of the type parameter U
      */
    private[util] class PekkoUtilSyntaxForFlowOpsMat[A, Mat, U <: FlowOpsMat[A, Mat]](
        private val graph: U
    ) extends AnyVal {

      private[util] def withMaterializedValueMat[M, Mat2](create: => M)(
          mat: (Mat, M) => Mat2
      ): U#ReprMat[(A, M), Mat2] =
        PekkoUtil.withMaterializedValueMat(create)(graph)(mat)

      def withUniqueKillSwitchMat[Mat2](
      )(mat: (Mat, UniqueKillSwitch) => Mat2): U#ReprMat[WithKillSwitch[A], Mat2] =
        PekkoUtil.withUniqueKillSwitch(graph)(mat)

      def injectKillSwitch(killSwitch: Mat => KillSwitch): U#ReprMat[WithKillSwitch[A], Mat] =
        PekkoUtil.injectKillSwitch(graph)(killSwitch)
    }
    // Use separate implicit conversions for Sources and Flows to help IntelliJ
    // Otherwise IntelliJ gets very resource hungry.
    implicit def pekkoUtilSyntaxForFlowOpsMatSource[A, Mat](
        graph: Source[A, Mat]
    ): PekkoUtilSyntaxForFlowOpsMat[A, Mat, graph.type] =
      new PekkoUtilSyntaxForFlowOpsMat(graph)
    implicit def pekkoUtilSyntaxForFlowOpsMat[A, B, Mat](
        graph: Flow[A, B, Mat]
    ): PekkoUtilSyntaxForFlowOpsMat[B, Mat, graph.type] =
      new PekkoUtilSyntaxForFlowOpsMat(graph)

    /** Extension method to contextualize a source over a single type constructor.
      * For complex type expressions, use [[ContextualizedFlowOps.contextualize]] with an explicit type argument.
      */
    implicit class PekkoUtilSyntaxContextualizeSource[Context[+_], A, Mat](
        private val source: Source[Context[A], Mat]
    ) extends AnyVal {
      def contextualize: ContextualizedSource[Context, A, Mat] =
        ContextualizedFlowOps.contextualize[Context](source)
    }

    /** Extension method to contextualize a flow over a single type constructor.
      * For complex type expressions, use [[ContextualizedFlowOps.contextualize]] with an explicit type argument.
      */
    implicit class PekkoUtilSyntaxContextualizeFlow[Context[+_], A, B, Mat](
        private val flow: Flow[A, Context[B], Mat]
    ) extends AnyVal {
      def contextualize: ContextualizedFlow[Context, A, B, Mat] =
        ContextualizedFlowOps.contextualize[Context](flow)
    }

    /** Extension methods on contextualized FlowOps */
    private[util] class PekkoUtilSyntaxForContextualizedFlowOps[
        Context[+_],
        A,
        Mat,
        U <: ContextualizedFlowOps[Context, A, Mat],
        C,
    ](private val graph: U)(implicit Context: SingletonTraverse.Aux[Context, C]) {
      def statefulMapAsyncContextualizedUS[S, B](
          initial: S
      )(f: (S, C, A) => FutureUnlessShutdown[(S, B)])(implicit
          loggingContext: NamedLoggingContext
      ): U#Repr[Context[UnlessShutdown[B]]] =
        PekkoUtil.statefulMapAsyncContextualizedUS(graph, initial)(f)(loggingContext, Context)
    }
    // Use separate implicit conversions for Sources and Flows to help IntelliJ
    // Otherwise IntelliJ gets very resource hungry.
    implicit def pekkoUtilSyntaxForContextualizedSource[Context[+_], A, Mat, C](
        graph: ContextualizedSource[Context, A, Mat]
    )(implicit context: SingletonTraverse.Aux[Context, C]): PekkoUtilSyntaxForContextualizedFlowOps[
      Context,
      A,
      Mat,
      ContextualizedSource[Context, A, Mat],
      C,
    ] = new PekkoUtilSyntaxForContextualizedFlowOps(graph)
    implicit def pekkoUtilSyntaxForContextualizedFlow[Context[+_], A, B, Mat, C](
        graph: ContextualizedFlow[Context, A, B, Mat]
    )(implicit context: SingletonTraverse.Aux[Context, C]): PekkoUtilSyntaxForContextualizedFlowOps[
      Context,
      B,
      Mat,
      ContextualizedFlow[Context, A, B, Mat],
      C,
    ] = new PekkoUtilSyntaxForContextualizedFlowOps(graph)
  }

  type ContextualizedFlowOps[+Context[+_], +A, +Mat] =
    ContextualizedFlowOpsImpl.Instance.ContextualizedFlowOps[Context, A, Mat]
  type ContextualizedSource[+Context[+_], +A, +Mat] =
    ContextualizedFlowOpsImpl.Instance.ContextualizedSource[Context, A, Mat]
  type ContextualizedFlow[+Context[+_], -A, +B, +Mat] =
    ContextualizedFlowOpsImpl.Instance.ContextualizedFlow[Context, A, B, Mat]

  sealed abstract class ContextualizedFlowOpsImpl {
    type ContextualizedFlowOps[+Context[+_], +A, +Mat] <: FlowOps[Context[A], Mat]
    type ContextualizedSource[+Context[+_], +A, +Mat] <: Source[Context[A], Mat] &
      ContextualizedFlowOps[Context, A, Mat]
    type ContextualizedFlow[+Context[+_], -A, +B, +Mat] <: Flow[A, Context[B], Mat] &
      ContextualizedFlowOps[Context, B, Mat]

    def substFlowOps[K[_[_]], Context[+_], Mat](
        ff: K[Lambda[a => FlowOps[Context[a], Mat]]]
    ): K[ContextualizedFlowOps[Context, *, Mat]]

    def substSource[K[_[_]], Context[+_], Mat](
        ff: K[Lambda[a => Source[Context[a], Mat]]]
    ): K[ContextualizedSource[Context, *, Mat]]

    def substFlow[K[_[_]], Context[+_], A, Mat](
        ff: K[Lambda[b => Flow[A, Context[b], Mat]]]
    ): K[ContextualizedFlow[Context, A, *, Mat]]
  }
  object ContextualizedFlowOps {
    def contextualize[Context[+_]]: ContextualizePartiallyApplied[Context] =
      new ContextualizePartiallyApplied[Context]

    final class ContextualizePartiallyApplied[Context[+_]](private val dummy: Boolean = false)
        extends AnyVal {
      def apply[A, Mat](
          flowOps: FlowOps[Context[A], Mat]
      ): ContextualizedFlowOps[Context, A, Mat] = {
        type K[T[_]] = Id[T[A]]
        ContextualizedFlowOpsImpl.Instance.substFlowOps[K, Context, Mat](flowOps)
      }

      def apply[A, Mat](source: Source[Context[A], Mat]): ContextualizedSource[Context, A, Mat] = {
        type K[T[_]] = Id[T[A]]
        ContextualizedFlowOpsImpl.Instance.substSource[K, Context, Mat](source)
      }

      def apply[A, B, Mat](
          flow: Flow[A, Context[B], Mat]
      ): ContextualizedFlow[Context, A, B, Mat] = {
        type K[T[_]] = Id[T[B]]
        ContextualizedFlowOpsImpl.Instance.substFlow[K, Context, A, Mat](flow)
      }
    }
  }

  object ContextualizedFlowOpsImpl {
    val Instance: ContextualizedFlowOpsImpl = new ContextualizedFlowOpsImpl {
      override type ContextualizedFlowOps[+Context[+_], +A, +Mat] = FlowOps[Context[A], Mat]
      override type ContextualizedSource[+Context[+_], +A, +Mat] = Source[Context[A], Mat]
      override type ContextualizedFlow[+Context[+_], -A, +B, +Mat] = Flow[A, Context[B], Mat]

      override def substFlowOps[K[_[_]], Context[+_], Mat](
          ff: K[Lambda[a => FlowOps[Context[a], Mat]]]
      ): K[ContextualizedFlowOps[Context, *, Mat]] = ff

      override def substSource[K[_[_]], Context[+_], Mat](
          ff: K[Lambda[a => Source[Context[a], Mat]]]
      ): K[ContextualizedSource[Context, *, Mat]] = ff

      override def substFlow[K[_[_]], Context[+_], A, Mat](
          ff: K[Lambda[b => Flow[A, Context[b], Mat]]]
      ): K[ContextualizedFlow[Context, A, *, Mat]] = ff
    }
  }
}
