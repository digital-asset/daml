// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import cats.Eval
import com.digitalasset.canton.lifecycle.{OnShutdownRunner, RunOnShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, LoggerUtil}

import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Try

/** A [[HealthElement]] maintains a health state and notifies [[HealthListener]]s whenever the state has changed.
  *
  * [[HealthElement]]s are refined in three dimensions
  * - They can be atomic maintaining their own state ([[AtomicHealthElement]]) or
  *   composite ([[CompositeHealthElement]]) aggregating the states of their dependencies
  * - The state can be refined to [[ToComponentHealthState]] with [[HealthQuasiComponent]]
  *   and further to [[ComponentHealthState]] with [[HealthComponent]].
  * - Whether they need to be closed on their own ([[CloseableHealthElement]]).
  *
  * The traits from each dimension can be mixed together to create the appropriate combination.
  * Do not mix several traits from the same dimension!
  */
trait HealthElement {
  import HealthElement.*
  import HealthElement.RefreshingState.*

  /** Name of the health element. Used for logging. */
  def name: String

  /** The set of currently registered listeners */
  private val listeners: concurrent.Map[HealthListener, Unit] =
    TrieMap.empty[HealthListener, Unit]

  /** Registers a listener that gets poked upon each change of this element's health state.
    *
    * @return Whether the listener was not registered before
    */
  def registerOnHealthChange(listener: HealthListener): Boolean = {
    val isNew = listeners.putIfAbsent(listener, ()).isEmpty
    if (isNew) listener.poke()(TraceContext.empty)
    isNew
  }

  /** Unregisters a listener.
    *
    * @return Whether the listener was registered before.
    */
  def unregisterOnHealthChange(listener: HealthListener): Boolean =
    listeners.remove(listener).isDefined

  /** The type of health states exposed by this component */
  type State
  protected def prettyState: Pretty[State]

  private lazy val internalState: AtomicReference[InternalState[State]] =
    new AtomicReference[InternalState[State]](InternalState(initialHealthState, Idle))

  /** Returns the current state */
  final def getState: State = internalState.get().state

  /** The initial state upon creation */
  protected def initialHealthState: State

  /** The state set when the [[associatedOnShutdownRunner]] closes */
  protected def closingState: State

  /** The [[com.digitalasset.canton.lifecycle.OnShutdownRunner]] associated with this object.
    *
    * When this [[com.digitalasset.canton.lifecycle.OnShutdownRunner]] closes, the health state permanently becomes [[closingState]]
    * and all listeners are notified about this.
    */
  protected def associatedOnShutdownRunner: OnShutdownRunner

  locally {
    import TraceContext.Implicits.Empty.*
    associatedOnShutdownRunner.runOnShutdown_(new RunOnShutdown {
      override def name: String = s"set-closing-state-of-${HealthElement.this.name}"
      override def done: Boolean = false
      override def run(): Unit = refreshState(Eval.now(closingState))
    })
  }

  protected def logger: TracedLogger

  /** Triggers a refresh of the component's state, using `newState` to determine the new state.
    * May return before the `newState` has been evaluated and the listeners have been poked.
    *
    * Note that listeners need not be poked about every state change;
    * it suffices that they are poked eventually after each state change.
    * So if there are frequent updates to the state, possibly from concurrent calls,
    * then the listeners may never see some intermediate states.
    */
  protected def refreshState(
      newState: Eval[State]
  )(implicit traceContext: TraceContext): Unit = {
    val previous = internalState.getAndUpdate {
      case InternalState(s, Idle) => InternalState(s, Refreshing)
      case InternalState(s, Refreshing) => InternalState(s, Poked(newState))
      case InternalState(s, Poked(_)) => InternalState(s, Poked(newState))
    }
    previous.refreshing match {
      case Idle => doRefresh(previous.state, newState)
      case Refreshing | Poked(_) =>
    }
  }

  @tailrec private def doRefresh(
      oldState: State,
      newState: Eval[State],
  )(implicit traceContext: TraceContext): Unit = {
    def errorOnIdle: Nothing = {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      ErrorUtil.internalError(
        new ConcurrentModificationException(s"State changed to $Idle while $Refreshing")
      )
    }
    // When we're closing, force the value to `closingState`.
    // This ensures that `closingState` is sticky.
    val newStateValue = if (associatedOnShutdownRunner.isClosing) closingState else newState.value
    logger.debug(s"Refreshing state of $name from $oldState to $newStateValue")

    val previous = internalState.getAndUpdate {
      case InternalState(_, Idle) => errorOnIdle
      case InternalState(_, Refreshing) => InternalState(newStateValue, Idle)
      case InternalState(_, Poked(_)) => InternalState(newStateValue, Refreshing)
    }
    if (previous.state != oldState) {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      ErrorUtil.internalError(
        new ConcurrentModificationException(
          s"State changed from $oldState to ${previous.state} while doRefresh was running"
        )
      )
    }
    if (newStateValue != oldState) {
      logStateChange(oldState, newStateValue)
      notifyListeners
    }
    previous.refreshing match {
      case Idle => errorOnIdle
      case Refreshing => ()
      case Poked(eval) => doRefresh(newStateValue, eval)
    }
  }

  private def logIfLongPokeTime(listener: HealthListener, start: Long)(implicit
      traceContext: TraceContext
  ): Unit = {
    val dur = Duration.fromNanos(System.nanoTime() - start)
    lazy val durationStr = LoggerUtil.roundDurationForHumans(dur)
    if (dur > 1.second) logger.warn(s"Listener ${listener.name} took $durationStr to run")
  }

  private def notifyListeners(implicit traceContext: TraceContext): Unit = {
    listeners.foreachEntry { (listener, _) =>
      logger.debug(s"Notifying listener ${listener.name} of health state change from $name")
      val start = System.nanoTime()
      Try(listener.poke()).failed.foreach { exception =>
        logger.error(s"Notification of ${listener.name} failed", exception)
      }
      logIfLongPokeTime(listener, start)
    }
  }

  private def logStateChange(
      oldState: State,
      newState: State,
  )(implicit traceContext: TraceContext): Unit = {
    implicit val prettyS: Pretty[State] = prettyState
    logger.info(show"${name.singleQuoted} is now in state $newState. Previous state was $oldState.")
  }
}

object HealthElement {

  /** The internal state of a [[HealthElement]] consists of the current health state `state` and state of the refreshing state machine */
  private final case class InternalState[+S](state: S, refreshing: RefreshingState[S])

  /** The states of the refreshing state machine implemented by [[HealthElement.refreshState]]
    * and [[HealthElement.doRefresh]].
    * - In [[RefreshingState.Idle]], nothing is happening.
    * - In [[RefreshingState.Refreshing]], the one thread that caused the transition
    *   from [[RefreshingState.Idle]] to [[RefreshingState.Refreshing]] is updating the state
    *   using the given `newState` method to obtain the new state.
    * - If another call to [[HealthElement.refreshState]] happens concurrently,
    *   the update is queued in state [[RefreshingState.Poked]] with the given `newState` method.
    * - Further calls to [[HealthElement.refreshState]] in state [[RefreshingState.Poked]]
    *   replace the previous [[RefreshingState.Poked]] state.
    * - When the thread performing the state update has finished and finds
    *   that there are queued updates ([[RefreshingState.Poked]]),
    *   it runs another update with the queued `newState` method.
    *   Otherwise, the state returns back to [[RefreshingState.Idle]].
    *
    * <pre>
    * ┌──────┐    refreshState       ┌────────────┐     refreshState   ┌──────────┐
    * │      ├───────────────────────►            ├────────────────────►          ├───────┐
    * │ Idle │                       │ Refreshing │                    │  Poked   │       │refresh
    * │      │                       │            │                    │ newState │       │State
    * │      ◄───────────────────────┤            ◄────────────────────┤          ◄───────┘
    * └──────┘    done refreshing    └────────────┘  done refreshing   └──────────┘
    * </pre>
    *
    * Listeners are notified after each state update that does change the state, even if further state updates are queued.
    * This ensures that continuous state changes propagate to the listeners timely.
    */
  private[HealthElement] sealed trait RefreshingState[+S] extends Product with Serializable
  private[HealthElement] object RefreshingState {
    case object Idle extends RefreshingState[Nothing]
    final case object Refreshing extends RefreshingState[Nothing]
    final case class Poked[S](newState: Eval[S]) extends RefreshingState[S]
  }
}
