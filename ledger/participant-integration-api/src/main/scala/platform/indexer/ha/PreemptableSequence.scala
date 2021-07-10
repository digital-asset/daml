// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.util.concurrent.atomic.AtomicReference

import akka.actor.Scheduler
import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** PreemptableSequence is a helper to
  * - facilitate a Future sequence, which can be stopped or aborted
  * - provide a Handle for the client
  * - manage the state to implement the above
  */
trait PreemptableSequence {

  /** Execute the preemptable sequence
    *
    * @param sequence This Future sequence needs to be constructed with the help of the SequenceHelper functions.
    * @return the Handle, to observe and to interact with the sequence.
    *         - The completion future will only complete as soon the sequence and all registered release functionality finished as well
    *         - The Handle is available immediately
    */
  def executeSequence(sequence: SequenceHelper => Future[_]): Handle
}

/** A collection of helper functions to compose a preemptable-sequence
  */
trait SequenceHelper {

  /** Register at any point in time a synchronous release function,
    * which will be ensured to run before completion future of the handle completes.
    *
    * @param block the release lambda
    */
  def registerRelease(block: => Unit): Unit

  /** Wrap a CBN (lazy) Future, so it is only started if the PreemptableSequence is not yet aborted/shut down.
    *
    * @param f The lazy Future block
    * @return the wrapped future
    */
  def goF[T](f: => Future[T]): Future[T]

  /** Wrap a CBN (lazy) synchronous function in a Future, which is only started if the PreemptableSequence is not yet aborted/shut down.
    *
    * @param t The lazy synchronous block
    * @return the wrapped future
    */
  def go[T](t: => T): Future[T]

  /** Wrap a synchronous block into a Future sequence, which
    * - will be preemptable
    * - will retry to execute a block if Exception-s thrown
    *
    * @return the preemptable, retrying Future sequence
    */
  def retry[T](waitMillisBetweenRetries: Long, maxAmountOfRetries: Long = -1)(
      block: => T
  ): Future[T]

  /** Delegate the preemptable-future sequence to another Handle
    * - the completion Future future of the PreemptableSequence will only finish after this Hanlde finishes,
    *   and previously registered release functions all completed
    * - KillSwitch events will be replayed to this handle
    * - In case of abort/shutdown the PreemptableSequence's completion result will conform to the KillSwitch usage,
    *   not to the completion of this handle (although it will wait for it naturally)
    *
    * @param handle The handle to delegate to
    * @return the completion of the Handle
    */
  def merge(handle: Handle): Future[Unit]

  /** The handle of the PreemprableSequence. This handle is available for sequence construction as well.
    * @return the Handle
    */
  def handle: Handle
}

// these family of KillSwitch-es enable the behavior of recording the usage of the KillSwitch
// - Shutdown always wins: in scenarios like multiple abort and then a shutdown will always capture a shutdown,
//   even if additional aborts arrive after the shutdown. This is needed so that graceful-shutdown can stop possible
//   recovery scenarios.
// - Always the last abort wins.
trait UsedKillSwitch extends KillSwitch {
  override def shutdown(): Unit = ()
  override def abort(ex: Throwable): Unit = ()
}
case object ShutDownKillSwitch extends UsedKillSwitch
case class AbortedKillSwitch(ex: Throwable, _myReference: AtomicReference[KillSwitch])
    extends CaptureKillSwitch(_myReference)
class CaptureKillSwitch(myReference: AtomicReference[KillSwitch]) extends KillSwitch {
  override def shutdown(): Unit = myReference.set(ShutDownKillSwitch)
  override def abort(ex: Throwable): Unit = myReference.set(AbortedKillSwitch(ex, myReference))
}

object PreemptableSequence {
  private val logger = ContextualizedLogger.get(this.getClass)

  /** @param executionContext this execution context will be used to:
    *   - execute future transformations
    *   - and encapsulate synchronous work in futures (this could be possibly blocking)
    *   Because of the possible blocking nature a dedicated pool is recommended.
    */
  def apply(scheduler: Scheduler)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): PreemptableSequence = { sequence =>
    val delegateKillSwitch = new AtomicReference[Option[KillSwitch]](None)
    val resultCompleted = Promise[Unit]()
    val mutableKillSwitch = new AtomicReference[KillSwitch]()
    mutableKillSwitch.set(new CaptureKillSwitch(mutableKillSwitch))
    val resultKillSwitch = new KillSwitch {
      override def shutdown(): Unit = {
        logger.info("Shutdown called for PreemptableSequence!")
        mutableKillSwitch.get().shutdown()
        delegateKillSwitch.get().foreach { ks =>
          logger.info("Shutdown call delegated!")
          ks.shutdown()
        }
      }

      override def abort(ex: Throwable): Unit = {
        logger.info(s"Abort called for PreemptableSequence! (${ex.getMessage})")
        mutableKillSwitch.get().abort(ex)
        delegateKillSwitch.get().foreach { ks =>
          logger.info(s"Abort call delegated! (${ex.getMessage})")
          ks.abort(ex)
        }
      }
    }
    val resultHandle = Handle(resultCompleted.future, resultKillSwitch)
    var releaseStack: List[() => Future[Unit]] = Nil

    val helper: SequenceHelper = new SequenceHelper {
      private def waitFor(delayMillis: Long): Future[Unit] =
        goF(akka.pattern.after(FiniteDuration(delayMillis, "millis"), scheduler)(Future.unit))

      override def registerRelease(block: => Unit): Unit = synchronized {
        logger.info(s"Registered release function")
        releaseStack = (() => Future(block)) :: releaseStack
      }

      override def goF[T](f: => Future[T]): Future[T] =
        mutableKillSwitch.get() match {
          case _: UsedKillSwitch =>
            // Failing Future here means we interrupt the Future sequencing.
            // The failure itself is not important, since the returning Handle-s completion-future-s result is overridden in case KillSwitch was used.
            logger.info(s"KillSwitch already used, interrupting sequence!")
            Future.failed(new Exception("UsedKillSwitch"))

          case _ =>
            f
        }

      override def go[T](t: => T): Future[T] = goF[T](Future(t))

      override def retry[T](waitMillisBetweenRetries: Long, maxAmountOfRetries: Long)(
          block: => T
      ): Future[T] =
        go(block).transformWith {
          // since we check countdown to 0, starting from negative means unlimited retries
          case Failure(ex) if maxAmountOfRetries == 0 =>
            logger.info(
              s"Maximum amount of retries reached (${maxAmountOfRetries}) failing permanently. (${ex.getMessage})"
            )
            Future.failed(ex)
          case Success(t) => Future.successful(t)
          case Failure(ex) =>
            logger.debug(s"Retrying (retires left: ${if (maxAmountOfRetries < 0) "unlimited"
            else maxAmountOfRetries - 1}). Due to: ${ex.getMessage}")
            waitFor(waitMillisBetweenRetries).flatMap(_ =>
              // Note: this recursion is out of stack
              retry(waitMillisBetweenRetries, maxAmountOfRetries - 1)(block)
            )
        }

      override def merge(handle: Handle): Future[Unit] = {
        logger.info(s"Delegating KillSwitch upon merge.")
        delegateKillSwitch.set(Some(handle.killSwitch))
        // for safety reasons. if between creation of that killSwitch and delegation there was a usage, we replay that after delegation (worst case multiple calls)
        mutableKillSwitch.get() match {
          case ShutDownKillSwitch =>
            logger.info(s"Replying ShutDown after merge.")
            handle.killSwitch.shutdown()
          case AbortedKillSwitch(ex, _) =>
            logger.info(s"Replaying abort (${ex.getMessage}) after merge.")
            handle.killSwitch.abort(ex)
          case _ => ()
        }
        val result = handle.completed
        // not strictly needed for this use case, but in theory multiple preemptable stages are possible after each other
        // this is needed to remove the delegation of the killSwitch after stage is complete
        result.onComplete(_ => delegateKillSwitch.set(None))
        result
      }

      override def handle: Handle = resultHandle
    }

    def release: Future[Unit] = synchronized {
      releaseStack match {
        case Nil => Future.unit
        case x :: xs =>
          releaseStack = xs
          x().transformWith(_ => release)
      }
    }

    sequence(helper).transformWith(fResult => release.transform(_ => fResult)).onComplete {
      case Success(_) =>
        mutableKillSwitch.get() match {
          case ShutDownKillSwitch => resultCompleted.success(())
          case AbortedKillSwitch(ex, _) => resultCompleted.failure(ex)
          case _ => resultCompleted.success(())
        }
      case Failure(ex) =>
        mutableKillSwitch.get() match {
          case ShutDownKillSwitch => resultCompleted.success(())
          case AbortedKillSwitch(ex, _) => resultCompleted.failure(ex)
          case _ => resultCompleted.failure(ex)
        }
    }

    resultHandle
  }
}
