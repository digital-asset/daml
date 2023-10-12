// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext

import java.util.{Timer, TimerTask}
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success}

/** PreemptableSequence is a helper to
  * - facilitate the execution of a sequence of Futures, which can be stopped or aborted
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
    * which will be ensured to run before the completion future of the handle completes.
    *
    * @param release the release lambda
    */
  def registerRelease(release: => Unit): Unit

  /** Wrap a CBN (lazy) Future, so it is only started if the PreemptableSequence is not yet aborted/shut down.
    *
    * @param f The lazy Future
    * @return the wrapped future
    */
  def goF[T](f: => Future[T]): Future[T]

  /** Wrap a CBN (lazy) synchronous function in a Future, which is only started if the PreemptableSequence is not yet aborted/shut down.
    *
    * @param body The lazy synchronous body
    * @return the wrapped future
    */
  def go[T](body: => T): Future[T]

  /** Wrap a synchronous call into a Future sequence, which
    * - will be preemptable
    * - will retry to execute the body if Exception-s thrown, and the exception is retryable
    *
    * @return the preemptable, retrying Future sequence
    */
  def retry[T](
      waitMillisBetweenRetries: Long,
      maxAmountOfRetries: Long = -1,
      retryable: Throwable => Boolean = _ => true,
  )(body: => T): Future[T]

  /** Delegate the preemptable-future sequence to another Handle
    * - the completion Future future of the PreemptableSequence will only finish after this Handle finishes,
    *   and previously registered release functions all completed
    * - KillSwitch events will be replayed to this handle
    * - In case of abort/shutdown the PreemptableSequence's completion result will conform to the KillSwitch usage,
    *   not to the completion of this handle (although it will wait for it naturally)
    *
    * @param handle The handle to delegate to
    * @return the completion of the Handle
    */
  def merge(handle: Handle): Future[Unit]

  /** The handle of the PreemptableSequence. This handle is available for sequence construction as well.
    * @return the Handle
    */
  def handle: Handle
}

object PreemptableSequence {

  /** @param executionContext this execution context will be used to:
    *   - execute future transformations
    *   - and encapsulate synchronous work in futures (this could be possibly blocking)
    *   Because of the possible blocking nature a dedicated pool is recommended.
    */
  def apply(timer: Timer, loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): PreemptableSequence = { sequence =>
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    val resultCompleted = Promise[Unit]()
    val killSwitchCaptor = new KillSwitchCaptor(loggerFactory)
    val resultHandle = Handle(resultCompleted.future, killSwitchCaptor)
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var releaseStack: List[() => Future[Unit]] = Nil

    val helper: SequenceHelper = new SequenceHelper {
      private def waitFor(delayMillis: Long): Future[Unit] = {
        val p = Promise[Unit]()
        timer.schedule(
          new TimerTask {
            override def run(): Unit = p.success(())
          },
          delayMillis,
        )
        goF(p.future)
      }

      override def registerRelease(release: => Unit): Unit = blocking(synchronized {
        logger.info(s"Registered release function")
        releaseStack = (() => Future(release)) :: releaseStack
      })

      override def goF[T](f: => Future[T]): Future[T] =
        killSwitchCaptor.state match {
          case _: KillSwitchCaptor.State.Used =>
            // Failing Future here means we interrupt the Future sequencing.
            // The failure itself is not important, since the returning Handle-s completion-future-s result is overridden in case KillSwitch was used.
            logger.info(s"KillSwitch already used, interrupting sequence!")
            Future.failed(new UsedKillSwitch)

          case _ =>
            f
        }

      override def go[T](body: => T): Future[T] = goF[T](Future(body))

      override def retry[T](
          waitMillisBetweenRetries: Long,
          maxAmountOfRetries: Long = -1,
          retryable: Throwable => Boolean = _ => true,
      )(body: => T): Future[T] =
        go(body).transformWith {
          case Success(t) => Future.successful(t)

          case Failure(ex) if retryable(ex) =>
            // since we check countdown to 0, starting from negative means unlimited retries
            if (maxAmountOfRetries == 0) {
              logger.warn(
                s"Maximum amount of retries reached ($maxAmountOfRetries). Failing permanently.",
                ex,
              )
              Future.failed(ex)
            } else {
              val retriesLeft =
                if (maxAmountOfRetries < 0) "unlimited"
                else maxAmountOfRetries - 1
              logger.debug(s"Retrying (retries left: $retriesLeft). Due to: ${ex.getMessage}")
              waitFor(waitMillisBetweenRetries).flatMap(_ =>
                // Note: this recursion is out of stack
                retry(waitMillisBetweenRetries, maxAmountOfRetries - 1, retryable)(body)
              )
            }

          case Failure(ex: UsedKillSwitch) =>
            Future.failed(ex)

          case Failure(ex) =>
            logger.warn(s"Failure not retryable.", ex)
            Future.failed(ex)
        }

      override def merge(handle: Handle): Future[Unit] = {
        logger.info(s"Delegating KillSwitch upon merge.")
        killSwitchCaptor.setDelegate(Some(handle.killSwitch))
        // for safety reasons. if between creation of that killSwitch and delegation there was a usage, we replay that after delegation (worst case multiple calls)
        killSwitchCaptor.state match {
          case KillSwitchCaptor.State.Shutdown =>
            logger.info(s"Replying ShutDown after merge.")
            handle.killSwitch.shutdown()
          case KillSwitchCaptor.State.Aborted(ex) =>
            logger.info(s"Replaying abort (${ex.getMessage}) after merge.")
            handle.killSwitch.abort(ex)
          case _ => ()
        }
        handle.completed
          .transform { r =>
            // not strictly needed for this use case, but in theory multiple preemptable stages are possible after each other
            // this is needed to remove the delegation of the killSwitch after stage is complete
            killSwitchCaptor.setDelegate(None)
            r
          }
      }

      override def handle: Handle = resultHandle
    }

    def release: Future[Unit] = blocking(synchronized {
      releaseStack match {
        case Nil => Future.unit
        case x :: xs =>
          releaseStack = xs
          x().transformWith(_ => release)
      }
    })

    sequence(helper).transformWith(fResult => release.transform(_ => fResult)).onComplete {
      case Success(_) =>
        killSwitchCaptor.state match {
          case KillSwitchCaptor.State.Shutdown => resultCompleted.success(())
          case KillSwitchCaptor.State.Aborted(ex) => resultCompleted.failure(ex)
          case _ => resultCompleted.success(())
        }
      case Failure(ex) =>
        killSwitchCaptor.state match {
          case KillSwitchCaptor.State.Shutdown => resultCompleted.success(())
          case KillSwitchCaptor.State.Aborted(ex) => resultCompleted.failure(ex)
          case _ => resultCompleted.failure(ex)
        }
    }

    resultHandle
  }

  class UsedKillSwitch extends Exception("UsedKillSwitch")
}
