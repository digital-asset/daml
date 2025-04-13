// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.concurrent.Semaphore
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

/** Mix-in for keeping track of a set of the readers. Used for implementing the
  * [[HasSynchronizeWithClosing]] logic: Each computation acquires one permit before it starts and
  * released it when done. [[HasSynchronizeWithReaders.synchronizeWithReaders]] attempts to acquire
  * all permits and logs progress if slow.
  */
trait HasSynchronizeWithReaders extends HasRunOnClosing {

  import HasSynchronizeWithReaders.*

  protected[this] def logger: TracedLogger

  /** Track running computations on shutdown. Set to true to get stack traces about all computations
    * that did not complete during shutdown. If set to false, we don't do anything.
    */
  protected[this] def keepTrackOfReaderCallStack: Boolean

  protected[this] def synchronizeWithClosingPatience: FiniteDuration

  protected[this] def nameInternal: String

  if (keepTrackOfReaderCallStack) {
    logger.warn(
      s"Tracking of reader call stacks is enabled for '$nameInternal', but this is only meant for debugging!"
    )(TraceContext.empty)
  }

  /** Semaphore for all the [[HasSynchronizeWithClosing.synchronizeWithClosing]] calls. Each such
    * call obtains a permit for the time the computation is running. Upon closing,
    * [[synchronizeWithReaders]] grabs all permits and thereby prevents further calls from
    * succeeding.
    */
  private[this] val readerSemaphore: Semaphore = new Semaphore(Int.MaxValue)

  /** An underapproximation of the [[HasSynchronizeWithClosing.synchornizeWithClosing]] calls that
    * currently hold a [[readerSemaphore]] permit. Used for logging the calls that interfere with
    * closing for too long.
    */
  private[this] val readerUnderapproximation = new TrieMap[ReaderHandle, Unit]

  protected[this] def withReader[F[_], A](
      name: String
  )(f: => F[A])(implicit traceContext: TraceContext, F: Thereafter[F]): UnlessShutdown[F[A]] =
    addReader(name).map { handle =>
      Try(f) match {
        case Success(fa) =>
          fa.thereafter { _ =>
            removeReader(handle)
          }
        case Failure(error) =>
          removeReader(handle)
          throw error
      }
    }

  /** TODO(#16601) Make this method private once PerformUnlessClosing doesn't need it any more
    */
  protected[this] def addReader(reader: String)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[ReaderHandle] =
    // Abort early if we are closing.
    // This prevents new readers from registering themselves so that eventually all permits become available
    // to grab in one go
    if (isClosing) AbortedDueToShutdown
    else if (readerSemaphore.tryAcquire()) {
      val locationO = Option.when(keepTrackOfReaderCallStack)(new ReaderCallStack)
      val handle = new ReaderHandle(reader, locationO)
      readerUnderapproximation.put(handle, ()).discard
      Outcome(handle)
    } else if (isClosing) {
      // We check again for closing because the closing may have been initiated concurrently since the previous check
      AbortedDueToShutdown
    } else {
      logger.error(
        s"All ${Int.MaxValue} reader locks for $nameInternal have been taken. Is there a memory/task leak somewhere?"
      )
      logger.debug(s"Currently registered readers: ${readerUnderapproximation.keys.mkString(",")}")
      throw new IllegalStateException(
        s"All ${Int.MaxValue} reader locks for '$nameInternal' have been taken."
      )
    }

  /** TODO(#16601) Make this method private once PerformUnlessClosing doesn't need it any more
    */
  protected[this] def removeReader(handle: ReaderHandle): Unit = {
    if (readerUnderapproximation.remove(handle).isEmpty) {
      throw new IllegalStateException(s"Reader $handle is unknown.")
    }
    readerSemaphore.release()
  }

  protected[this] def synchronizeWithReaders()(implicit traceContext: TraceContext): Boolean = {
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
            s"Timeout $synchronizeWithClosingPatience expired, but readers are still active. Shutting down forcibly."
          )
          logger.debug(s"Active readers: ${readerUnderapproximation.keys.mkString(",")}")
          dumpRunning()
          false
        } else {
          val readerCount = Int.MaxValue - readerSemaphore.availablePermits()
          val nextPatienceMillis =
            (patienceMillis * 2) min maxSleepMillis min timeLeft.toMillis
          logger.debug(
            s"At least $readerCount active readers prevent closing. Next log message in ${nextPatienceMillis}ms. Active readers: ${readerUnderapproximation.keys
                .mkString(",")}"
          )
          poll(nextPatienceMillis)
        }
      }
    }

    poll(initialReaderPollingPatience)
  }

  private def dumpRunning()(implicit traceContext: TraceContext): Unit =
    readerUnderapproximation.keys.foreach { handle =>
      // Only dump those for which we have recorded a stack trace
      handle.location.foreach { loc =>
        logger.debug(s"Computation '${handle.name}' is still running.", loc)
      }
    }

  protected[this] def remainingReaders(): Seq[String] =
    // If we were not able to synchronize with all readers above, then make another attempt
    // at grabbing all of them. Maybe they all have finished by now?
    if (!readerSemaphore.tryAcquire(Int.MaxValue)) {
      readerUnderapproximation.keys.map(_.name).toSeq
    } else Seq.empty
}

object HasSynchronizeWithReaders {
  private val zeroDuration: FiniteDuration =
    FiniteDuration(0, java.util.concurrent.TimeUnit.MILLISECONDS)

  /** How often to poll to check that all readers have completed. */
  private val maxSleepMillis: Long = 500

  /** How long to wait for all readers to finish initially, in milliseconds. */
  private val initialReaderPollingPatience: Long = 10L

  /** TODO(#16601) Make private once [[com.digitalasset.canton.lifecycle.PerformUnlessClosing]]
    * doesn't need the barebones methods any more
    */
  private[lifecycle] final class ReaderHandle(val name: String, val location: Option[Exception]) {
    override def toString: String = name
  }

  private final class ReaderCallStack extends Exception {
    override def toString: String = "Synchronization was initiated from the given stack trace:"
  }

  /** Logged upon forced shutdown. Pulled out a string here so that test log checking can refer to
    * it.
    */
  val forceShutdownStr = "Shutting down forcibly"
}
