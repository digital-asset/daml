// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import com.digitalasset.canton.config.RequireTypes.Port
import org.slf4j.{Logger, LoggerFactory}

import java.nio.channels.{FileLock, OverlappingFileLockException}
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import java.time.Duration
import scala.annotation.tailrec
import scala.concurrent.blocking
import scala.util.*

/** Generates host-wide unique ports for canton tests that we guarantee won't be used in our tests.
  * Syncs with other processes' UniquePortGenerators via a file + exclusive file lock. Doesn't check
  * that the port hasn't been bound by other processes on the host.
  */
object UniquePortGenerator {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val PortRangeStart: Int = 30000
  val PortRangeEnd: Int = 65535

  private val SharedPortNumFile: File = File.temp / "canton_tests_unique_port_generator.dat"

  SharedPortNumFile.createFileIfNotExists(createParents = true)
  logger.debug(s"Initialized port file: ${SharedPortNumFile.path.toString}")

  private val counter = new UniqueBoundedCounter(
    dataFile = SharedPortNumFile.path,
    startValue = PortRangeStart,
    maxValue = PortRangeEnd,
  )(logger)

  /** Finds the next network port for use in canton tests.
    *
    * May throw an exception, in particular an instance of
    * [[java.nio.channels.OverlappingFileLockException]] when failing to get an exclusive file lock
    * after exhausting retries. (See [[com.digitalasset.canton.UniqueBoundedCounter#maxRetries]])
    *
    * @return
    *   unique port for canton tests, throws otherwise
    */
  def next: Port = {
    logger.debug("Attempting to find unique port ...")
    val start = System.nanoTime()
    val port = Port.tryCreate(counter.incrementAndGet().fold(throw _, identity))
    logger.debug(
      s"Found unique port $port after ${Duration.ofNanos(System.nanoTime() - start).toMillis} [ms]"
    )
    port
  }

}

/** A counter implementation, reading and writing the integer value to file.
  *
  * Synchronization uses a JVM-level synchronized block for *intra-process* thead safety, and within
  * that, an OS-level exclusive [[java.nio.channels.FileLock]] on a separate lock file for
  * inter-process safety (acquired via blocking lock()).
  *
  * Allows specifying initial/maximum values (with mandatory maximum for wrap-around) and includes
  * retry logic for the overall operation (if FileLock fails).
  *
  * Manages two files (data file and lock file).
  *
  * IMPORTANT: Consult [[java.nio.channels.FileLock]] before making changes.
  *
  * @param dataFile
  *   The path to the data file which stores the counter.
  * @param startValue
  *   The value to initialize the counter with if the data file is created or found empty/invalid.
  * @param maxValue
  *   The maximum value (inclusive). The counter wraps around to `initialValue` when its value
  *   exceeds `maximumValue`.
  * @param maxRetries
  *   Maximum times to retry the *entire* operation if certain exceptions (like
  *   OverlappingFileLockException from lock()) occur. Defaults to 100.
  * @param retryDelayMillis
  *   Delay between *entire* operation retries. Defaults to 300ms.
  * @param logger
  *   An SLF4J logger instance.
  */
class UniqueBoundedCounter(
    dataFile: File,
    startValue: Int,
    maxValue: Int,
    maxRetries: Int = 100,
    retryDelayMillis: Long = 300,
)(logger: Logger) {

  require(maxValue > startValue, s"maxValue $maxValue must be greater than startValue $startValue")

  private val lockFile: File = File(dataFile.pathAsString + ".lock")

  Try(lockFile.createIfNotExists(createParents = true)) match {
    case Success(_) => // OK
    case Failure(e: SecurityException) =>
      logger.error(s"Permission issue creating lock file '$lockFile': ${e.getMessage}")
      throw e
    case Failure(e) =>
      logger.error(s"Unexpected error creating lock file '$lockFile': ${e.getMessage}")
      throw e
  }

  def incrementAndGet(): Try[Int] = updateCounter(_ + 1)
  def get(): Try[Int] = updateCounter(identity)
  def addAndGet(delta: Int): Try[Int] = updateCounter(_ + delta)

  private def updateCounter(updateFn: Int => Int): Try[Int] = attemptWithRetries(updateFn, 1)

  @tailrec
  private def attemptWithRetries(operation: Int => Int, attempt: Int): Try[Int] = {

    val result: Try[Int] = perform(operation)

    result match {
      case Success(value) => Success(value)
      // Retry only on OverlappingFileLockException which might may occur from lock() due to inter-process contention
      case Failure(e: OverlappingFileLockException) =>
        if (attempt <= maxRetries) {
          logger.debug(
            s"Retrying operation due to OverlappingFileLockException (Attempt $attempt/$maxRetries), sleeping ${retryDelayMillis}ms ...",
            e,
          )
          blocking(Thread.sleep(retryDelayMillis))
          attemptWithRetries(operation, attempt + 1)
        } else {
          val retriesExhaustedErrorMessage =
            s"Operation failed after $maxRetries attempts due to OverlappingFileLockException on '$lockFile'."
          logger.error(retriesExhaustedErrorMessage, e)
          Failure(new RuntimeException(retriesExhaustedErrorMessage, e))
        }
      case Failure(other) =>
        // Other errors that are not retried, for example IO errors during mutation, potentially others from lock()
        val otherErrorMessage =
          s"Operation failed with non-retryable error. Lock file: '$lockFile' | Data file: '$dataFile'"
        logger.error(otherErrorMessage, other)
        Failure(new RuntimeException(otherErrorMessage, other))
    }
  }

  /** Performs a single attempt to acquire the file lock and executes the operation.
    *
    * It ensures
    *   - single thread attempts file locking (reduces lock file contention OS processes), and that
    *   - data file mutation is properly serialized (required as per [[java.nio.channels.FileLock]]
    *     JavaDoc).
    */
  private def perform(operation: Int => Int): Try[Int] = this.synchronized {
    blocking {
      Using(lockFile.newFileChannel(Seq(StandardOpenOption.WRITE, StandardOpenOption.CREATE))) {
        lockChannel =>
          var fileLock: FileLock = null
          try {
            // Acquire file lock using blocking lock()
            // This may block and throw OverlappingFileLockException if another OS process holds the lock.
            // This may also throw numerous other exceptions!
            logger.debug("Attempting to acquire file lock via blocking lock()...")
            fileLock = lockChannel.lock()
            logger.debug("Acquired file lock.")

            logger.debug("Mutating counter...")
            val dataAccessResult = mutateCounter(operation)
            logger.debug("Counter changed.")

            dataAccessResult.fold(throw _, identity)
          } finally {
            if (fileLock != null) {
              logger.debug("Releasing file lock.")
              Try(fileLock.release())
            } else {
              logger.debug(
                "Nothing to release. File lock was null because the attempt to acquire the file lock failed " +
                  "with an exception, most likely an OverlappingFileLockException has been thrown."
              )
            }
          }
      }
    }
  }

  /** Mutates the counter value in the data file based on the given update function.
    *
    * IMPORTANT: This method should only be called when the JVM lock (part of a synchronized block)
    * AND the file lock are held!
    */
  private def mutateCounter(updateFn: Int => Int): Try[Int] = Try {
    val currentValue = if (dataFile.isEmpty) {
      logger.debug(s"Data file empty, using initial value $startValue")
      startValue
    } else {
      dataFile.contentAsString(StandardCharsets.UTF_8).toInt
    }

    val potentialNewValue = updateFn(currentValue)
    val newValue = if (potentialNewValue > maxValue) startValue else potentialNewValue

    dataFile.overwrite(newValue.toString)(charset = StandardCharsets.UTF_8)

    newValue
  }

}
