// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.platform.store.backend.DBLockStorageBackend
import com.digitalasset.canton.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.KillSwitch

import java.sql.Connection
import java.util.Timer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** A handle of a running program
  * @param completed will complete right after the program completed
  *                  - if no KillSwitch used,
  *                    - it will complete successfully as program successfully ends
  *                    - it will complete with the same failure that failed the program
  *                  - if KillSwitch aborted, this completes with the same Throwable
  *                  - if KillSwitch shut down, this completes successfully
  *                  After signalling completion, the program finished it's execution and has released all resources it acquired.
  * @param killSwitch to signal abortion and shutdown
  */
final case class Handle(completed: Future[Unit], killSwitch: KillSwitch)

/** This functionality initializes a worker Connection, and clears it for further usage.
  * This only needs to be done once at the beginning of the Connection life-cycle
  * Initialization errors are signaled by throwing an exception.
  */
trait ConnectionInitializer {
  def initialize(connection: Connection): Unit
}

/** To add High Availability related features to a program, which intends to use database-connections to do it's work.
  * Features include:
  *   - Safety: mutual exclusion of these programs ensured by DB locking mechanisms
  *   - Availability: release of the exclusion is detected by idle programs, which start competing for the lock to do
  *     their work.
  */
trait HaCoordinator {

  /** Execute in High Availability mode.
    * Wraps around the Handle of the execution.
    *
    * @param initializeExecution HaCoordinator provides a ConnectionInitializer that must to be used for all database connections during execution
    *                            Future[Handle] embodies asynchronous initialization of the execution
    *                            (e.g. not the actual work. That asynchronous execution completes with the completed Future of the Handle)
    * @return the new Handle, which is available immediately to observe and interact with the complete program here
    */
  def protectedExecution(initializeExecution: ConnectionInitializer => Future[Handle]): Handle
}

final case class HaConfig(
    mainLockAcquireRetryTimeout: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMillis(500),
    workerLockAcquireRetryTimeout: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMillis(500),
    workerLockAcquireMaxRetries: NonNegativeLong = NonNegativeLong.tryCreate(1000),
    mainLockCheckerPeriod: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(1000),
    mainLockCheckerJdbcNetworkTimeout: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofMillis(10000),
    indexerLockId: Int = 0x646d6c0, // note 0x646d6c equals ASCII encoded "dml"
    indexerWorkerLockId: Int = 0x646d6c1,
)

object HaCoordinator {

  /** This implementation of the HaCoordinator
    * - provides a database lock based isolation of the protected executions
    * - will run the execution at-most once during the entire lifecycle
    * - will wait infinitely to acquire the lock needed to start the protected execution
    * - provides a ConnectionInitializer function which is mandatory to execute on all worker connections during execution
    * - will spawn a polling-daemon to observe continuous presence of the main lock
    *
    * @param mainConnectionFactory to spawn the main connection which keeps the Indexer Main Lock
    * @param storageBackend is the database-independent abstraction of session/connection level database locking
    * @param executionContext which is use to execute initialisation, will do blocking/IO work, so dedicated execution context is recommended
    */
  def databaseLockBasedHaCoordinator(
      mainConnectionFactory: () => Connection,
      storageBackend: DBLockStorageBackend,
      executionContext: ExecutionContext,
      timer: Timer,
      haConfig: HaConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext): HaCoordinator = {
    implicit val ec: ExecutionContext = executionContext

    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    val indexerLockId = storageBackend.lock(haConfig.indexerLockId)
    val indexerWorkerLockId = storageBackend.lock(haConfig.indexerWorkerLockId)
    val preemptableSequence = PreemptableSequence(timer, loggerFactory)

    new HaCoordinator {
      override def protectedExecution(
          initializeExecution: ConnectionInitializer => Future[Handle]
      ): Handle = {
        def acquireLock(connection: Connection, lockId: LockId, lockMode: LockMode): Lock = {
          logger.debug(s"Acquiring lock $lockId $lockMode")
          storageBackend
            .tryAcquire(lockId, lockMode)(connection)
            .getOrElse(
              throw new CannotAcquireLockException(lockId, lockMode)
            )
        }

        def acquireMainLock(connection: Connection): Unit =
          acquireLock(connection, indexerLockId, LockMode.Exclusive).discard

        preemptableSequence.executeSequence { sequenceHelper =>
          import sequenceHelper.*
          logger.info("Starting IndexDB HA Coordinator")
          for {
            mainConnection <- go[Connection](mainConnectionFactory())
            _ = logger.debug("Step 1: creating main-connection - DONE")
            _ = registerRelease {
              logger.debug("Releasing main connection...")
              mainConnection.close()
              logger.debug("Step 8: Released main connection")
              logger.info("Stepped down as leader, IndexDB HA Coordinator shut down")
            }
            _ = logger.info("Waiting to be elected as leader")
            _ <- retry(
              waitMillisBetweenRetries = haConfig.mainLockAcquireRetryTimeout.duration.toMillis,
              retryable = _.isInstanceOf[CannotAcquireLockException],
            )(acquireMainLock(mainConnection))
            _ = logger.info("Elected as leader: starting initialization")
            _ = logger.info("Waiting for previous IndexDB HA Coordinator to finish work")
            _ = logger.debug(
              "Step 2: acquire exclusive Indexer Main Lock on main-connection - DONE"
            )
            exclusiveWorkerLock <- retry[Lock](
              waitMillisBetweenRetries = haConfig.workerLockAcquireRetryTimeout.duration.toMillis,
              maxAmountOfRetries = haConfig.workerLockAcquireMaxRetries.unwrap,
              retryable = _.isInstanceOf[CannotAcquireLockException],
            )(acquireLock(mainConnection, indexerWorkerLockId, LockMode.Exclusive))
            _ = logger.info(
              "Previous IndexDB HA Coordinator finished work, starting DB connectivity polling"
            )
            _ = logger.debug(
              "Step 3: acquire exclusive Indexer Worker Lock on main-connection - DONE"
            )
            _ <- go(storageBackend.release(exclusiveWorkerLock)(mainConnection))
            _ = logger.debug(
              "Step 4: release exclusive Indexer Worker Lock on main-connection - DONE"
            )
            mainLockChecker <- go[PollingChecker](
              new PollingChecker(
                periodMillis = haConfig.mainLockCheckerPeriod.duration.toMillis,
                checkBody = acquireMainLock(mainConnection),
                killSwitch =
                  handle.killSwitch, // meaning: this PollingChecker will shut down the main preemptableSequence
                loggerFactory = loggerFactory,
              )
            )
            _ = logger.debug(
              "Step 5: activate periodic checker of the exclusive Indexer Main Lock on the main connection - DONE"
            )
            _ = registerRelease {
              logger.debug(
                "Releasing periodic checker of the exclusive Indexer Main Lock on the main connection..."
              )
              logger.info("Stepping down as leader, stopping DB connectivity polling")
              mainLockChecker.close()
              logger.debug(
                "Step 7: Released periodic checker of the exclusive Indexer Main Lock on the main connection"
              )
            }
            protectedHandle <- goF(initializeExecution(workerConnection => {
              // this is the checking routine on connection creation
              // step 1: acquire shared worker-lock
              logger.debug(s"Preparing worker connection. Step 1: acquire lock.")
              acquireLock(workerConnection, indexerWorkerLockId, LockMode.Shared).discard
              // step 2: check if main connection still holds the lock
              logger.debug(s"Preparing worker connection. Step 2: checking main lock.")
              mainLockChecker.check()
              logger.debug(s"Preparing worker connection DONE.")
            }))
            _ = logger.debug("Step 6: initialize protected execution - DONE")
            _ = logger.info("Elected as leader: initialization complete")
            _ <- merge(protectedHandle)
          } yield ()
        }
      }
    }
  }

  class CannotAcquireLockException(lockId: LockId, lockMode: LockMode) extends RuntimeException {
    override def getMessage: String =
      s"Cannot acquire lock $lockId in lock-mode $lockMode"
  }
}

object NoopHaCoordinator extends HaCoordinator {
  override def protectedExecution(
      initializeExecution: ConnectionInitializer => Future[Handle]
  ): Handle =
    Await.result(initializeExecution(_ => ()), Duration.Inf)
}
