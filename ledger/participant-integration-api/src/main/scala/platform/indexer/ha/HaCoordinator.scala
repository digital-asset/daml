// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.Connection
import java.util.Timer

import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import com.daml.platform.store.backend.DBLockStorageBackend
import javax.sql.DataSource

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
case class Handle(completed: Future[Unit], killSwitch: KillSwitch)

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

case class HaConfig(
    mainLockAquireRetryMillis: Long = 500,
    workerLockAquireRetryMillis: Long = 500,
    workerLockAquireMaxRetry: Long = 1000,
    mainLockCheckerPeriodMillis: Long = 1000,
    indexerLockId: Int = 0x646d6c00, // note 0x646d6c equals ASCII encoded "dml"
    indexerWorkerLockId: Int = 0x646d6c01,
    enable: Boolean = false, // TODO ha: remove as stable
)

object HaCoordinator {

  private val logger = ContextualizedLogger.get(this.getClass)

  /** This implementation of the HaCoordinator
    * - provides a database lock based isolation of the protected executions
    * - will run the execution at-most once during the entire lifecycle
    * - will wait infinitely to acquire the lock needed to start the protected execution
    * - provides a ConnectionInitializer function which is mandatory to execute on all worker connections during execution
    * - will spawn a polling-daemon to observe continuous presence of the main lock
    *
    * @param dataSource to spawn the main connection which keeps the Indexer Main Lock
    * @param storageBackend is the database-independent abstraction of session/connection level database locking
    * @param executionContext which is use to execute initialisation, will do blocking/IO work, so dedicated execution context is recommended
    */
  def databaseLockBasedHaCoordinator(
      dataSource: DataSource,
      storageBackend: DBLockStorageBackend,
      executionContext: ExecutionContext,
      timer: Timer,
      haConfig: HaConfig,
  )(implicit loggingContext: LoggingContext): HaCoordinator = {
    implicit val ec: ExecutionContext = executionContext

    val indexerLockId = storageBackend.lock(haConfig.indexerLockId)
    val indexerWorkerLockId = storageBackend.lock(haConfig.indexerWorkerLockId)
    val preemptableSequence = PreemptableSequence(timer)

    new HaCoordinator {
      override def protectedExecution(
          initializeExecution: ConnectionInitializer => Future[Handle]
      ): Handle = {
        def acquireLock(connection: Connection, lockId: LockId, lockMode: LockMode): Lock = {
          logger.debug(s"Acquiring lock $lockId $lockMode")
          storageBackend
            .tryAcquire(lockId, lockMode)(connection)
            .getOrElse(
              throw new Exception(s"Cannot acquire lock $lockId in lock-mode $lockMode: lock busy")
            )
        }

        def acquireMainLock(connection: Connection): Unit = {
          acquireLock(connection, indexerLockId, LockMode.Exclusive)
          ()
        }

        preemptableSequence.executeSequence { sequenceHelper =>
          import sequenceHelper._
          logger.info("Starting databaseLockBasedHaCoordinator")
          for {
            mainConnection <- go[Connection](dataSource.getConnection)
            _ = logger.info("Step 1: creating main-connection - DONE")
            _ = registerRelease {
              logger.info("Releasing main connection...")
              mainConnection.close()
              logger.info("Released main connection")
            }
            _ <- retry(haConfig.mainLockAquireRetryMillis)(acquireMainLock(mainConnection))
            _ = logger.info("Step 2: acquire exclusive Indexer Main Lock on main-connection - DONE")
            exclusiveWorkerLock <- retry[Lock](
              haConfig.workerLockAquireRetryMillis,
              haConfig.workerLockAquireMaxRetry,
            )(
              acquireLock(mainConnection, indexerWorkerLockId, LockMode.Exclusive)
            )
            _ = logger.info(
              "Step 3: acquire exclusive Indexer Worker Lock on main-connection - DONE"
            )
            _ <- go(storageBackend.release(exclusiveWorkerLock)(mainConnection))
            _ = logger.info(
              "Step 4: release exclusive Indexer Worker Lock on main-connection - DONE"
            )
            mainLockChecker <- go[PollingChecker](
              new PollingChecker(
                periodMillis = haConfig.mainLockCheckerPeriodMillis,
                checkBody = acquireMainLock(mainConnection),
                killSwitch =
                  handle.killSwitch, // meaning: this PollingChecker will shut down the main preemptableSequence
              )
            )
            _ = logger.info(
              "Step 5: activate periodic checker of the exclusive Indexer Main Lock on the main connection - DONE"
            )
            _ = registerRelease {
              logger.info(
                "Releasing periodic checker of the exclusive Indexer Main Lock on the main connection..."
              )
              mainLockChecker.close()
              logger.info(
                "Released periodic checker of the exclusive Indexer Main Lock on the main connection"
              )
            }
            protectedHandle <- goF(initializeExecution(workerConnection => {
              // this is the checking routine on connection creation
              // step 1: acquire shared worker-lock
              logger.info(s"Preparing worker connection. Step 1: acquire lock.")
              acquireLock(workerConnection, indexerWorkerLockId, LockMode.Shared)
              // step 2: check if main connection still holds the lock
              logger.info(s"Preparing worker connection. Step 2: checking main lock.")
              mainLockChecker.check()
              logger.info(s"Preparing worker connection DONE.")
            }))
            _ = logger.info("Step 6: initialize protected execution - DONE")
            _ <- merge(protectedHandle)
          } yield ()
        }
      }
    }
  }
}

object NoopHaCoordinator extends HaCoordinator {
  override def protectedExecution(
      initializeExecution: ConnectionInitializer => Future[Handle]
  ): Handle =
    Await.result(initializeExecution(_ => ()), Duration.Inf)
}
