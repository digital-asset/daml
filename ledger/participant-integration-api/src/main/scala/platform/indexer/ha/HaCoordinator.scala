// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.Connection

import akka.actor.Scheduler
import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import com.daml.platform.store.backend.DBLockStorageBackend
import javax.sql.DataSource

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** A handle of a running program
  * @param completed will complete as the program completes
  *                  - if no KillSwitch used,
  *                    - it will complete successfully as program successfully ends
  *                    - it will complete with a failure as program failed
  *                  - if KillSwitch aborted, this completes with the same Throwable
  *                  - if KillSwitch shut down, this completes successfully
  *                  As this completes, the program finished it's execution, also all resources released as well.
  * @param killSwitch to signal abortion and shutdown
  */
case class Handle(completed: Future[Unit], killSwitch: KillSwitch)

/** This functionality sign off a worker Connection, anc clears it for further usage.
  * This only need to be done once at the beginning of the Connection life-cycle
  * On any error an exception will be thrown
  */
trait SignConnection {
  def sign(connection: Connection): Unit
}

/** To add High Availability related features to a program
  */
trait HaCoordinator {

  /** Execute block in High Availability mode.
    * Wraps around the Handle of the block.
    *
    * @param block HaCoordinator provides a SignConnection which need to be used for all database connections to do work in the block
    *              Future[Handle] embodies asynchronous initialisation of the block
    *              (e.g. not the actual work. That asynchronous execution completes with the completed Future of the Handle)
    * @return the new Handle, which is available immediately to observe and interact with the complete program here
    */
  def protectedBlock(block: SignConnection => Future[Handle]): Handle
}

case class HaConfig(
    mainLockAquireRetryMillis: Long = 500,
    workerLockAquireRetryMillis: Long = 500,
    workerLockAquireMaxRetry: Long = 1000,
    mainLockCheckerPeriodMillis: Long = 1000,
    indexerLockId: Int = 100,
    indexerWorkerLockId: Int = 101,
)

object HaCoordinator {

  private val logger = ContextualizedLogger.get(this.getClass)

  /** This implementation of the HaCoordinator
    * - provides a database lock based isolation of the protected blocks
    * - will run the block at-most once during the entire lifecycle
    * - will wait infinitely to acquire the lock needed to start the protected block
    * - provides a SignConnection function which is mandatory to execute on all worker connections inside of the block
    * - will spawn a polling-daemon to observe continuous presence of the main lock
    *
    * @param dataSource to spawn the main connection which keeps the Indexer Lock
    * @param storageBackend is the database-independent abstraction of session/connection level database locking
    * @param executionContext which is use to execute initialisation, will do blocking/IO work, so dedicated execution context is recommended
    */
  def databaseLockBasedHaCoordinator(
      dataSource: DataSource,
      storageBackend: DBLockStorageBackend,
      executionContext: ExecutionContext,
      scheduler: Scheduler,
      haConfig: HaConfig,
  )(implicit loggingContext: LoggingContext): HaCoordinator = {
    implicit val ec: ExecutionContext = executionContext

    val indexerLockId = storageBackend.lock(haConfig.indexerLockId)
    val indexerWorkerLockId = storageBackend.lock(haConfig.indexerWorkerLockId)
    val preemptableSequence = PreemptableSequence(scheduler)

    asyncHandle =>
      def acquireLock(connection: Connection, lockId: LockId, lockMode: LockMode): Lock = {
        logger.debug(s"Acquiring lock $lockId $lockMode")
        storageBackend
          .aquireImmediately(lockId, lockMode)(connection)
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
          _ = logger.info("Step 2: acquire exclusive Indexer Lock on main-connection - DONE")
          exclusiveWorkerLock <- retry[Lock](
            haConfig.workerLockAquireRetryMillis,
            haConfig.workerLockAquireMaxRetry,
          )(
            acquireLock(mainConnection, indexerWorkerLockId, LockMode.Exclusive)
          )
          _ = logger.info("Step 3: acquire exclusive Indexer Worker Lock on main-connection - DONE")
          _ <- go(storageBackend.release(exclusiveWorkerLock)(mainConnection))
          _ = logger.info("Step 4: release exclusive Indexer Worker Lock on main-connection - DONE")
          mainLockChecker <- go[PollingChecker](
            new PollingChecker(
              periodMillis = haConfig.mainLockCheckerPeriodMillis,
              checkBlock = acquireMainLock(mainConnection),
              killSwitch =
                handle.killSwitch, // meaning: this PollingChecker will shut down the main preemptableSequence
            )
          )
          _ = logger.info(
            "Step 5: activate periodic checker of the exclusive Indexer Lock on the main connection - DONE"
          )
          _ = registerRelease {
            logger.info(
              "Releasing periodic checker of the exclusive Indexer Lock on the main connection..."
            )
            mainLockChecker.close()
            logger.info(
              "Released periodic checker of the exclusive Indexer Lock on the main connection"
            )
          }
          protectedHandle <- goF(asyncHandle(workerConnection => {
            // this is the checking routine on connection creation
            // step 1: acquire shared worker-lock
            logger.info(s"Preparing worker connection. Step 1: acquire lock.")
            acquireLock(workerConnection, indexerWorkerLockId, LockMode.Shared)
            // step 2: check if main connection still holds the lock
            logger.info(s"Preparing worker connection. Step 2: checking main lock.")
            mainLockChecker.check()
            logger.info(s"Preparing worker connection DONE.")
          }))
          _ = logger.info("Step 6: initialize protected block - DONE")
          _ <- merge(protectedHandle)
        } yield ()
      }
  }
}

object NoopHaCoordinator extends HaCoordinator {
  override def protectedBlock(block: SignConnection => Future[Handle]): Handle =
    Await.result(block(_ => ()), Duration.Inf)
}
