// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.Connection
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.stream.KillSwitch
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.DBLockStorageBackend.{Lock, LockId, LockMode}
import com.daml.platform.store.backend.DBLockStorageBackend
import javax.sql.DataSource

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

case class Handle(completed: Future[Unit], killSwitch: KillSwitch)

trait SignConnection {
  def sign(connection: Connection): Unit
}

trait HaCoordinator {
  def protectedBlock(block: SignConnection => Future[Handle]): Handle
}

object HaCoordinator {

  private val logger = ContextualizedLogger.get(this.getClass)

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

  class PreemptableSequence(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ) {
    private val logger = ContextualizedLogger.get(this.getClass)

    private val delegateKillSwitch = new AtomicReference[Option[KillSwitch]](None)
    private val resultCompleted = Promise[Unit]()
    private val mutableKillSwitch = new AtomicReference[KillSwitch]()
    mutableKillSwitch.set(new CaptureKillSwitch(mutableKillSwitch))
    private val resultKillSwitch = new KillSwitch {
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
    private val resultHandle = Handle(resultCompleted.future, resultKillSwitch)

    private var releaseStack: List[() => Future[Unit]] = Nil

    def registerRelease(block: => Unit): Unit = synchronized {
      logger.info(s"Registered release function")
      releaseStack = (() => Future(block)) :: releaseStack
    }

    def goF[T](f: => Future[T]): Future[T] =
      mutableKillSwitch.get() match {
        case _: UsedKillSwitch =>
          // Failing Future here means we interrupt the Future sequencing.
          // The failure itself is not important, since the returning Handle-s completion-future-s result is overridden in case KillSwitch was used.
          logger.info(s"KillSwitch already used, interrupting sequence!")
          Future.failed(new Exception("UsedKillSwitch"))

        case _ =>
          f
      }

    def go[T](t: => T): Future[T] = goF[T](Future(t))

    def retry[T](waitMillisBetweenRetries: Long, maxAmountOfRetries: Long = -1)(
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
            // Note: this recursion is out of stack // TODO proof?
            retry(waitMillisBetweenRetries, maxAmountOfRetries - 1)(block)
          )
      }

    // this one is deliberately without go/goF usage, since the handle is already materialized: meaning, we need to wait for completion, and replay killSwitch events.
    def merge(handle: Handle): Future[Unit] = {
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

    private def waitFor(delayMillis: Long): Future[Unit] =
      go {
        logger.debug(s"Waiting $delayMillis millis.")
        Thread.sleep(delayMillis)
        logger.debug(s"Waited $delayMillis millis.")
      } // TODO improve with java Timer

    private def release: Future[Unit] = synchronized {
      releaseStack match {
        case Nil => Future.unit
        case x :: xs =>
          releaseStack = xs
          x().transformWith(_ => release)
      }
    }

    def executeSequence(f: Future[_]): Unit =
      f.transformWith(fResult => release.transform(_ => fResult)).onComplete {
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

    def handle: Handle = resultHandle
  }

  class PollingChecker(
      periodMillis: Long,
      checkBlock: => Unit,
      killSwitch: KillSwitch,
  )(implicit loggingContext: LoggingContext) {
    private val logger = ContextualizedLogger.get(this.getClass)

    private val timer = new Timer(true)

    private val lostMainConnectionEmulation = new AtomicBoolean(false)

    timer.scheduleAtFixedRate(
      new TimerTask { // TODO AtFixedRate: is that good here?
        override def run(): Unit = {
          Try(check())
          ()
        }
      },
      periodMillis,
      periodMillis,
    )

    // TODO uncomment this for main-connection-lost simulation
//    timer.schedule(
//      new TimerTask {
//        override def run(): Unit = lostMainConnectionEmulation.set(true)
//      },
//      20000,
//    )

    // TODO this is a cruel approach for ensuring single threaded usage of the mainConnection
    // TODO in theory this could have been made much more efficient: not enqueueing for a check of it's own, but collecting requests, and replying in batches. Although experiments show approx 1s until a full connection pool is initialized at first (the peek scenario) which might be enough, and which can leave this code very simple.
    def check(): Unit = synchronized {
      logger.debug(s"Checking...")
      Try(checkBlock) match {
        case Success(_) if !lostMainConnectionEmulation.get =>
          logger.debug(s"Check successful.")

        case Success(_) =>
          logger.info(
            s"Check failed due to lost-main-connection simulation. KillSwitch/abort called."
          )
          killSwitch.abort(
            new Exception(
              "Check failed due to lost-main-connection simulation. KillSwitch/abort called."
            )
          )
          throw new Exception("Check failed due to lost-main-connection simulation.")

        case Failure(ex) =>
          logger.info(s"Check failed (${ex.getMessage}). KillSwitch/abort called.")
          killSwitch.abort(new Exception("check failed, killSwitch aborted", ex))
          throw ex
      }
    }

    def close(): Unit = timer.cancel()
  }

  def databaseLockBasedHaCoordinator(
      dataSource: DataSource,
      storageBackend: DBLockStorageBackend,
      executionContext: ExecutionContext,
      mainLockAquireRetryMillis: Long,
      workerLockAquireRetryMillis: Long,
      workerLockAquireMaxRetry: Long,
      mainLockCheckerPeriodMillis: Long,
      indexerLock: LockId,
      indexerWorkerLock: LockId,
  )(implicit loggingContext: LoggingContext): HaCoordinator = {
    implicit val ec: ExecutionContext = executionContext
    asyncHandle =>
      val preemptableSequence = new PreemptableSequence
      import preemptableSequence._

      def acquireLock(connection: Connection, lockId: LockId, lockMode: LockMode): Lock = {
        logger.debug(s"Acquiring lock $lockId $lockMode")
        storageBackend
          .aquireImmediately(lockId, lockMode)(connection)
          .getOrElse(
            throw new Exception(s"Cannot acquire lock $lockId in lock-mode $lockMode: lock busy")
          )
      }

      def acquireMainLock(connection: Connection): Unit = {
        acquireLock(connection, indexerLock, LockMode.Exclusive)
        ()
      }

      logger.info("Starting databaseLockBasedHaCoordinator")
      executeSequence(for {
        mainConnection <- go[Connection](dataSource.getConnection)
        _ = logger.info("Step 1: creating main-connection - DONE")
        _ = registerRelease {
          logger.info("Releasing main connection...")
          mainConnection.close()
          logger.info("Released main connection")
        }
        _ <- retry(mainLockAquireRetryMillis)(acquireMainLock(mainConnection))
        _ = logger.info("Step 2: acquire exclusive Indexer Lock on main-connection - DONE")
        exclusiveWorkerLock <- retry[Lock](workerLockAquireRetryMillis, workerLockAquireMaxRetry)(
          acquireLock(mainConnection, indexerWorkerLock, LockMode.Exclusive)
        )
        _ = logger.info("Step 3: acquire exclusive Indexer Worker Lock on main-connection - DONE")
        _ <- go(storageBackend.release(exclusiveWorkerLock)(mainConnection))
        _ = logger.info("Step 4: release exclusive Indexer Worker Lock on main-connection - DONE")
        mainLockChecker <- go[PollingChecker](
          new PollingChecker(
            periodMillis = mainLockCheckerPeriodMillis,
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
          acquireLock(workerConnection, indexerWorkerLock, LockMode.Shared)
          // step 2: check if main connection still holds the lock
          logger.info(s"Preparing worker connection. Step 2: checking main lock.")
          mainLockChecker.check()
          logger.info(s"Preparing worker connection DONE.")
        }))
        _ = logger.info("Step 6: initialize protected block - DONE")
        _ <- merge(protectedHandle)
      } yield ())

      handle
  }
}

object NoopHaCoordinator extends HaCoordinator {
  override def protectedBlock(block: SignConnection => Future[Handle]): Handle =
    Await.result(block(_ => ()), Duration.Inf)
}
