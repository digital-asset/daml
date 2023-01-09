// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import java.sql.Connection
import java.util.Timer
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.KillSwitch
import akka.testkit.TestProbe
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.backend.DBLockStorageBackend
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Random, Try}

class HaCoordinatorSpec
    extends AsyncFlatSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with Eventually {
  implicit val ec: ExecutionContext =
    system.dispatcher // we need this to not use the default EC which is coming from AsyncTestSuite, and which is serial
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val logger = ContextualizedLogger.get(this.getClass)
  private val timer = new Timer(true)

  private val mainLockAcquireRetryMillis = 20L
  private val workerLockAcquireRetryMillis = 20L
  private val mainLockCheckerPeriodMillis = 20L
  private val timeoutToleranceMillis =
    600000L // unfortunately this needs to be a insanely big tolerance, not to render the test flaky. under normal circumstances this should pass with +5 millis

  private val mainLockId = 10
  private val main = TestLockId(mainLockId)

  private val workerLockId = 20
  private val worker = TestLockId(workerLockId)

  behavior of "databaseLockBasedHaCoordinator graceful shutdown"

  it should "successfully propagate successful end of execution" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      _ <- connectionInitializerFuture
      _ = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        completeExecutionInitialization()
        info("As protected execution initialization is finished")
        executionCompletedPromise.success(())
        info("As execution is completed")
      }
      _ <- protectedHandle.completed
    } yield {
      info("Protected Handle is completed successfully")
      1 shouldBe 1
    }
  }

  it should "successfully propagate failed end of execution" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      _ <- connectionInitializerFuture
      _ = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        completeExecutionInitialization()
        info("As protected execution initialization is finished")
        executionCompletedPromise.failure(new Exception("failed execution"))
        info("As execution completes with failure")
      }
      ex <- protectedHandle.completed.failed
    } yield {
      info("Protected Handle is completed with failure")
      ex.getMessage shouldBe "failed execution"
    }
  }

  it should "propagate graceful shutdown to the protected execution" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        completeExecutionInitialization()
        info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        protectedHandle.killSwitch.shutdown()
        info("And as graceful shutdown started")
      }
      _ <- executionShutdownFuture
      _ = {
        info("Shutdown is observed at execution")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        executionAbortedFuture.isCompleted shouldBe false
        info("Abort is not observed at execution")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle not completed")
        Thread.sleep(200)
        info("As waiting 200 millis")
        protectedHandle.completed.isCompleted shouldBe false
        info(
          "Protected Handle is still not completed (hence it is waiting for execution to finish as the first step of the teardown process)"
        )
        executionCompletedPromise.success(())
        info("As execution is completed")
      }
      _ <- protectedHandle.completed
    } yield {
      info("Protected Handle is completed successfully")
      Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
      info("Connection initializer does not work anymore")
      1 shouldBe 1
    }
  }

  it should "propagate graceful shutdown to the protected execution if shutdown initiated before execution initialization is finished" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        protectedHandle.killSwitch.shutdown()
        info("As graceful shutdown started")
        completeExecutionInitialization()
        info("And As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        info(
          "Connection initializer still works (release process first releases the execution, and only then the main connection and the poller)"
        )
      }
      _ <- executionShutdownFuture
      _ = {
        info("Shutdown is observed at execution")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        executionAbortedFuture.isCompleted shouldBe false
        info("Abort is not observed at execution")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle not completed")
        executionCompletedPromise.success(())
        info("As execution is completed")
      }
      _ <- protectedHandle.completed
    } yield {
      info("Protected Handle is completed successfully")
      Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
      info("Connection initializer does not work anymore")
      1 shouldBe 1
    }
  }

  it should "swallow failures if graceful shutdown is underway" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        completeExecutionInitialization()
        info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        protectedHandle.killSwitch.shutdown()
        info("And as graceful shutdown started")
      }
      _ <- executionShutdownFuture
      _ = {
        info("Shutdown is observed at execution")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        executionAbortedFuture.isCompleted shouldBe false
        info("Abort is not observed at execution")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle not completed")
        executionCompletedPromise.failure(new Exception("some exception"))
        info("As execution is completes with failure")
      }
      _ <- protectedHandle.completed
    } yield {
      info("Protected Handle is completed successfully")
      Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
      info("Connection initializer does not work anymore")
      1 shouldBe 1
    }
  }

  behavior of "databaseLockBasedHaCoordinator initialization"

  it should "fail if getting connection fails" in {
    val protectedSetup =
      setup(connectionFactory = () => throw new Exception("as getting connection"))
    import protectedSetup._

    for {
      result <- protectedHandle.completed.failed
    } yield {
      info("Protected Handle is completed with failure")
      result.getMessage shouldBe "as getting connection"
    }
  }

  it should "wait if main lock cannot be acquired" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    val blockingLock =
      dbLock.tryAcquire(main, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).get
    info("As acquiring the main lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup._
    info("As acquiring the main lock from the outside")
    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    dbLock.release(blockingLock)(blockingConnection) shouldBe true
    info("As releasing the blocking lock")

    for {
      _ <- connectionInitializerFuture
      _ = {
        info("Initialisation should completed successfully")
        completeExecutionInitialization() // cleanup
        executionCompletedPromise.success(()) // cleanup
      }
      _ <- protectedHandle.completed
    } yield {
      1 shouldBe 1
    }
  }

  it should "wait for main lock can be interrupted by graceful shutdown" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(main, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).get
    info("As acquiring the main lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup._
    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    protectedHandle.killSwitch.shutdown()
    info("As graceful shutdown started")

    for {
      _ <- protectedHandle.completed
    } yield {
      info("Protected Handle is completed successfully")
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "wait for main lock can be interrupted by exception thrown during tryAcquire" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(main, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).get
    info("As acquiring the main lock from the outside")
    val mainConnection = new TestConnection
    val protectedSetup = setup(
      dbLock = dbLock,
      connectionFactory = () => mainConnection,
    )
    import protectedSetup._
    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    mainConnection.close()
    info("As main connection is closed (triggers exception as used for acquiring lock)")

    for {
      failure <- protectedHandle.completed.failed
    } yield {
      info("Protected Handle is completed with a failure")
      failure.getMessage shouldBe "trying to acquire on a closed connection"
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "wait if worker lock cannot be acquired due to exclusive blocking" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    val blockingLock =
      dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).get
    info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup._
    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    dbLock.release(blockingLock)(blockingConnection) shouldBe true
    info("As releasing the blocking lock")

    for {
      _ <- connectionInitializerFuture
      _ = {
        info("Initialisation should completed successfully")
        completeExecutionInitialization() // cleanup
        executionCompletedPromise.success(()) // cleanup
      }
      _ <- protectedHandle.completed
    } yield {
      1 shouldBe 1
    }
  }

  it should "wait if worker lock cannot be acquired due to shared blocking" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    val blockingLock =
      dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).get
    info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup._

    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    dbLock.release(blockingLock)(blockingConnection) shouldBe true
    info("As releasing the blocking lock")

    for {
      _ <- connectionInitializerFuture
      _ = {
        info("Initialisation should completed successfully")
        completeExecutionInitialization() // cleanup
        executionCompletedPromise.success(()) // cleanup
      }
      _ <- protectedHandle.completed
    } yield {
      1 shouldBe 1
    }
  }

  it should "wait for worker lock can be interrupted by graceful shutdown" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).get
    info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup._

    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    protectedHandle.killSwitch.shutdown()
    info("As graceful shutdown starts")

    for {
      _ <- protectedHandle.completed
    } yield {
      info("Protected Handle completes successfully")
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "wait for worker lock can be interrupted by exception thrown during tryAcquire" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).get
    info("As acquiring the worker lock from the outside")
    val mainConnection = new TestConnection
    val protectedSetup = setup(
      dbLock = dbLock,
      connectionFactory = () => mainConnection,
    )
    import protectedSetup._

    Thread.sleep(200)
    info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    info("Initialization should be waiting")
    mainConnection.close()
    info("As main connection is closed (triggers exception as used for acquiring lock)")

    for {
      failure <- protectedHandle.completed.failed
    } yield {
      info("Protected Handle completed with a failure")
      failure.getMessage shouldBe "trying to acquire on a closed connection"
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "fail if worker lock cannot be acquired in time due to shared blocking" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).get
    info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(
      dbLock = dbLock,
      workerLockAcquireMaxRetry = 2,
    )
    import protectedSetup._
    Thread.sleep(200)
    info("And as waiting for 200 millis")
    for {
      failure <- protectedHandle.completed.failed
    } yield {
      info("Initialisation should completed with failure")
      failure.getMessage shouldBe "Cannot acquire lock TestLockId(20) in lock-mode Exclusive"
    }
  }

  it should "fail if execution initialization fails" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      _ <- connectionInitializerFuture
      _ = {
        info("As execution initialization starts")
        failDuringExecutionInitialization(new Exception("failed as initializing"))
        info("Initialization fails")
      }
      failure <- protectedHandle.completed.failed
    } yield {
      info("Protected execution fails with failure")
      failure.getMessage shouldBe "failed as initializing"
    }
  }

  behavior of "databaseLockBasedHaCoordinator main connection polling"

  it should "successfully prevent further worker connection-spawning, and trigger shutdown in execution, if main lock cannot be acquired anymore, triggered by connection-spawning" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        completeExecutionInitialization()
        info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        Thread.sleep(200)
        info("As waiting 200 millis")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is still not completed")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer still works")
        executionAbortedFuture.isCompleted shouldBe false
        info("Execution is not aborted yet")
        dbLock.cutExclusiveLockHoldingConnection(mainLockId)
        info("As main connection is severed")
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
        info("Connection initializer not working anymore")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is still not completed")
      }
      abortException <- executionAbortedFuture
      _ = {
        info("Execution is aborted")
        abortException.getMessage shouldBe "check failed, killSwitch aborted"
        executionCompletedPromise.failure(
          new Exception("execution failed due to abort coming from outside")
        )
        info("As execution fails")
      }
      failure <- protectedHandle.completed.failed
    } yield {
      info("Protected Handle is completed with failure")
      failure.getMessage shouldBe "check failed, killSwitch aborted"
      info("And completion failure is populated by check-failure")
      1 shouldBe 1
    }
  }

  it should "successfully prevent further worker connection-spawning, and trigger shutdown in execution, if main lock cannot be acquired anymore, triggered by timeout" in {
    val protectedSetup = setup()
    import protectedSetup._

    for {
      connectionInitializer <- connectionInitializerFuture
      mainConnectionSeveredAtNanos = {
        info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is not completed")
        completeExecutionInitialization()
        info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer works")
        Thread.sleep(200)
        info("As waiting 200 millis")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is still not completed")
        connectionInitializer.initialize(new TestConnection)
        info("Connection initializer still works")
        protectedSetup.executionAbortedFuture.isCompleted shouldBe false
        info("Execution is not aborted yet")
        dbLock.cutExclusiveLockHoldingConnection(mainLockId)
        info("As main connection is severed")
        System.nanoTime()
      }
      abortException <- executionAbortedFuture
      _ = {
        info("Execution is aborted")
        abortException.getMessage shouldBe "check failed, killSwitch aborted"
        (System.nanoTime() - mainConnectionSeveredAtNanos) should be < ((
          mainLockAcquireRetryMillis +
            timeoutToleranceMillis
        ) * 1000L * 1000L)
        info("Within polling time-bounds")
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
        info("Connection initializer not working anymore")
        protectedHandle.completed.isCompleted shouldBe false
        info("Protected Handle is still not completed")
        executionCompletedPromise.failure(
          new Exception("execution failed due to abort coming from outside")
        )
        info("As execution fails")
      }
      failure <- protectedHandle.completed.failed
    } yield {
      info("Protected Handle is completed with failure")
      failure.getMessage shouldBe "check failed, killSwitch aborted"
      info("And completion failure is populated by check-failure")
      1 shouldBe 1
    }
  }

  behavior of "databaseLockBasedHaCoordinator in multi-node setup"

  it should "successfully protect execution, and respect upper bound threshold as switching over in a 5 node setup, without worker locking" in {
    val dbLock = new TestDBLockStorageBackend

    var nodes: Set[ProtectedSetup] = Set.empty
    val nodeStartedExecutionProbe = TestProbe()
    val nodeStoppedExecutionProbe = TestProbe()
    val nodeHaltedProbe = TestProbe()

    def addNode(): Unit = synchronized {
      val node = setup(dbLock = dbLock)
      node.connectionInitializerFuture
        .foreach { _ =>
          logger.info(s"execution started")
          node.completeExecutionInitialization()
          nodeStartedExecutionProbe.send(nodeStartedExecutionProbe.ref, "started")
        }
      node.executionShutdownFuture
        .foreach { _ =>
          node.executionCompletedPromise.success(())
          nodeStoppedExecutionProbe.send(nodeStoppedExecutionProbe.ref, "stopped")
        }
      node.executionAbortedFuture
        .foreach { t =>
          node.executionCompletedPromise.failure(t)
          nodeStoppedExecutionProbe.send(nodeStoppedExecutionProbe.ref, "stopped")
        }
      node.protectedHandle.completed
        .onComplete { _ =>
          removeNode(node)
          nodeHaltedProbe.send(nodeHaltedProbe.ref, "halted")
        }
      nodes = nodes + node
      info("As a node added")
    }

    def removeNode(node: ProtectedSetup): Unit = synchronized {
      nodes = nodes - node
    }

    def verifyCleanSlate(expectedNumberOfNodes: Int): Unit = synchronized {
      if (expectedNumberOfNodes == 0) {
        nodes.size shouldBe 0
      } else {
        nodes.size shouldBe expectedNumberOfNodes
        nodes.exists(_.protectedHandle.completed.isCompleted) shouldBe false
        nodes.count(_.connectionInitializerFuture.isCompleted) shouldBe 1
        val activeNode = nodes.find(_.connectionInitializerFuture.isCompleted).get
        activeNode.executionAbortedFuture.isCompleted shouldBe false
        activeNode.executionShutdownFuture.isCompleted shouldBe false
      }
      nodeStartedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeStoppedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeHaltedProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      info("Cluster is in expected shape")
    }

    def wait(): Unit = {
      val waitMillis: Long = Random.nextInt(100).toLong
      Thread.sleep(waitMillis)
      info(s"As waiting $waitMillis millis")
    }

    addNode()
    nodeStartedExecutionProbe.expectMsg("started")
    info("The first node started execution")
    verifyCleanSlate(1)
    addNode()
    verifyCleanSlate(2)
    addNode()
    verifyCleanSlate(3)
    addNode()
    verifyCleanSlate(4)
    addNode()
    verifyCleanSlate(5)
    info("As adding 5 nodes")
    wait()
    verifyCleanSlate(5)
    info("Cluster stabilized")

    for (_ <- 1 to 30) {
      wait()
      verifyCleanSlate(5)
      dbLock.cutExclusiveLockHoldingConnection(mainLockId)
      logger.info(s"main lock force-released")
      val mainConnCutNanos = System.nanoTime()
      info(
        "As active node looses main connection (and main index lock is available for acquisition again)"
      )
      nodeStartedExecutionProbe.expectMsg("started")
      (System.nanoTime() - mainConnCutNanos) should be < ((
        mainLockAcquireRetryMillis +
          timeoutToleranceMillis
      ) * 1000L * 1000L)
      info("Some other node started execution within time bounds")
      nodeStoppedExecutionProbe.expectMsg("stopped")
      nodeHaltedProbe.expectMsg("halted")
      info("And originally active node stopped")
      verifyCleanSlate(4)
      addNode()
      verifyCleanSlate(5)
    }

    def tearDown(): Unit = {
      dbLock.cutExclusiveLockHoldingConnection(mainLockId)
      nodeStartedExecutionProbe.expectMsg("started")
      nodeStoppedExecutionProbe.expectMsg("stopped")
      nodeHaltedProbe.expectMsg("halted")
      ()
    }

    tearDown()
    verifyCleanSlate(4)
    tearDown()
    verifyCleanSlate(3)
    tearDown()
    verifyCleanSlate(2)
    tearDown()
    verifyCleanSlate(1)
    dbLock.cutExclusiveLockHoldingConnection(mainLockId)
    nodeStoppedExecutionProbe.expectMsg("stopped")
    nodeHaltedProbe.expectMsg("halted")
    verifyCleanSlate(0)

    Future.successful(1 shouldBe 1)
  }

  it should "successfully protect execution, and respect upper bound threshold as switching over in a 5 node setup, with worker connection locking: loosing only indexer lock" in {
    val dbLock = new TestDBLockStorageBackend

    val keepUsingWorkerAfterShutdownMillis = 100L

    var nodes: Set[ProtectedSetup] = Set.empty
    val nodeStartedExecutionProbe = TestProbe()
    val nodeStoppedExecutionProbe = TestProbe()
    val nodeHaltedProbe = TestProbe()
    val concurrentWorkers = new AtomicInteger(0)

    def addNode(): Unit = synchronized {
      val node = setup(dbLock = dbLock)
      val workerConnection = new TestConnection
      node.connectionInitializerFuture
        .foreach { connectionInitializer =>
          logger.info(s"execution started")
          node.completeExecutionInitialization()
          connectionInitializer.initialize(workerConnection)
          concurrentWorkers.incrementAndGet()
          nodeStartedExecutionProbe.send(nodeStartedExecutionProbe.ref, "started")
        }
      node.executionShutdownFuture
        .foreach { _ =>
          Thread.sleep(keepUsingWorkerAfterShutdownMillis)
          concurrentWorkers.decrementAndGet()
          dbLock.release(DBLockStorageBackend.Lock(worker, DBLockStorageBackend.LockMode.Shared))(
            workerConnection
          )
          node.executionCompletedPromise.success(())
          nodeStoppedExecutionProbe.send(nodeStoppedExecutionProbe.ref, "stopped")
        }
      node.executionAbortedFuture
        .foreach { t =>
          Thread.sleep(keepUsingWorkerAfterShutdownMillis)
          concurrentWorkers.decrementAndGet()
          dbLock.release(DBLockStorageBackend.Lock(worker, DBLockStorageBackend.LockMode.Shared))(
            workerConnection
          )
          node.executionCompletedPromise.failure(t)
          nodeStoppedExecutionProbe.send(nodeStoppedExecutionProbe.ref, "stopped")
        }
      node.protectedHandle.completed
        .onComplete { _ =>
          removeNode(node)
          nodeHaltedProbe.send(nodeHaltedProbe.ref, "halted")
        }
      nodes = nodes + node
      info("As a node added")
    }

    def removeNode(node: ProtectedSetup): Unit = synchronized {
      nodes = nodes - node
    }

    def verifyCleanSlate(expectedNumberOfNodes: Int): Unit = synchronized {
      if (expectedNumberOfNodes == 0) {
        nodes.size shouldBe 0
      } else {
        concurrentWorkers.get() shouldBe 1
        nodes.size shouldBe expectedNumberOfNodes
        nodes.exists(_.protectedHandle.completed.isCompleted) shouldBe false
        nodes.count(_.connectionInitializerFuture.isCompleted) shouldBe 1
        val activeNode = nodes.find(_.connectionInitializerFuture.isCompleted).get
        activeNode.executionAbortedFuture.isCompleted shouldBe false
        activeNode.executionShutdownFuture.isCompleted shouldBe false
      }
      nodeStartedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeStoppedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeHaltedProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      info("Cluster is in expected shape")
    }

    def wait(): Unit = {
      val waitMillis: Long = Random.nextInt(100).toLong
      Thread.sleep(waitMillis)
      info(s"As waiting $waitMillis millis")
    }

    addNode()
    nodeStartedExecutionProbe.expectMsg("started")
    info("The first node started execution")
    verifyCleanSlate(1)
    addNode()
    verifyCleanSlate(2)
    addNode()
    verifyCleanSlate(3)
    addNode()
    verifyCleanSlate(4)
    addNode()
    verifyCleanSlate(5)
    info("As adding 5 nodes")
    wait()
    verifyCleanSlate(5)
    info("Cluster stabilized")

    for (_ <- 1 to 30) {
      wait()
      verifyCleanSlate(5)
      dbLock.cutExclusiveLockHoldingConnection(mainLockId)
      logger.info(s"main lock force-released")
      val mainConnCutNanos = System.nanoTime()
      info("As active node looses main connection (and with time drops worker connection as well)")
      nodeStartedExecutionProbe.expectMsg("started")
      (System.nanoTime() - mainConnCutNanos) should be < ((
        mainLockCheckerPeriodMillis + // first active node has to realize that it lost the lock
          keepUsingWorkerAfterShutdownMillis + // then it is shutting down, but it will take this long to release the worker lock as well
          workerLockAcquireRetryMillis + // by the time of here the new active node already acquired the main lock, and it is polling for the worker lock, so maximum so much time we need to wait
          timeoutToleranceMillis
      ) * 1000L * 1000L)
      info("Some other node started execution within time bounds")
      nodeStoppedExecutionProbe.expectMsg("stopped")
      nodeHaltedProbe.expectMsg("halted")
      info("And originally active node stopped")
      verifyCleanSlate(4)
      addNode()
      verifyCleanSlate(5)
    }

    def tearDown(): Unit = {
      dbLock.cutExclusiveLockHoldingConnection(mainLockId)
      nodeStartedExecutionProbe.expectMsg("started")
      nodeStoppedExecutionProbe.expectMsg("stopped")
      nodeHaltedProbe.expectMsg("halted")
      ()
    }

    tearDown()
    verifyCleanSlate(4)
    tearDown()
    verifyCleanSlate(3)
    tearDown()
    verifyCleanSlate(2)
    tearDown()
    verifyCleanSlate(1)
    dbLock.cutExclusiveLockHoldingConnection(mainLockId)
    nodeStoppedExecutionProbe.expectMsg("stopped")
    nodeHaltedProbe.expectMsg("halted")
    verifyCleanSlate(0)

    Future.successful(1 shouldBe 1)
  }

  private def setup(
      connectionFactory: () => Connection = () => new TestConnection,
      workerLockAcquireMaxRetry: Long = 100,
      dbLock: TestDBLockStorageBackend = new TestDBLockStorageBackend,
  ): ProtectedSetup = {
    val connectionInitializerPromise = Promise[ConnectionInitializer]()
    val executionHandlePromise = Promise[Unit]()

    val shutdownPromise = Promise[Unit]()
    val abortPromise = Promise[Throwable]()
    val completedPromise = Promise[Unit]()

    val protectedHandle = HaCoordinator
      .databaseLockBasedHaCoordinator(
        mainConnectionFactory = connectionFactory,
        storageBackend = dbLock,
        executionContext = system.dispatcher,
        timer = timer,
        haConfig = HaConfig(
          mainLockAcquireRetryMillis = mainLockAcquireRetryMillis,
          workerLockAcquireRetryMillis = workerLockAcquireRetryMillis,
          workerLockAcquireMaxRetry = workerLockAcquireMaxRetry,
          mainLockCheckerPeriodMillis = mainLockCheckerPeriodMillis,
          indexerLockId = 10,
          indexerWorkerLockId = 20,
        ),
      )
      .protectedExecution { connectionInitializer =>
        connectionInitializerPromise.success(connectionInitializer)
        executionHandlePromise.future.map(_ =>
          Handle(
            killSwitch = new KillSwitch {
              override def shutdown(): Unit = shutdownPromise.success(())
              override def abort(ex: Throwable): Unit = abortPromise.success(ex)
            },
            completed = completedPromise.future,
          )
        )
      }

    ProtectedSetup(
      protectedHandle = protectedHandle,
      connectionInitializerFuture = connectionInitializerPromise.future,
      executionHandlePromise = executionHandlePromise,
      executionShutdownFuture = shutdownPromise.future,
      executionAbortedFuture = abortPromise.future,
      executionCompletedPromise = completedPromise,
      dbLock = dbLock,
    )
  }

  case class ProtectedSetup(
      protectedHandle: Handle, // the protected Handle to observe and interact with
      connectionInitializerFuture: Future[
        ConnectionInitializer
      ], // observe ConnectionInitializer, this completes as HA arrives to the stage when execution initialization starts
      executionHandlePromise: Promise[Unit], // trigger end of execution initialization
      executionShutdownFuture: Future[Unit], // observe shutdown in execution
      executionAbortedFuture: Future[Throwable], // observe abort in execution
      executionCompletedPromise: Promise[Unit], // trigger completion of execution
      dbLock: TestDBLockStorageBackend, // the lock backend
  ) {

    /** trigger end of execution initialization */
    def completeExecutionInitialization(): Unit = {
      executionHandlePromise.success(())
    }

    /** simulate a failure during execution initialization */
    def failDuringExecutionInitialization(cause: Throwable): Unit = {
      executionHandlePromise.failure(cause)
    }
  }
}
