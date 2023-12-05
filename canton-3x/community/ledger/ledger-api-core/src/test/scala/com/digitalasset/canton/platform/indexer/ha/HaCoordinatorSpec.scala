// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.logging.{SuppressingLogger, TracedLogger}
import com.digitalasset.canton.platform.store.backend.DBLockStorageBackend
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.testkit.TestProbe
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import java.util.Timer
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Random, Try}

class HaCoordinatorSpec
    extends AsyncFlatSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with Eventually {
  implicit val ec: ExecutionContext =
    system.dispatcher // we need this to not use the default EC which is coming from AsyncTestSuite, and which is serial
  private val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)
  private implicit val traceContext: TraceContext = TraceContext.empty
  private val logger = TracedLogger(loggerFactory.getLogger(getClass))
  private val timer = new Timer(true)

  private val mainLockAcquireRetryTimeout = NonNegativeFiniteDuration.ofMillis(20)
  private val workerLockAcquireRetryTimeout = NonNegativeFiniteDuration.ofMillis(20)
  private val mainLockCheckerPeriod = NonNegativeFiniteDuration.ofMillis(20)
  private val timeoutTolerance = NonNegativeFiniteDuration.ofSeconds(
    600
  ) // unfortunately this needs to be a insanely big tolerance, not to render the test flaky. under normal circumstances this should pass with +5 millis

  private val mainLockId = 10
  private val main = TestLockId(mainLockId)

  private val workerLockId = 20
  private val worker = TestLockId(workerLockId)

  implicit class LockPicker(dbLock: Option[DBLockStorageBackend.Lock]) {
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    def pick: DBLockStorageBackend.Lock = dbLock.get
  }

  behavior of "databaseLockBasedHaCoordinator graceful shutdown"

  it should "successfully propagate successful end of execution" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      _ <- connectionInitializerFuture
      _ = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        completeExecutionInitialization()
        logger.info("As protected execution initialization is finished")
        executionCompletedPromise.success(())
        logger.info("As execution is completed")
      }
      _ <- protectedHandle.completed
    } yield {
      logger.info("Protected Handle is completed successfully")
      1 shouldBe 1
    }
  }

  it should "successfully propagate failed end of execution" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      _ <- connectionInitializerFuture
      _ = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        completeExecutionInitialization()
        logger.info("As protected execution initialization is finished")
        executionCompletedPromise.failure(new Exception("failed execution"))
        logger.info("As execution completes with failure")
      }
      ex <- protectedHandle.completed.failed
    } yield {
      logger.info("Protected Handle is completed with failure")
      ex.getMessage shouldBe "failed execution"
    }
  }

  it should "propagate graceful shutdown to the protected execution" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        completeExecutionInitialization()
        logger.info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        protectedHandle.killSwitch.shutdown()
        logger.info("And as graceful shutdown started")
      }
      _ <- executionShutdownFuture
      _ = {
        logger.info("Shutdown is observed at execution")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        executionAbortedFuture.isCompleted shouldBe false
        logger.info("Abort is not observed at execution")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle not completed")
        Threading.sleep(200)
        logger.info("As waiting 200 millis")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info(
          "Protected Handle is still not completed (hence it is waiting for execution to finish as the first step of the teardown process)"
        )
        executionCompletedPromise.success(())
        logger.info("As execution is completed")
      }
      _ <- protectedHandle.completed
    } yield {
      logger.info("Protected Handle is completed successfully")
      loggerFactory.assertLogs(
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true,
        _.errorMessage should include(
          "Internal Error: This check should not be called from outside by the time the PollingChecker is closed."
        ),
      )
      logger.info("Connection initializer does not work anymore")
      1 shouldBe 1
    }
  }

  it should "propagate graceful shutdown to the protected execution if shutdown initiated before execution initialization is finished" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        protectedHandle.killSwitch.shutdown()
        logger.info("As graceful shutdown started")
        completeExecutionInitialization()
        logger.info("And As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        logger.info(
          "Connection initializer still works (release process first releases the execution, and only then the main connection and the poller)"
        )
      }
      _ <- executionShutdownFuture
      _ = {
        logger.info("Shutdown is observed at execution")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        executionAbortedFuture.isCompleted shouldBe false
        logger.info("Abort is not observed at execution")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle not completed")
        executionCompletedPromise.success(())
        logger.info("As execution is completed")
      }
      _ <- protectedHandle.completed
    } yield {
      logger.info("Protected Handle is completed successfully")
      loggerFactory.assertLogs(
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true,
        _.errorMessage should include(
          "Internal Error: This check should not be called from outside by the time the PollingChecker is closed."
        ),
      )
      logger.info("Connection initializer does not work anymore")
      1 shouldBe 1
    }
  }

  it should "swallow failures if graceful shutdown is underway" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        completeExecutionInitialization()
        logger.info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        protectedHandle.killSwitch.shutdown()
        logger.info("And as graceful shutdown started")
      }
      _ <- executionShutdownFuture
      _ = {
        logger.info("Shutdown is observed at execution")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        executionAbortedFuture.isCompleted shouldBe false
        logger.info("Abort is not observed at execution")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle not completed")
        executionCompletedPromise.failure(new Exception("some exception"))
        logger.info("As execution is completes with failure")
      }
      _ <- protectedHandle.completed
    } yield {
      logger.info("Protected Handle is completed successfully")
      loggerFactory.assertLogs(
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true,
        _.errorMessage should include(
          "Internal Error: This check should not be called from outside by the time the PollingChecker is closed."
        ),
      )
      logger.info("Connection initializer does not work anymore")
      1 shouldBe 1
    }
  }

  behavior of "databaseLockBasedHaCoordinator initialization"

  it should "fail if getting connection fails" in {
    val protectedSetup =
      setup(connectionFactory = () => throw new Exception("as getting connection"))
    import protectedSetup.*

    for {
      result <- protectedHandle.completed.failed
    } yield {
      logger.info("Protected Handle is completed with failure")
      result.getMessage shouldBe "as getting connection"
    }
  }

  it should "wait if main lock cannot be acquired" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    val blockingLock =
      dbLock.tryAcquire(main, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).pick
    logger.info("As acquiring the main lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup.*
    logger.info("As acquiring the main lock from the outside")
    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")
    dbLock.release(blockingLock)(blockingConnection) shouldBe true
    logger.info("As releasing the blocking lock")

    for {
      _ <- connectionInitializerFuture
      _ = {
        logger.info("Initialisation should completed successfully")
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
    dbLock.tryAcquire(main, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).pick
    logger.info("As acquiring the main lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup.*
    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")
    protectedHandle.killSwitch.shutdown()
    logger.info("As graceful shutdown started")

    for {
      _ <- protectedHandle.completed
    } yield {
      logger.info("Protected Handle is completed successfully")
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "wait for main lock can be interrupted by exception thrown during tryAcquire" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(main, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).pick
    logger.info("As acquiring the main lock from the outside")
    val mainConnection = new TestConnection
    val protectedSetup = setup(
      dbLock = dbLock,
      connectionFactory = () => mainConnection,
    )
    import protectedSetup.*
    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")

    for {
      failure <- loggerFactory.assertLogs(
        {
          mainConnection.close()
          logger.info(
            "As main connection is closed (triggers exception as used for acquiring lock)"
          )
          protectedHandle.completed.failed
        },
        _.warningMessage should include("Failure not retryable"),
      )
    } yield {
      logger.info("Protected Handle is completed with a failure")
      failure.getMessage shouldBe "trying to acquire on a closed connection"
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "wait if worker lock cannot be acquired due to exclusive blocking" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    val blockingLock =
      dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Exclusive)(blockingConnection).pick
    logger.info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup.*
    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")
    dbLock.release(blockingLock)(blockingConnection) shouldBe true
    logger.info("As releasing the blocking lock")

    for {
      _ <- connectionInitializerFuture
      _ = {
        logger.info("Initialisation should completed successfully")
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
      dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).pick
    logger.info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup.*

    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")
    dbLock.release(blockingLock)(blockingConnection) shouldBe true
    logger.info("As releasing the blocking lock")

    for {
      _ <- connectionInitializerFuture
      _ = {
        logger.info("Initialisation should completed successfully")
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
    dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).pick
    logger.info("As acquiring the worker lock from the outside")
    val protectedSetup = setup(dbLock = dbLock)
    import protectedSetup.*

    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")
    protectedHandle.killSwitch.shutdown()
    logger.info("As graceful shutdown starts")

    for {
      _ <- protectedHandle.completed
    } yield {
      logger.info("Protected Handle completes successfully")
      connectionInitializerFuture.isCompleted shouldBe false
    }
  }

  it should "wait for worker lock can be interrupted by exception thrown during tryAcquire" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).pick
    logger.info("As acquiring the worker lock from the outside")
    val mainConnection = new TestConnection
    val protectedSetup = setup(
      dbLock = dbLock,
      connectionFactory = () => mainConnection,
    )
    import protectedSetup.*

    Threading.sleep(200)
    logger.info("And as waiting for 200 millis")
    connectionInitializerFuture.isCompleted shouldBe false
    protectedHandle.completed.isCompleted shouldBe false
    logger.info("Initialization should be waiting")

    loggerFactory.assertLogs(
      {
        mainConnection.close()
        logger.info("As main connection is closed (triggers exception as used for acquiring lock)")
        for {
          failure <- protectedHandle.completed.failed
        } yield {
          logger.info("Protected Handle completed with a failure")
          failure.getMessage shouldBe "trying to acquire on a closed connection"
          connectionInitializerFuture.isCompleted shouldBe false
        }
      },
      _.warningMessage should include("Failure not retryable."),
    )
  }

  it should "fail if worker lock cannot be acquired in time due to shared blocking" in {
    val dbLock = new TestDBLockStorageBackend
    val blockingConnection = new TestConnection
    dbLock.tryAcquire(worker, DBLockStorageBackend.LockMode.Shared)(blockingConnection).pick
    logger.info("As acquiring the worker lock from the outside")
    loggerFactory.assertLogs(
      within = {
        val protectedSetup = setup(
          dbLock = dbLock,
          workerLockAcquireMaxRetries = NonNegativeLong.tryCreate(2),
        )
        import protectedSetup.*
        Threading.sleep(200)
        logger.info("And as waiting for 200 millis")
        for {
          failure <- protectedHandle.completed.failed
        } yield {
          logger.info("Initialisation should completed with failure")
          failure.getMessage shouldBe "Cannot acquire lock TestLockId(20) in lock-mode Exclusive"
        }
      },
      _.warningMessage should include("Maximum amount of retries reached (0). Failing permanently."),
    )
  }

  it should "fail if execution initialization fails" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      _ <- connectionInitializerFuture
      _ = {
        logger.info("As execution initialization starts")
        failDuringExecutionInitialization(new Exception("failed as initializing"))
        logger.info("Initialization fails")
      }
      failure <- protectedHandle.completed.failed
    } yield {
      logger.info("Protected execution fails with failure")
      failure.getMessage shouldBe "failed as initializing"
    }
  }

  behavior of "databaseLockBasedHaCoordinator main connection polling"

  it should "successfully prevent further worker connection-spawning, and trigger shutdown in execution, if main lock cannot be acquired anymore, triggered by connection-spawning" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      connectionInitializer <- connectionInitializerFuture
      _ = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        completeExecutionInitialization()
        logger.info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        Threading.sleep(200)
        logger.info("As waiting 200 millis")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is still not completed")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer still works")
        executionAbortedFuture.isCompleted shouldBe false
        logger.info("Execution is not aborted yet")
        dbLock.cutExclusiveLockHoldingConnection(mainLockId)
        logger.info("As main connection is severed")
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
        logger.info("Connection initializer not working anymore")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is still not completed")
      }
      abortException <- executionAbortedFuture
      _ = {
        logger.info("Execution is aborted")
        abortException.getMessage shouldBe "check failed, killSwitch aborted"
        executionCompletedPromise.failure(
          new Exception("execution failed due to abort coming from outside")
        )
        logger.info("As execution fails")
      }
      failure <- protectedHandle.completed.failed
    } yield {
      logger.info("Protected Handle is completed with failure")
      failure.getMessage shouldBe "check failed, killSwitch aborted"
      logger.info("And completion failure is populated by check-failure")
      1 shouldBe 1
    }
  }

  it should "successfully prevent further worker connection-spawning, and trigger shutdown in execution, if main lock cannot be acquired anymore, triggered by timeout" in {
    val protectedSetup = setup()
    import protectedSetup.*

    for {
      connectionInitializer <- connectionInitializerFuture
      mainConnectionSeveredAtNanos = {
        logger.info("As HACoordinator is initialized")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is not completed")
        completeExecutionInitialization()
        logger.info("As protected execution initialization is finished")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer works")
        Threading.sleep(200)
        logger.info("As waiting 200 millis")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is still not completed")
        connectionInitializer.initialize(new TestConnection)
        logger.info("Connection initializer still works")
        protectedSetup.executionAbortedFuture.isCompleted shouldBe false
        logger.info("Execution is not aborted yet")
        dbLock.cutExclusiveLockHoldingConnection(mainLockId)
        logger.info("As main connection is severed")
        System.nanoTime()
      }
      abortException <- executionAbortedFuture
      _ = {
        logger.info("Execution is aborted")
        abortException.getMessage shouldBe "check failed, killSwitch aborted"
        (System.nanoTime() - mainConnectionSeveredAtNanos) should be < ((
          mainLockAcquireRetryTimeout +
            timeoutTolerance
        ).duration.toNanos)
        logger.info("Within polling time-bounds")
        Try(connectionInitializer.initialize(new TestConnection)).isFailure shouldBe true
        logger.info("Connection initializer not working anymore")
        protectedHandle.completed.isCompleted shouldBe false
        logger.info("Protected Handle is still not completed")
        executionCompletedPromise.failure(
          new Exception("execution failed due to abort coming from outside")
        )
        logger.info("As execution fails")
      }
      failure <- protectedHandle.completed.failed
    } yield {
      logger.info("Protected Handle is completed with failure")
      failure.getMessage shouldBe "check failed, killSwitch aborted"
      logger.info("And completion failure is populated by check-failure")
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

    def addNode(): Unit = blocking(synchronized {
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
      logger.info("As a node added")
    })

    def removeNode(node: ProtectedSetup): Unit = blocking(synchronized {
      nodes = nodes - node
    })

    def verifyCleanSlate(expectedNumberOfNodes: Int): Unit = blocking(synchronized {
      if (expectedNumberOfNodes == 0) {
        nodes.size shouldBe 0
      } else {
        nodes.size shouldBe expectedNumberOfNodes
        nodes.exists(_.protectedHandle.completed.isCompleted) shouldBe false
        nodes.count(_.connectionInitializerFuture.isCompleted) shouldBe 1
        nodes.find(_.connectionInitializerFuture.isCompleted).foreach { activeNode =>
          activeNode.executionAbortedFuture.isCompleted shouldBe false
          activeNode.executionShutdownFuture.isCompleted shouldBe false
        }
      }
      nodeStartedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeStoppedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeHaltedProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      logger.info("Cluster is in expected shape")
    })

    def wait(): Unit = {
      val waitMillis: Long = Random.nextInt(100).toLong
      Threading.sleep(waitMillis)
      logger.info(s"As waiting $waitMillis millis")
    }

    addNode()
    nodeStartedExecutionProbe.expectMsg("started")
    logger.info("The first node started execution")
    verifyCleanSlate(1)
    addNode()
    verifyCleanSlate(2)
    addNode()
    verifyCleanSlate(3)
    addNode()
    verifyCleanSlate(4)
    addNode()
    verifyCleanSlate(5)
    logger.info("As adding 5 nodes")
    wait()
    verifyCleanSlate(5)
    logger.info("Cluster stabilized")

    for (_ <- 1 to 30) {
      wait()
      verifyCleanSlate(5)
      dbLock.cutExclusiveLockHoldingConnection(mainLockId)
      logger.info(s"main lock force-released")
      val mainConnCutNanos = System.nanoTime()
      logger.info(
        "As active node looses main connection (and main index lock is available for acquisition again)"
      )
      nodeStartedExecutionProbe.expectMsg("started")
      (System.nanoTime() - mainConnCutNanos) should be < ((
        mainLockAcquireRetryTimeout +
          timeoutTolerance
      ).duration.toNanos)
      logger.info("Some other node started execution within time bounds")
      nodeStoppedExecutionProbe.expectMsg("stopped")
      nodeHaltedProbe.expectMsg("halted")
      logger.info("And originally active node stopped")
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

    def addNode(): Unit = blocking(synchronized {
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
          Threading.sleep(keepUsingWorkerAfterShutdownMillis)
          concurrentWorkers.decrementAndGet()
          dbLock.release(DBLockStorageBackend.Lock(worker, DBLockStorageBackend.LockMode.Shared))(
            workerConnection
          )
          node.executionCompletedPromise.success(())
          nodeStoppedExecutionProbe.send(nodeStoppedExecutionProbe.ref, "stopped")
        }
      node.executionAbortedFuture
        .foreach { t =>
          Threading.sleep(keepUsingWorkerAfterShutdownMillis)
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
      logger.info("As a node added")
    })

    def removeNode(node: ProtectedSetup): Unit = blocking(synchronized {
      nodes = nodes - node
    })

    def verifyCleanSlate(expectedNumberOfNodes: Int): Unit = blocking(synchronized {
      if (expectedNumberOfNodes == 0) {
        nodes.size shouldBe 0
      } else {
        concurrentWorkers.get() shouldBe 1
        nodes.size shouldBe expectedNumberOfNodes
        nodes.exists(_.protectedHandle.completed.isCompleted) shouldBe false
        nodes.count(_.connectionInitializerFuture.isCompleted) shouldBe 1
        nodes.find(_.connectionInitializerFuture.isCompleted).foreach { activeNode =>
          activeNode.executionAbortedFuture.isCompleted shouldBe false
          activeNode.executionShutdownFuture.isCompleted shouldBe false
        }
      }
      nodeStartedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeStoppedExecutionProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      nodeHaltedProbe.expectNoMessage(FiniteDuration(0, "seconds"))
      logger.info("Cluster is in expected shape")
    })

    def wait(): Unit = {
      val waitMillis: Long = Random.nextInt(100).toLong
      Threading.sleep(waitMillis)
      logger.info(s"As waiting $waitMillis millis")
    }

    addNode()
    nodeStartedExecutionProbe.expectMsg("started")
    logger.info("The first node started execution")
    verifyCleanSlate(1)
    addNode()
    verifyCleanSlate(2)
    addNode()
    verifyCleanSlate(3)
    addNode()
    verifyCleanSlate(4)
    addNode()
    verifyCleanSlate(5)
    logger.info("As adding 5 nodes")
    wait()
    verifyCleanSlate(5)
    logger.info("Cluster stabilized")

    for (_ <- 1 to 30) {
      wait()
      verifyCleanSlate(5)
      dbLock.cutExclusiveLockHoldingConnection(mainLockId)
      logger.info(s"main lock force-released")
      val mainConnCutNanos = System.nanoTime()
      logger.info(
        "As active node looses main connection (and with time drops worker connection as well)"
      )
      nodeStartedExecutionProbe.expectMsg("started")
      (System.nanoTime() - mainConnCutNanos) should be < ((
        mainLockCheckerPeriod + // first active node has to realize that it lost the lock
          NonNegativeFiniteDuration.ofMillis(
            keepUsingWorkerAfterShutdownMillis
          ) + // then it is shutting down, but it will take this long to release the worker lock as well
          workerLockAcquireRetryTimeout + // by the time of here the new active node already acquired the main lock, and it is polling for the worker lock, so maximum so much time we need to wait
          timeoutTolerance
      ).duration.toNanos)
      logger.info("Some other node started execution within time bounds")
      nodeStoppedExecutionProbe.expectMsg("stopped")
      nodeHaltedProbe.expectMsg("halted")
      logger.info("And originally active node stopped")
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
      workerLockAcquireMaxRetries: NonNegativeLong = NonNegativeLong.tryCreate(100),
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
          mainLockAcquireRetryTimeout = mainLockAcquireRetryTimeout,
          workerLockAcquireRetryTimeout = workerLockAcquireRetryTimeout,
          workerLockAcquireMaxRetries = workerLockAcquireMaxRetries,
          mainLockCheckerPeriod = mainLockCheckerPeriod,
          indexerLockId = 10,
          indexerWorkerLockId = 20,
        ),
        loggerFactory = loggerFactory,
      )
      .protectedExecution { connectionInitializer =>
        connectionInitializerPromise.success(connectionInitializer)
        executionHandlePromise.future.map(_ =>
          Handle(
            killSwitch = new KillSwitch {
              override def shutdown(): Unit = shutdownPromise.trySuccess(())
              override def abort(ex: Throwable): Unit = abortPromise.trySuccess(ex)
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
