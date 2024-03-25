// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  ExecutionContextMonitor,
  ExecutorServiceExtensions,
  Threading,
}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference

/** Mixin that provides an executor for tests.
  * The executor supports blocking operations, provided they are wrapped in [[scala.concurrent.blocking]]
  * or [[scala.concurrent.Await]].
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
trait HasExecutorService extends BeforeAndAfterAll with HasExecutorServiceGeneric { this: Suite =>
  def handleFailure(message: String): Nothing = fail(message)

  override def afterAll(): Unit =
    try super.afterAll()
    finally {
      closeExecutor()
    }
}

trait HasExecutorServiceGeneric extends NamedLogging {

  private case class ExecutorState(
      scheduler: ScheduledExecutorService,
      executor: ExecutionContextIdlenessExecutorService,
      monitor: ExecutionContextMonitor,
  ) extends AutoCloseable {
    override def close(): Unit = {
      monitor.close()
      ExecutorServiceExtensions(scheduler)(logger, DefaultProcessingTimeouts.testing).close()
      ExecutorServiceExtensions(executor)(logger, DefaultProcessingTimeouts.testing).close()
    }
  }

  private def createScheduler(): ScheduledExecutorService = {
    Threading.singleThreadScheduledExecutor(
      loggerFactory.threadName + "-test-execution-context-monitor",
      noTracingLogger,
    )
  }

  private def createExecutor(
      scheduler: ScheduledExecutorService
  ): (ExecutionContextIdlenessExecutorService, ExecutionContextMonitor) = {
    val threads = Threading.detectNumberOfThreads(noTracingLogger)
    val service = Threading.newExecutionContext(
      executionContextName,
      noTracingLogger,
      threads,
      exitOnFatal = exitOnFatal,
    )
    val monitor =
      new ExecutionContextMonitor(
        loggerFactory,
        NonNegativeFiniteDuration.tryOfSeconds(3),
        NonNegativeFiniteDuration.tryOfSeconds(10),
        DefaultProcessingTimeouts.testing,
      )(scheduler)
    monitor.monitor(service)
    (service, monitor)
  }

  private def createExecutorState(): ExecutorState = {
    // Monitor the execution context to get useful information on deadlocks.
    val scheduler = createScheduler()
    val (executor, monitor) = createExecutor(scheduler)
    ExecutorState(scheduler, executor, monitor)
  }

  def handleFailure(message: String): Nothing

  private def getOrCreateExecutorState(): Option[ExecutorState] =
    executorStateRef
      .updateAndGet(_.orElse(Some(createExecutorState())))

  private def getOrCreateExecutor(): ExecutionContextIdlenessExecutorService =
    getOrCreateExecutorState()
      .map(_.executor)
      .getOrElse(handleFailure("Executor was not created"))

  private def getOrCreateScheduler(): ScheduledExecutorService =
    getOrCreateExecutorState()
      .map(_.scheduler)
      .getOrElse(handleFailure("Scheduler was not created"))

  private lazy val executorStateRef: AtomicReference[Option[ExecutorState]] =
    new AtomicReference[Option[ExecutorState]](None)

  protected def executionContextName: String = loggerFactory.threadName + "-test-execution-context"

  protected def exitOnFatal: Boolean = true

  protected def executorService: ExecutionContextIdlenessExecutorService = getOrCreateExecutor()

  protected def scheduledExecutor(): ScheduledExecutorService = getOrCreateScheduler()

  protected def closeExecutor(): Unit = {
    val executorStateClose: AutoCloseable = () => {
      executorStateRef.updateAndGet { executorStateO =>
        executorStateO.foreach(_.close())
        None
      }
    }
    Lifecycle.close(executorStateClose)(logger)
  }
}
