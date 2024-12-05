// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slick.util

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.typesafe.scalalogging.Logger
import slick.util.AsyncExecutor.{PrioritizedRunnable, WithConnection}

import java.lang.management.ManagementFactory
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{TimeUnit, *}
import javax.management.{InstanceNotFoundException, ObjectName}
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.*
import scala.util.control.NonFatal

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.IsInstanceOf",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Var",
  )
)
class AsyncExecutorWithMetrics(
    name: String,
    minThreads: Int,
    maxThreads: Int,
    queueSize: Int,
    logger: Logger,
    queryCostTracker: QueryCostTracker,
    maxConnections: Int = Integer.MAX_VALUE,
    keepAliveTime: FiniteDuration = 1.minute,
    registerMbeans: Boolean = false,
) extends AsyncExecutorWithShutdown {

  @volatile private[this] lazy val mbeanName = new ObjectName(
    s"slick:type=AsyncExecutor,name=$name"
  );

  // Before init: 0, during init: 1, after init: 2, during/after shutdown: 3
  private[this] val state = new AtomicInteger(0)
  override def isShuttingDown: Boolean = state.get() == 3

  @volatile private[this] var executor: ThreadPoolExecutor = _

  if (maxConnections > maxThreads) {
    // NOTE: when using transactions or DB locks, it may happen that a task has a lock on the database but no thread
    // to complete its action, while other tasks may have all the threads but are waiting for the first task to
    // complete. This creates a deadlock.
    logger.warn(
      "Having maxConnection > maxThreads can result in deadlocks if transactions or database locks are used."
    )
  }

  lazy val executionContext: ExecutionContextExecutor = {
    if (!state.compareAndSet(0, 1))
      throw new IllegalStateException(
        "Cannot initialize ExecutionContext; AsyncExecutor already shut down"
      )
    val queue: BlockingQueue[Runnable] = queueSize match {
      case 0 =>
        // NOTE: SynchronousQueue does not schedule high-priority tasks before others and so it cannot be used when
        // the number of connections is limited (lest high-priority tasks may be holding all connections and low/mid
        // priority tasks all threads -- resulting in a deadlock).
        require(
          maxConnections == Integer.MAX_VALUE,
          "When using queueSize == 0 (direct hand-off), maxConnections must be Integer.MAX_VALUE.",
        )

        new SynchronousQueue[Runnable]
      case -1 =>
        // NOTE: LinkedBlockingQueue does not schedule high-priority tasks before others and so it cannot be used when
        // the number of connections is limited (lest high-priority tasks may be holding all connections and low/mid
        // priority tasks all threads -- resulting in a deadlock).
        require(
          maxConnections == Integer.MAX_VALUE,
          "When using queueSize == -1 (unlimited), maxConnections must be Integer.MAX_VALUE.",
        )

        new LinkedBlockingQueue[Runnable]
      case n =>
        // NOTE: The current implementation of ManagedArrayBlockingQueue is flawed. It makes the assumption that all
        // tasks go through the queue (which is responsible for scheduling high-priority tasks first). However, that
        // assumption is wrong since the ThreadPoolExecutor bypasses the queue when it creates new threads. This
        // happens whenever it creates a new thread to run a task, i.e. when minThreads < maxThreads and the number
        // of existing threads is < maxThreads.
        //
        // The only way to prevent problems is to have minThreads == maxThreads when using the
        // ManagedArrayBlockingQueue.
        require(
          minThreads == maxThreads,
          "When using queueSize > 0, minThreads == maxThreads is required.",
        )

        // NOTE: The current implementation of ManagedArrayBlockingQueue.increaseInUseCount implicitly `require`s that
        // maxThreads <= maxConnections.
        require(
          maxThreads <= maxConnections,
          "When using queueSize > 0, maxThreads <= maxConnections is required.",
        )

        // NOTE: Adding up the above rules
        // - maxThreads >= maxConnections, to prevent database locking issues when using transactions
        // - maxThreads <= maxConnections, required by ManagedArrayBlockingQueue
        // - maxThreads == minThreads, ManagedArrayBlockingQueue
        //
        // We have maxThreads == minThreads == maxConnections as the only working configuration

        (new ManagedArrayBlockingQueue(maxConnections, n)).asInstanceOf[BlockingQueue[Runnable]]
    }

    val tf = new DaemonThreadFactory(name + "-")
    executor = new ThreadPoolExecutor(
      minThreads,
      maxThreads,
      keepAliveTime.toMillis,
      TimeUnit.MILLISECONDS,
      queue,
      tf,
    ) {

      /** If the runnable/task is a low/medium priority item, we increase the items in use count, because first thing it will do
        * is open a Jdbc connection from the pool.
        */
      override def beforeExecute(t: Thread, r: Runnable): Unit = {
        (r, queue) match {
          case (pr: PrioritizedRunnable, q: BlockingQueue[Runnable])
              if pr.priority() != WithConnection =>
            if (q.isInstanceOf[ManagedArrayBlockingQueue]) {
              q.asInstanceOf[ManagedArrayBlockingQueue].attemptPrepare(pr).discard
            }
          case _ =>
        }
        // canton change begin
        queryCostTracker.scheduled(r)
        // canton change end
        super.beforeExecute(t, r)
      }

      // canton change begin
      override def execute(command: Runnable): Unit = {
        queryCostTracker.register(command)
        try {
          super.execute(command)
        } catch {
          // if we throw here, the task will never be executed. therefore, we'll have to remove the task statistics
          // again to not leak memory
          case NonFatal(e) =>
            queryCostTracker.failed(command)
            throw e
        }
      }
      // canton change end

      /** If the runnable/task has released the Jdbc connection we decrease the counter again
        */
      override def afterExecute(r: Runnable, t: Throwable): Unit =
        try {
          super.afterExecute(r, t)
          (r, queue) match {
            case (pr: PrioritizedRunnable, q: BlockingQueue[Runnable]) if pr.connectionReleased =>
              if (q.isInstanceOf[ManagedArrayBlockingQueue]) {
                q.asInstanceOf[ManagedArrayBlockingQueue].attemptCleanUp(pr).discard
              }
            case _ =>
          }
          // canton change begin
        } finally {
          queryCostTracker.completed(r)
          // canton change end
        }
      // canton change begin
      override def shutdownNow(): util.List[Runnable] = {
        queryCostTracker.shutdownNow()
        super.shutdownNow()
      }
      // canton change end
    }
    if (registerMbeans) {
      try {
        val mbeanServer = ManagementFactory.getPlatformMBeanServer
        if (mbeanServer.isRegistered(mbeanName))
          logger.warn(s"MBean $mbeanName already registered (AsyncExecutor names should be unique)")
        else {
          logger.debug(s"Registering MBean $mbeanName")
          mbeanServer.registerMBean(
            new AsyncExecutorMXBean {
              def getMaxQueueSize = queueSize
              def getQueueSize = queue.size()
              def getMaxThreads = maxThreads
              def getActiveThreads = executor.getActiveCount
            },
            mbeanName,
          )
        }
      } catch { case NonFatal(ex) => logger.error("Error registering MBean", ex) }
    }
    if (!state.compareAndSet(1, 2)) {
      unregisterMbeans()
      executor.shutdownNow()
      throw new IllegalStateException(
        "Cannot initialize ExecutionContext; AsyncExecutor shut down during initialization"
      )
    }
    new ExecutionContextExecutor {

      override def reportFailure(t: Throwable): Unit =
        logger.error("Async executor failed with exception", t)

      override def execute(command: Runnable): Unit =
        if (command.isInstanceOf[PrioritizedRunnable]) {
          executor.execute(command)
        } else {
          /*
            Slick 3.5.2 implements this else-branch as:
            ```
              executor.execute(new PrioritizedRunnable {
                override def priority(): Priority = WithConnection
                override def run(): Unit = command.run()
              })
            ```
            Because `PrioritizedRunnable` is a sealed trait in the library, we
            use the `apply` method of its companion object instead.
           */
          executor.execute(
            PrioritizedRunnable(
              priority = WithConnection,
              _ => command.run(),
            )
          )
        }
    }
  }

  private[this] def unregisterMbeans(): Unit = if (registerMbeans) {
    try {
      val mbeanServer = ManagementFactory.getPlatformMBeanServer
      logger.debug(s"Unregistering MBean $mbeanName")
      try mbeanServer.unregisterMBean(mbeanName)
      catch { case _: InstanceNotFoundException => }
    } catch { case NonFatal(ex) => logger.error("Error unregistering MBean", ex) }
  }

  def close(): Unit = if (state.getAndSet(3) == 2) {
    unregisterMbeans()
    executor.shutdownNow()
    if (!executor.awaitTermination(30, TimeUnit.SECONDS))
      logger.warn("Abandoning ThreadPoolExecutor (not yet destroyed after 30 seconds)")
  }

  private class DaemonThreadFactory(namePrefix: String) extends ThreadFactory {
    private[this] val group = Thread.currentThread.getThreadGroup
    private[this] val threadNumber = new AtomicInteger(1)

    def newThread(r: Runnable): Thread = {
      val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
      if (!t.isDaemon) t.setDaemon(true)
      if (t.getPriority != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY)
      t
    }
  }

}

object AsyncExecutorWithMetrics {
  def createSingleThreaded(
      name: String,
      logger: Logger,
  ): AsyncExecutorWithMetrics & AsyncExecutorWithShutdown =
    new AsyncExecutorWithMetrics(
      name = s"${name}DatabaseExecutor",
      minThreads = 1,
      maxThreads = 1,
      queueSize = 1000,
      logger = logger,
      queryCostTracker = QueryCostTracker.NoOp,
      maxConnections = 1,
    )
}
