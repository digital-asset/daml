package com.daml.platform.apiserver.execution

import com.daml.scalautil.Statement.discard

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutorService, Future, TimeUnit}

object ExecutorServiceProbe {
  def apply(delegate: ExecutorService): (ExecutorService, () => Int) = {
    val probe = new AtomicInteger()
    val ec = new ExecutorServiceProbe(delegate, probe)
    (ec, () => probe.get())
  }
}

private class ExecutorServiceProbe(val delegate: ExecutorService, queued: AtomicInteger) extends ExecutorService {

  private def inc(): Unit = discard(queued.incrementAndGet())
  private def inc(i: Int): Unit = discard(queued.addAndGet(i))
  private def dec(): Unit = discard(queued.decrementAndGet())

  override def execute(runnable: Runnable): Unit = {
    inc()
    delegate.execute(new InstrumentedRunnable(runnable))
  }

  override def submit(runnable: Runnable): Future[_] = {
    inc()
    delegate.submit(new InstrumentedRunnable(runnable))
  }

  override def submit[T](runnable: Runnable, result: T): Future[T] = {
    inc()
    delegate.submit(new InstrumentedRunnable(runnable), result)
  }

  override def submit[T](task: Callable[T]): Future[T] = {
    inc()
    delegate.submit(new InstrumentedCallable[T](task))
  }

  override def invokeAll[T](tasks: util.Collection[_ <: Callable[T]]): util.List[Future[T]] = {
    inc(tasks.size)
    delegate.invokeAll(instrument(tasks))
  }

  override def invokeAll[T](tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): util.List[Future[T]] = {
    inc(tasks.size)
    delegate.invokeAll(instrument(tasks), timeout, unit)
  }

  override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]]): T = {
    inc(tasks.size)
    delegate.invokeAny(instrument(tasks))
  }

  override def invokeAny[T](tasks: util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit): T = {
    inc(tasks.size)
    delegate.invokeAny(instrument(tasks), timeout, unit)
  }

  private def instrument[T](tasks: util.Collection[_ <: Callable[T]]): util.Collection[_ <: Callable[T]] = {
    val instrumented: util.List[InstrumentedCallable[T]] = new util.ArrayList[InstrumentedCallable[T]](tasks.size)
    val it= tasks.iterator()
    while (it.hasNext) {
      instrumented.add(new InstrumentedCallable[T](it.next()))
    }
    instrumented
  }

  override def shutdown(): Unit = delegate.shutdown()

  override def shutdownNow: util.List[Runnable] = delegate.shutdownNow

  override def isShutdown: Boolean = delegate.isShutdown

  override def isTerminated: Boolean = delegate.isTerminated

  override def awaitTermination(l: Long, timeUnit: TimeUnit): Boolean = delegate.awaitTermination(l, timeUnit)

  private class InstrumentedRunnable (val task: Runnable) extends Runnable {
    override def run(): Unit = {
      try task.run() finally dec()
    }
  }

  private class InstrumentedCallable[T] (val callable: Callable[T]) extends Callable[T] {
    override def call(): T = {
      try callable.call finally dec()
    }
  }

}

