// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration

class ACSReaderSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  private val actorSystem = ActorSystem()
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  override def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), Duration(10, "seconds"))
    ()
  }

  behavior of "pullWorkerSource"

  it should "give an empty source if initialTasks are empty" in {
    FilterTableACSReader
      .pullWorkerSource[Int, String](
        workerParallelism = 1,
        materializer = materializer,
      )(
        work = _ => Future.successful("a" -> None),
        initialTasks = Nil,
      )
      .runWith(Sink.collection)
      .map(_ should have size 0)
  }

  it should "iterate through one task to completion with parallelism 1 and 1 element" in {
    FilterTableACSReader
      .pullWorkerSource[Int, String](
        workerParallelism = 1,
        materializer = materializer,
      )(
        work = i => Future.successful(i.toString -> Some(i + 1).filter(_ < 0)),
        initialTasks = 0 :: Nil,
      )
      .runWith(Sink.collection)
      .map(_.map(_._2) shouldBe List("0"))
  }

  it should "iterate through one task to completion with parallelism 1 and 3 elements" in {
    FilterTableACSReader
      .pullWorkerSource[Int, String](
        workerParallelism = 1,
        materializer = materializer,
      )(
        work = i => Future.successful(i.toString -> Some(i + 1).filter(_ < 3)),
        initialTasks = 0 :: Nil,
      )
      .runWith(Sink.collection)
      .map(_.map(_._2) shouldBe List("0", "1", "2"))
  }

  private val simple4Task = List(1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4).reverse
  private val simple4Worker: ((Int, Int)) => Future[(String, Option[(Int, Int)])] =
    i =>
      Future.successful(
        i.toString ->
          Some(i._2 + (if (i._1 == 2) 5 else 10))
            .filter(_ < 50)
            .map(i._1 -> _)
      )
  val simple4WorkerExpectedOrderedResult = List(
    "(1,1)",
    "(2,2)",
    "(3,3)",
    "(4,4)",
    "(2,7)",
    "(1,11)",
    "(2,12)",
    "(3,13)",
    "(4,14)",
    "(2,17)",
    "(1,21)",
    "(2,22)",
    "(3,23)",
    "(4,24)",
    "(2,27)",
    "(1,31)",
    "(2,32)",
    "(3,33)",
    "(4,34)",
    "(2,37)",
    "(1,41)",
    "(2,42)",
    "(3,43)",
    "(4,44)",
    "(2,47)",
  )

  it should "always pick the smallest with parallelism 1" in {
    FilterTableACSReader
      .pullWorkerSource[(Int, Int), String](
        workerParallelism = 1,
        materializer = materializer,
      )(
        work = simple4Worker,
        initialTasks = simple4Task,
      )(Ordering.by[(Int, Int), Int](_._2))
      .runWith(Sink.collection)
      .map(
        _.map(_._2) shouldBe simple4WorkerExpectedOrderedResult
      )
  }

  it should "finish and provide the expected set of results with parallelism 2" in {
    FilterTableACSReader
      .pullWorkerSource[(Int, Int), String](
        workerParallelism = 2,
        materializer = materializer,
      )(
        work = simple4Worker,
        initialTasks = simple4Task,
      )(Ordering.by[(Int, Int), Int](_._2))
      .runWith(Sink.collection)
      .map(
        _.map(_._2).toSet shouldBe simple4WorkerExpectedOrderedResult.toSet
      )
  }

  it should "finish and provide the expected set of results with parallelism 10" in {
    FilterTableACSReader
      .pullWorkerSource[(Int, Int), String](
        workerParallelism = 10,
        materializer = materializer,
      )(
        work = simple4Worker,
        initialTasks = simple4Task,
      )(Ordering.by[(Int, Int), Int](_._2))
      .runWith(Sink.collection)
      .map(
        _.map(_._2).toSet shouldBe simple4WorkerExpectedOrderedResult.toSet
      )
  }

  it should "fail if a worker fails" in {
    FilterTableACSReader
      .pullWorkerSource[Int, String](
        workerParallelism = 1,
        materializer = materializer,
      )(
        work = i =>
          if (i == 3) Future.failed(new Exception("boom"))
          else Future.successful(i.toString -> Some(i + 1).filter(_ < 5)),
        initialTasks = 0 :: Nil,
      )
      .runWith(Sink.collection)
      .failed
      .map(_.getMessage shouldBe "boom")
  }

  case class PuppetTask(
      i: Int, // value for Ordering
      startedPromise: Promise[Unit] = Promise(), // completed by worker
  ) {
    private val finishedPromise: Promise[(Int, Option[PuppetTask])] = Promise()
    def finished: Future[(Int, Option[PuppetTask])] = finishedPromise.future
    def started: Future[Unit] = startedPromise.future

    def continueWith(next: Int)(thisResult: Int): PuppetTask = {
      val r = PuppetTask(next)
      finishedPromise.success(thisResult -> Some(r))
      r
    }

    def finish(thisResult: Int): Unit =
      finishedPromise.success(thisResult -> None)

  }
  def puppetWorker: PuppetTask => Future[(Int, Option[PuppetTask])] =
    puppetTask => {
      puppetTask.startedPromise.success(())
      puppetTask.finished
    }
  def waitMillis(millis: Long): Unit = Thread.sleep(millis)
  def stillRunning(streamResultsFuture: Future[immutable.Iterable[(PuppetTask, Int)]]): Unit = {
    waitMillis(5)
    streamResultsFuture.isCompleted shouldBe false
    ()
  }
  def notStartedYet(tasks: PuppetTask*): Unit = {
    waitMillis(5)
    tasks.foreach(_.started.isCompleted shouldBe false)
  }

  it should "provide correct execution order with parallelism 3 for 5 tasks" in {
    val puppetTask1 = PuppetTask(1)
    val puppetTask2 = PuppetTask(2)
    val puppetTask3 = PuppetTask(3)
    val puppetTask4 = PuppetTask(4)
    val puppetTask6 = PuppetTask(6)
    val streamResultsFuture: Future[immutable.Iterable[(PuppetTask, Int)]] =
      FilterTableACSReader
        .pullWorkerSource[PuppetTask, Int](
          workerParallelism = 3,
          materializer = materializer,
        )(
          work = puppetWorker,
          initialTasks = List(puppetTask1, puppetTask6, puppetTask3, puppetTask4, puppetTask2),
        )(Ordering.by[PuppetTask, Int](_.i))
        .runWith(Sink.collection)
    info("As stream processing starts")
    for {
      _ <- puppetTask1.started
      _ <- puppetTask2.started
      _ <- puppetTask3.started
      puppetTask10 = {
        stillRunning(streamResultsFuture)
        notStartedYet(puppetTask4, puppetTask6)
        info("The first three task started: Running: [1, 2, 3] Queueing: [4, 6]")
        info(
          "As 2 completes with continuation 10 -- completion inserts at the end of the queue case"
        )
        puppetTask2.continueWith(10)(100)
      }
      _ <- puppetTask4.started
      puppetTask5 = {
        stillRunning(streamResultsFuture)
        notStartedYet(puppetTask6, puppetTask10)
        info("4 started: Running: [1, 3, 4] Queueing: [6, 10]")
        info(
          "As 3 finishes with continuation 5 -- completion inserts at the beginning of the queue case"
        )
        puppetTask3.continueWith(5)(101)
      }
      _ <- puppetTask5.started
      _ = {
        stillRunning(streamResultsFuture)
        notStartedYet(puppetTask6, puppetTask10)
        info("5 started: Running: [1, 4, 5] Queueing: [6, 10]")
        info("As 1 finishes")
        puppetTask1.finish(102)
      }
      _ <- puppetTask6.started
      _ = {
        stillRunning(streamResultsFuture)
        notStartedYet(puppetTask10)
        info("6 started: Running: [4, 5, 6] Queueing: [10]")
        info("As 5 finishes")
        puppetTask5.finish(103)
      }
      _ <- puppetTask10.started
      _ = {
        stillRunning(streamResultsFuture)
        info("10 started: Running: [4, 6, 10] Queueing: []")
        info("As 10 finishes")
        puppetTask10.finish(104)
        stillRunning(streamResultsFuture)
        info("Running: [4, 6] Queueing: []")
        info("As 6 finishes")
        puppetTask6.finish(105)
        stillRunning(streamResultsFuture)
        info("Running: [4] Queueing: []")
        info("As 4 finishes")
        puppetTask4.finish(106)
      }
      streamResults <- streamResultsFuture
    } yield {
      streamResults.map(_._2) shouldBe List(
        100, 101, 102, 103, 104, 105, 106,
      )
      info("Stream is also finished, with the expected results")
      succeed
    }
  }
}
